"""基于 SQLite 的本地持久化存储（WAL + 事务 + 行级条件更新）。

设计要点：
- 单一连接配合进程内 RLock 串行化写入；WAL 模式保证读写不互相阻塞。
- run 创建时把工作流定义快照(def_json)固化，之后定义变更不影响在途运行。
- 所有状态转移均为条件 UPDATE（WHERE state IN (...)），由数据库裁决状态机，
  非法/竞态转移返回 False，由上层翻译为 INVALID_TRANSITION。
- decision_logs 记录关键调度决策，重启恢复过程同样写入，保证可解释。
"""
from __future__ import annotations

import dataclasses
import enum
import json
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterable

from app.models import (
    DecisionLog,
    RunState,
    RunInfo,
    TaskAttempt,
    TaskInfo,
    TaskState,
    WorkflowDef,
)


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_id() -> str:
    return uuid.uuid4().hex


def dumps(obj: Any) -> str:
    return json.dumps(_to_jsonable(obj), ensure_ascii=False, sort_keys=True)


def _to_jsonable(obj: Any) -> Any:
    if dataclasses.is_dataclass(obj):
        return {k: _to_jsonable(v) for k, v in dataclasses.asdict(obj).items()}
    if isinstance(obj, enum.Enum):
        return obj.value
    if isinstance(obj, dict):
        return {k: _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_jsonable(v) for v in obj]
    return obj


class Store:
    def __init__(self, path: str):
        self.path = path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")

    # ------------------------------------------------------------------ 基础
    @contextmanager
    def tx(self):
        with self._lock:
            try:
                yield self._conn
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def init_schema(self) -> None:
        with self.tx() as c:
            c.executescript(
                """
                CREATE TABLE IF NOT EXISTS workflows (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tenant_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    def_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(tenant_id, name)
                );
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    workflow_name TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    trigger TEXT NOT NULL,
                    def_json TEXT NOT NULL,
                    queued_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    error_code TEXT,
                    error_message TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_runs_tw
                    ON runs(tenant_id, workflow_name, state);
                CREATE INDEX IF NOT EXISTS idx_runs_state ON runs(state, queued_at);

                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
                    tenant_id TEXT NOT NULL,
                    task_key TEXT NOT NULL,
                    state TEXT NOT NULL,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL,
                    depends_json TEXT NOT NULL,
                    handler TEXT NOT NULL,
                    params_json TEXT NOT NULL,
                    timeout_seconds REAL,
                    scheduled_at TEXT,
                    started_at TEXT,
                    finished_at TEXT,
                    next_run_at TEXT,
                    error_code TEXT,
                    error_message TEXT,
                    UNIQUE(run_id, task_key)
                );
                CREATE INDEX IF NOT EXISTS idx_tasks_run ON tasks(run_id, state);

                CREATE TABLE IF NOT EXISTS task_attempts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
                    attempt_no INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    error_code TEXT,
                    error_message TEXT,
                    timed_out INTEGER NOT NULL DEFAULT 0,
                    UNIQUE(task_id, attempt_no)
                );

                CREATE TABLE IF NOT EXISTS decision_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    tenant_id TEXT NOT NULL,
                    run_id TEXT,
                    task_id TEXT,
                    kind TEXT NOT NULL,
                    detail TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_logs_run ON decision_logs(tenant_id, run_id);

                CREATE TABLE IF NOT EXISTS schedules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tenant_id TEXT NOT NULL,
                    schedule_name TEXT NOT NULL,
                    workflow_name TEXT NOT NULL,
                    interval_seconds REAL NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    last_run_at TEXT,
                    next_fire_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(tenant_id, schedule_name)
                );
                """
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------- 工作流定义
    def upsert_workflow(self, tenant_id: str, definition: WorkflowDef) -> None:
        now = utcnow_iso()
        payload = dumps(definition.__dict__)
        with self.tx() as c:
            row = c.execute(
                "SELECT id, created_at FROM workflows WHERE tenant_id=? AND name=?",
                (tenant_id, definition.name),
            ).fetchone()
            if row is None:
                c.execute(
                    "INSERT INTO workflows(tenant_id,name,version,def_json,"
                    "created_at,updated_at) VALUES(?,?,?,?,?,?)",
                    (tenant_id, definition.name, definition.version, payload, now, now),
                )
            else:
                c.execute(
                    "UPDATE workflows SET version=?, def_json=?, updated_at=? "
                    "WHERE tenant_id=? AND name=?",
                    (definition.version, payload, now, tenant_id, definition.name),
                )

    def get_workflow(self, tenant_id: str, name: str) -> WorkflowDef | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT def_json FROM workflows WHERE tenant_id=? AND name=?",
                (tenant_id, name),
            ).fetchone()
        return _load_def(row["def_json"]) if row else None

    def list_workflows(self, tenant_id: str) -> list[WorkflowDef]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT def_json FROM workflows WHERE tenant_id=? ORDER BY name",
                (tenant_id,),
            ).fetchall()
        return [_load_def(r["def_json"]) for r in rows]

    # ------------------------------------------------------------------- 运行
    def create_run(
        self,
        tenant_id: str,
        definition: WorkflowDef,
        trigger: str,
        state: RunState = RunState.PENDING,
    ) -> str:
        """创建运行并物化任务行（任务均为 PENDING）。返回 run_id。"""
        run_id = new_id()
        now = utcnow_iso()
        def_json = dumps(definition.__dict__)
        with self.tx() as c:
            c.execute(
                "INSERT INTO runs(id,tenant_id,workflow_name,version,state,"
                "trigger,def_json,queued_at) VALUES(?,?,?,?,?,?,?,?)",
                (run_id, tenant_id, definition.name, definition.version,
                 state.value, trigger, def_json, now),
            )
            started_at = now if state == RunState.RUNNING else None
            for t in definition.tasks:
                retry = t.retry
                max_attempts = 1 + (retry.max_retries if retry else 0)
                c.execute(
                    "INSERT INTO tasks(id,run_id,tenant_id,task_key,state,"
                    "attempt_count,max_attempts,depends_json,handler,params_json,"
                    "timeout_seconds,scheduled_at,started_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (new_id(), run_id, tenant_id, t.key, TaskState.PENDING.value,
                     0, max_attempts, dumps(list(t.depends_on)), t.handler,
                     dumps(t.params), t.timeout_seconds, started_at, started_at),
                )
        return run_id

    @staticmethod
    def _row_to_run(row: sqlite3.Row) -> RunInfo:
        return RunInfo(
            id=row["id"], tenant_id=row["tenant_id"],
            workflow_name=row["workflow_name"], version=row["version"],
            state=RunState(row["state"]), trigger=row["trigger"],
            queued_at=row["queued_at"], started_at=row["started_at"],
            finished_at=row["finished_at"], error_code=row["error_code"],
            error_message=row["error_message"],
        )

    def get_run(self, tenant_id: str, run_id: str) -> RunInfo | None:
        """租户隔离：只能读取本租户的运行。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM runs WHERE id=? AND tenant_id=?", (run_id, tenant_id)
            ).fetchone()
        return self._row_to_run(row) if row else None

    def list_runs(self, tenant_id: str, workflow_name: str | None = None) -> list[RunInfo]:
        sql = "SELECT * FROM runs WHERE tenant_id=?"
        args: list[Any] = [tenant_id]
        if workflow_name:
            sql += " AND workflow_name=?"
            args.append(workflow_name)
        sql += " ORDER BY queued_at DESC"
        with self._lock:
            rows = self._conn.execute(sql, args).fetchall()
        return [self._row_to_run(r) for r in rows]

    def count_active_runs(self, tenant_id: str, workflow_name: str) -> int:
        with self._lock:
            return self._conn.execute(
                "SELECT COUNT(*) AS n FROM runs WHERE tenant_id=? AND workflow_name=? "
                "AND state IN (?, ?)",
                (tenant_id, workflow_name, RunState.PENDING.value, RunState.RUNNING.value),
            ).fetchone()["n"]

    def count_running_runs(self, tenant_id: str, workflow_name: str) -> int:
        with self._lock:
            return self._conn.execute(
                "SELECT COUNT(*) AS n FROM runs WHERE tenant_id=? AND workflow_name=? "
                "AND state=?",
                (tenant_id, workflow_name, RunState.RUNNING.value),
            ).fetchone()["n"]

    def list_queued_runs(self, limit: int = 100) -> list[RunInfo]:
        """全局扫描排队运行（调度器跨租户 FIFO 放行）。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM runs WHERE state=? ORDER BY queued_at ASC LIMIT ?",
                (RunState.PENDING.value, limit),
            ).fetchall()
        return [self._row_to_run(r) for r in rows]

    def transition_run(
        self,
        run_id: str,
        expected: Iterable[RunState],
        new_state: RunState,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> bool:
        """条件转移：仅当当前状态属于 expected 时生效。"""
        states = tuple(s.value for s in expected)
        now = utcnow_iso()
        placeholders = ",".join("?" for _ in states)
        with self.tx() as c:
            cur = c.execute(
                f"UPDATE runs SET state=?, finished_at=?, error_code=?, error_message=? "
                f"WHERE id=? AND state IN ({placeholders})",
                (new_state.value, now, error_code, error_message, run_id, *states),
            )
            return cur.rowcount == 1

    def mark_run_started(self, run_id: str) -> None:
        now = utcnow_iso()
        with self.tx() as c:
            c.execute(
                "UPDATE runs SET state=?, started_at=COALESCE(started_at,?) WHERE id=? AND state=?",
                (RunState.RUNNING.value, now, run_id, RunState.PENDING.value),
            )

    def get_run_definition_snapshot(self, run_id: str) -> WorkflowDef:
        with self._lock:
            row = self._conn.execute(
                "SELECT def_json FROM runs WHERE id=?", (run_id,)
            ).fetchone()
        return _load_def(row["def_json"])

    # ------------------------------------------------------------------- 任务
    @staticmethod
    def _row_to_task(row: sqlite3.Row) -> TaskInfo:
        return TaskInfo(
            id=row["id"], run_id=row["run_id"], task_key=row["task_key"],
            state=TaskState(row["state"]), attempt_count=row["attempt_count"],
            scheduled_at=row["scheduled_at"], started_at=row["started_at"],
            finished_at=row["finished_at"], error_code=row["error_code"],
            error_message=row["error_message"], attempts=[],
        )

    def list_tasks(self, run_id: str) -> list[TaskInfo]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM tasks WHERE run_id=? ORDER BY scheduled_at, id", (run_id,)
            ).fetchall()
        return [self._row_to_task(r) for r in rows]

    def list_task_rows(self, run_id: str) -> list[sqlite3.Row]:
        with self._lock:
            return list(self._conn.execute(
                "SELECT * FROM tasks WHERE run_id=?", (run_id,)
            ).fetchall())

    def get_task_row(self, task_id: str) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM tasks WHERE id=?", (task_id,)
            ).fetchone()

    def get_task(self, tenant_id: str, run_id: str, task_key: str) -> TaskInfo | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE run_id=? AND task_key=? AND tenant_id=? "
                "AND run_id IN (SELECT id FROM runs WHERE tenant_id=?)",
                (run_id, task_key, tenant_id, tenant_id),
            ).fetchone()
        return self._row_to_task(row) if row else None

    def claim_ready_task(self, task_id: str) -> sqlite3.Row | None:
        """原子领取：READY -> RUNNING，保证同一任务不会被并发执行两次。"""
        now = utcnow_iso()
        with self.tx() as c:
            cur = c.execute(
                "UPDATE tasks SET state=?, started_at=?, attempt_count=attempt_count+1 "
                "WHERE id=? AND state=?",
                (TaskState.RUNNING.value, now, task_id, TaskState.READY.value),
            )
            if cur.rowcount != 1:
                return None
            row = c.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        return row

    def get_retry_due_task_ids(self, now_iso: str) -> list[str]:
        """退避已到期的 RETRY_WAIT 任务（下一拍可转为 READY）。"""
        with self._lock:
            return [r["id"] for r in self._conn.execute(
                "SELECT id FROM tasks WHERE state=? AND next_run_at IS NOT NULL "
                "AND next_run_at<=?",
                (TaskState.RETRY_WAIT.value, now_iso),
            ).fetchall()]

    def set_task_next_run_at(self, task_id: str, when_iso: str) -> None:
        with self.tx() as c:
            c.execute("UPDATE tasks SET next_run_at=? WHERE id=?", (when_iso, task_id))

    def list_retry_wait_rows(self) -> list[sqlite3.Row]:
        with self._lock:
            return list(self._conn.execute(
                "SELECT * FROM tasks WHERE state=? AND next_run_at IS NULL",
                (TaskState.RETRY_WAIT.value,),
            ).fetchall())

    def list_runs_with_pending_tasks(self) -> list[tuple[str, str]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT DISTINCT run_id, tenant_id FROM tasks "
                "WHERE state IN (?, ?)",
                (TaskState.PENDING.value, TaskState.RETRY_WAIT.value),
            ).fetchall()
        return [(r["run_id"], r["tenant_id"]) for r in rows]

    def list_ready_task_rows(self, limit: int) -> list[sqlite3.Row]:
        with self._lock:
            return list(self._conn.execute(
                "SELECT t.id AS id FROM tasks t JOIN runs r ON t.run_id=r.id "
                "WHERE t.state=? AND r.state=? "
                "ORDER BY t.scheduled_at ASC, t.id ASC LIMIT ?",
                (TaskState.READY.value, RunState.RUNNING.value, limit),
            ).fetchall())

    def list_all_run_ids(self) -> list[tuple[str, str]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT DISTINCT run_id, tenant_id FROM tasks"
            ).fetchall()
        return [(r["run_id"], r["tenant_id"]) for r in rows]

    def insert_attempt(
        self, task_id: str, attempt_no: int, state: TaskState,
        started_at: str, finished_at: str | None,
        error_code: str | None, error_message: str | None, timed_out: bool,
    ) -> None:
        with self.tx() as c:
            c.execute(
                "INSERT INTO task_attempts(task_id,attempt_no,state,started_at,"
                "finished_at,error_code,error_message,timed_out) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (task_id, attempt_no, state.value, started_at, finished_at,
                 error_code, error_message, 1 if timed_out else 0),
            )

    def list_attempts(self, task_id: str) -> list[TaskAttempt]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM task_attempts WHERE task_id=? ORDER BY attempt_no",
                (task_id,),
            ).fetchall()
        return [
            TaskAttempt(
                attempt_no=r["attempt_no"], state=r["state"],
                started_at=r["started_at"], finished_at=r["finished_at"],
                error_code=r["error_code"], error_message=r["error_message"],
                timed_out=bool(r["timed_out"]),
            )
            for r in rows
        ]

    def update_task_after_attempt(
        self, task_id: str, *, new_state: TaskState,
        finished_at: str | None, next_run_at: str | None,
        error_code: str | None, error_message: str | None,
    ) -> None:
        with self.tx() as c:
            c.execute(
                "UPDATE tasks SET state=?, finished_at=?, next_run_at=?, "
                "error_code=?, error_message=? WHERE id=?",
                (new_state.value, finished_at, next_run_at, error_code,
                 error_message, task_id),
            )

    def mark_tasks_ready(self, task_ids: list[str]) -> None:
        if not task_ids:
            return
        now = utcnow_iso()
        with self.tx() as c:
            c.executemany(
                "UPDATE tasks SET state=?, scheduled_at=? WHERE id=? AND state IN (?,?)",
                [(TaskState.READY.value, now, tid,
                  TaskState.PENDING.value, TaskState.RETRY_WAIT.value)
                 for tid in task_ids],
            )

    def skip_task(self, task_id: str, reason: str) -> None:
        now = utcnow_iso()
        with self.tx() as c:
            c.execute(
                "UPDATE tasks SET state=?, finished_at=?, error_code=?, error_message=? "
                "WHERE id=? AND state=?",
                (TaskState.SKIPPED.value, now, "UPSTREAM_FAILED", reason,
                 task_id, TaskState.PENDING.value),
            )

    def recover_interrupted_tasks(self) -> list[tuple[str, str, str, int]]:
        """把进程崩溃前处于 RUNNING 的任务标记为 INTERRUPTED 并按重试策略处置。

        返回 (task_id, tenant_id, run_id, attempt_no) 供上层写决策日志。
        状态由调用方（scheduler.recover）决定，本方法只负责把中断的执行
        落为一次 attempt 记录，避免“未完成的任务被静默丢弃”。
        """
        now = utcnow_iso()
        out: list[tuple[str, str, str, int]] = []
        with self.tx() as c:
            rows = c.execute(
                "SELECT * FROM tasks WHERE state=?", (TaskState.RUNNING.value,)
            ).fetchall()
            for r in rows:
                attempt_no = r["attempt_count"] if r["attempt_count"] > 0 else 1
                c.execute(
                    "INSERT OR IGNORE INTO task_attempts(task_id,attempt_no,state,"
                    "started_at,finished_at,error_code,error_message,timed_out) "
                    "VALUES(?,?,?,?,?,?,?,0)",
                    (r["id"], attempt_no, TaskState.FAILED.value,
                     r["started_at"] or now, now, "INTERRUPTED",
                     "进程在任务执行期间重启"),
                )
                if attempt_no < r["max_attempts"]:
                    c.execute(
                        "UPDATE tasks SET state=?, next_run_at=?, error_code=?, "
                        "error_message=?, finished_at=NULL WHERE id=?",
                        (TaskState.RETRY_WAIT.value, now, "INTERRUPTED",
                         "进程重启中断，等待重试", r["id"]),
                    )
                else:
                    c.execute(
                        "UPDATE tasks SET state=?, finished_at=?, next_run_at=NULL, "
                        "error_code=?, error_message=? WHERE id=?",
                        (TaskState.FAILED.value, now, "INTERRUPTED",
                         "进程重启中断且重试已耗尽", r["id"]),
                    )
                out.append((r["id"], r["tenant_id"], r["run_id"], attempt_no))
        return out

    # --------------------------------------------------------------- 决策日志
    def add_decision(
        self, tenant_id: str, kind: str, detail: str,
        run_id: str | None = None, task_id: str | None = None,
    ) -> None:
        with self.tx() as c:
            c.execute(
                "INSERT INTO decision_logs(ts,tenant_id,run_id,task_id,kind,detail) "
                "VALUES(?,?,?,?,?,?)",
                (utcnow_iso(), tenant_id, run_id, task_id, kind, detail),
            )

    def list_decisions(
        self, tenant_id: str, run_id: str | None = None, limit: int = 200
    ) -> list[DecisionLog]:
        sql = "SELECT * FROM decision_logs WHERE tenant_id=?"
        args: list[Any] = [tenant_id]
        if run_id:
            sql += " AND run_id=?"
            args.append(run_id)
        sql += " ORDER BY id DESC LIMIT ?"
        args.append(limit)
        with self._lock:
            rows = self._conn.execute(sql, args).fetchall()
        return [
            DecisionLog(id=r["id"], ts=r["ts"], tenant_id=r["tenant_id"],
                        run_id=r["run_id"], task_id=r["task_id"],
                        kind=r["kind"], detail=r["detail"])
            for r in rows
        ]

    # ----------------------------------------------------------------- 定时表
    def create_schedule(
        self, tenant_id: str, schedule_name: str, workflow_name: str,
        interval_seconds: float, next_fire_at: str,
    ) -> int:
        with self.tx() as c:
            cur = c.execute(
                "INSERT INTO schedules(tenant_id,schedule_name,workflow_name,"
                "interval_seconds,enabled,next_fire_at,created_at) "
                "VALUES(?,?,?,?,1,?,?)",
                (tenant_id, schedule_name, workflow_name, interval_seconds,
                 next_fire_at, utcnow_iso()),
            )
            return int(cur.lastrowid)

    def get_schedule(self, tenant_id: str, schedule_name: str):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM schedules WHERE tenant_id=? AND schedule_name=?",
                (tenant_id, schedule_name),
            ).fetchone()

    def list_schedules(self, tenant_id: str):
        with self._lock:
            return list(self._conn.execute(
                "SELECT * FROM schedules WHERE tenant_id=? ORDER BY id", (tenant_id,)
            ).fetchall())

    def list_due_schedules(self, now_iso: str):
        with self._lock:
            return list(self._conn.execute(
                "SELECT * FROM schedules WHERE enabled=1 AND next_fire_at<=?",
                (now_iso,),
            ).fetchall())

    def advance_schedule(self, schedule_id: int, last_run_at: str, next_fire_at: str) -> None:
        with self.tx() as c:
            c.execute(
                "UPDATE schedules SET last_run_at=?, next_fire_at=? WHERE id=?",
                (last_run_at, next_fire_at, schedule_id),
            )

    def delete_schedule(self, tenant_id: str, schedule_name: str) -> bool:
        with self.tx() as c:
            cur = c.execute(
                "DELETE FROM schedules WHERE tenant_id=? AND schedule_name=?",
                (tenant_id, schedule_name),
            )
            return cur.rowcount == 1

    # ---------------------------------------------------------------- 历史裁剪
    def prune_history(self, max_terminal_per_workflow: int) -> int:
        """每个 (租户,工作流) 只保留最近 N 条终态运行；级联删除任务/尝试/日志。"""
        if max_terminal_per_workflow <= 0:
            return 0
        deleted = 0
        with self.tx() as c:
            groups = c.execute(
                "SELECT tenant_id, workflow_name FROM runs "
                "WHERE state IN (?, ?) GROUP BY tenant_id, workflow_name",
                (RunState.SUCCEEDED.value, RunState.FAILED.value),
            ).fetchall()
            for g in groups:
                keep = c.execute(
                    "SELECT id FROM runs WHERE tenant_id=? AND workflow_name=? "
                    "AND state IN (?, ?) ORDER BY finished_at DESC LIMIT ?",
                    (g["tenant_id"], g["workflow_name"], RunState.SUCCEEDED.value,
                     RunState.FAILED.value, max_terminal_per_workflow),
                ).fetchall()
                keep_ids = tuple(r["id"] for r in keep)
                if keep_ids:
                    placeholders = ",".join("?" for _ in keep_ids)
                    old = c.execute(
                        f"SELECT id FROM runs WHERE tenant_id=? AND workflow_name=? "
                        f"AND state IN (?, ?) AND id NOT IN ({placeholders})",
                        (g["tenant_id"], g["workflow_name"],
                         RunState.SUCCEEDED.value, RunState.FAILED.value, *keep_ids),
                    ).fetchall()
                else:
                    old = []
                for r in old:
                    c.execute("DELETE FROM runs WHERE id=?", (r["id"],))
                    deleted += 1
        return deleted


def _load_def(def_json: str) -> WorkflowDef:
    from app.models import ConcurrencyPolicy, RetryPolicy, TaskDef

    data = json.loads(def_json)
    tasks = []
    for t in data["tasks"]:
        retry = t.get("retry")
        tasks.append(TaskDef(
            key=t["key"], handler=t["handler"],
            depends_on=tuple(t.get("depends_on", ())),
            params=t.get("params", {}),
            timeout_seconds=t.get("timeout_seconds"),
            retry=RetryPolicy(**retry) if retry else None,
        ))
    return WorkflowDef(
        name=data["name"], version=data["version"], tasks=tuple(tasks),
        concurrency=ConcurrencyPolicy(data.get("concurrency", "REJECT")),
        max_queued_runs=data.get("max_queued_runs", 0),
    )
