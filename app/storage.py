"""SQLite 本地持久化：工作流、运行、任务运行、事件日志。

一致性约定：
- 单连接 + RLock，所有读写串行化，SQLite 开启 WAL；
- 多步写入（如创建 run 及其 task_runs）放在同一事务；
- claim_ready_task 在锁内完成"读取-置位"，保证同一任务不会被重复领取。
"""
from __future__ import annotations

import dataclasses
import json
import sqlite3
import threading

from .models import (
    Run, RunStatus, TaskRun, TaskSpec, TaskStatus, Workflow, WorkflowSpec, now,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS workflows (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    owner TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    UNIQUE(owner, name)
);
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    owner TEXT NOT NULL,
    status TEXT NOT NULL,
    trigger TEXT NOT NULL,
    idempotency_key TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
-- 幂等键唯一：同一工作流同一键永远只对应一次运行（去重的存储层保证）
CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_idem
    ON runs(workflow_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
CREATE TABLE IF NOT EXISTS task_runs (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    max_attempts INTEGER NOT NULL,
    next_retry_at REAL,
    error TEXT,
    started_at REAL,
    finished_at REAL,
    UNIQUE(run_id, name)  -- 同一任务在同一次运行中不重复
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT,
    task TEXT,
    kind TEXT NOT NULL,
    message TEXT NOT NULL,
    ts REAL NOT NULL
);
"""


def _spec_to_json(spec: WorkflowSpec) -> str:
    return json.dumps(dataclasses.asdict(spec))


def _spec_from_json(s: str) -> WorkflowSpec:
    d = json.loads(s)
    tasks = [TaskSpec(**t) for t in d.pop("tasks")]
    return WorkflowSpec(tasks=tasks, **d)


def _row_to_workflow(r: sqlite3.Row) -> Workflow:
    return Workflow(id=r["id"], name=r["name"], owner=r["owner"],
                    spec=_spec_from_json(r["spec_json"]),
                    created_at=r["created_at"])


def _row_to_run(r: sqlite3.Row) -> Run:
    return Run(id=r["id"], workflow_id=r["workflow_id"], owner=r["owner"],
               status=RunStatus(r["status"]), trigger=r["trigger"],
               idempotency_key=r["idempotency_key"],
               created_at=r["created_at"], updated_at=r["updated_at"])


def _row_to_task(r: sqlite3.Row) -> TaskRun:
    return TaskRun(id=r["id"], run_id=r["run_id"], name=r["name"],
                   status=TaskStatus(r["status"]), attempt=r["attempt"],
                   max_attempts=r["max_attempts"],
                   next_retry_at=r["next_retry_at"], error=r["error"],
                   started_at=r["started_at"], finished_at=r["finished_at"])


class Storage:
    """所有写操作走单连接 + 锁，保证多线程下状态一致。"""

    def __init__(self, db_path: str) -> None:
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        with self._lock, self._conn:
            self._conn.executescript(_SCHEMA)

    # ---- 工作流 ----
    def save_workflow(self, wf: Workflow) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO workflows(id,name,owner,spec_json,created_at)"
                " VALUES(?,?,?,?,?)",
                (wf.id, wf.name, wf.owner, _spec_to_json(wf.spec), wf.created_at))

    def get_workflow(self, workflow_id: str) -> Workflow | None:
        with self._lock:
            r = self._conn.execute(
                "SELECT * FROM workflows WHERE id=?", (workflow_id,)).fetchone()
        return _row_to_workflow(r) if r else None

    def get_workflow_by_name(self, owner: str, name: str) -> Workflow | None:
        with self._lock:
            r = self._conn.execute(
                "SELECT * FROM workflows WHERE owner=? AND name=?",
                (owner, name)).fetchone()
        return _row_to_workflow(r) if r else None

    def list_workflows(self, owner: str) -> list[Workflow]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM workflows WHERE owner=? ORDER BY created_at",
                (owner,)).fetchall()
        return [_row_to_workflow(r) for r in rows]

    def list_scheduled_workflows(self) -> list[Workflow]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM workflows").fetchall()
        return [w for w in map(_row_to_workflow, rows)
                if w.spec.schedule_interval_seconds is not None]

    # ---- 运行 ----
    def create_run(self, run: Run, tasks: list[TaskRun]) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO runs(id,workflow_id,owner,status,trigger,"
                "idempotency_key,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                (run.id, run.workflow_id, run.owner, run.status.value,
                 run.trigger, run.idempotency_key, run.created_at, run.updated_at))
            for t in tasks:
                self._conn.execute(
                    "INSERT INTO task_runs(id,run_id,name,status,attempt,"
                    "max_attempts,next_retry_at,error,started_at,finished_at)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (t.id, t.run_id, t.name, t.status.value, t.attempt,
                     t.max_attempts, t.next_retry_at, t.error,
                     t.started_at, t.finished_at))

    def get_run(self, run_id: str) -> Run | None:
        with self._lock:
            r = self._conn.execute(
                "SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
        return _row_to_run(r) if r else None

    def find_run_by_key(self, workflow_id: str, key: str) -> Run | None:
        with self._lock:
            r = self._conn.execute(
                "SELECT * FROM runs WHERE workflow_id=? AND idempotency_key=?",
                (workflow_id, key)).fetchone()
        return _row_to_run(r) if r else None

    def count_active_runs(self) -> int:
        with self._lock:
            (n,) = self._conn.execute(
                "SELECT COUNT(*) FROM runs WHERE status IN ('PENDING','RUNNING')"
            ).fetchone()
        return n

    def list_runs(self, owner: str, workflow_id: str | None = None) -> list[Run]:
        q = "SELECT * FROM runs WHERE owner=?"
        args: list = [owner]
        if workflow_id:
            q += " AND workflow_id=?"
            args.append(workflow_id)
        q += " ORDER BY created_at DESC"
        with self._lock:
            rows = self._conn.execute(q, args).fetchall()
        return [_row_to_run(r) for r in rows]

    def update_run_status(self, run_id: str, status: RunStatus) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE runs SET status=?, updated_at=? WHERE id=?",
                (status.value, now(), run_id))

    # ---- 任务运行 ----
    def list_task_runs(self, run_id: str) -> list[TaskRun]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM task_runs WHERE run_id=? ORDER BY rowid",
                (run_id,)).fetchall()
        return [_row_to_task(r) for r in rows]

    def get_task_run(self, run_id: str, name: str) -> TaskRun | None:
        with self._lock:
            r = self._conn.execute(
                "SELECT * FROM task_runs WHERE run_id=? AND name=?",
                (run_id, name)).fetchone()
        return _row_to_task(r) if r else None

    def update_task(self, task_id: str, **fields) -> None:
        """按字段名更新任务运行；status 自动取枚举值。"""
        cols, vals = [], []
        for k, v in fields.items():
            cols.append(f"{k}=?")
            vals.append(v.value if isinstance(v, TaskStatus) else v)
        vals.append(task_id)
        with self._lock, self._conn:
            self._conn.execute(
                f"UPDATE task_runs SET {', '.join(cols)} WHERE id=?", vals)

    def claim_ready_task(self) -> tuple[TaskRun, Run] | None:
        """原子地领取一个到期可执行的 READY 任务（置为 RUNNING）。"""
        with self._lock, self._conn:
            r = self._conn.execute(
                "SELECT t.* FROM task_runs t JOIN runs r ON t.run_id=r.id"
                " WHERE t.status='READY' AND r.status='RUNNING'"
                " AND (t.next_retry_at IS NULL OR t.next_retry_at<=?)"
                " ORDER BY r.created_at, t.rowid LIMIT 1", (now(),)).fetchone()
            if not r:
                return None
            task = _row_to_task(r)
            started = now()
            self._conn.execute(
                "UPDATE task_runs SET status='RUNNING', started_at=?,"
                " attempt=attempt+1 WHERE id=? AND status='READY'",
                (started, task.id))
            run = self._conn.execute(
                "SELECT * FROM runs WHERE id=?", (task.run_id,)).fetchone()
        task.status = TaskStatus.RUNNING
        task.started_at = started
        task.attempt += 1
        return task, _row_to_run(run)

    def list_interrupted_tasks(self) -> list[TaskRun]:
        """重启恢复：仍处于 RUNNING 的任务（进程已死，必为中断）。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM task_runs WHERE status='RUNNING'").fetchall()
        return [_row_to_task(r) for r in rows]

    def list_active_runs(self) -> list[Run]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM runs WHERE status IN ('PENDING','RUNNING')"
                " ORDER BY created_at").fetchall()
        return [_row_to_run(r) for r in rows]

    # ---- 事件日志 ----
    def log_event(self, run_id: str | None, task: str | None,
                  kind: str, message: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO events(run_id,task,kind,message,ts)"
                " VALUES(?,?,?,?,?)",
                (run_id, task, kind, message, now()))

    def list_events(self, run_id: str) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM events WHERE run_id=? ORDER BY id",
                (run_id,)).fetchall()
        return [dict(r) for r in rows]

    def close(self) -> None:
        with self._lock:
            self._conn.close()
