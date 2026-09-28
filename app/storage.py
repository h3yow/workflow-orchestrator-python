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
-- 保留策略清理后的运行墓碑：区分"被清理"与"从未存在"
CREATE TABLE IF NOT EXISTS run_tombstones (
    run_id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    owner TEXT NOT NULL,
    terminal_status TEXT NOT NULL,  -- 被清理时的终态
    purged_at REAL NOT NULL,
    reason TEXT NOT NULL
);
-- 收敛/拒绝/清理的可解释审计记录（自身有界，由 trim_audit 定期收缩）
CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    owner TEXT NOT NULL,
    workflow_id TEXT,
    run_id TEXT,
    category TEXT NOT NULL,   -- convergence | retention
    action TEXT NOT NULL,     -- REUSED / REJECTED / PURGED / ...
    scope TEXT NOT NULL,    -- 人类可读的影响范围说明
    detail TEXT NOT NULL,
    ts REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_audit_owner_id ON audit_log(owner, id);
CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(ts);
CREATE INDEX IF NOT EXISTS idx_runs_wf_status
    ON runs(workflow_id, status);
"""


def _spec_to_json(spec: WorkflowSpec) -> str:
    return json.dumps(dataclasses.asdict(spec))


def _spec_from_json(s: str) -> WorkflowSpec:
    d = json.loads(s)
    tasks = [TaskSpec(**t) for t in d.pop("tasks")]
    # 兼容旧版本序列化的 spec：补齐新字段默认值
    d.setdefault("schedule_interval_seconds", None)
    d.setdefault("active_run_policy", "allow")
    d.setdefault("retain_last_runs", None)
    d.setdefault("retain_seconds", None)
    d.setdefault("tombstone_limit", 100)
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

    def update_workflow_spec(self, wf: Workflow) -> None:
        """更新工作流定义中的策略字段（并发收敛/保留策略持久化）。"""
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE workflows SET spec_json=? WHERE id=?",
                (_spec_to_json(wf.spec), wf.id))

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

    def get_active_run(self, workflow_id: str) -> Run | None:
        """返回该工作流当前的活跃运行（PENDING/RUNNING），无则 None。"""
        with self._lock:
            r = self._conn.execute(
                "SELECT * FROM runs WHERE workflow_id=? "
                "AND status IN ('PENDING','RUNNING') "
                "ORDER BY created_at LIMIT 1", (workflow_id,)).fetchone()
        return _row_to_run(r) if r else None

    def find_or_create_run(
        self, run: Run, tasks: list[TaskRun], check_active: bool
    ) -> tuple[str, Run]:
        """锁+单事务内完成"幂等键查重 / 活跃运行收敛检查 / 创建"。

        返回 (outcome, run)：
        - ("created", run)            新运行已创建；
        - ("idempotent_hit", existing) 同幂等键运行已存在；
        - ("active_exists", active)    本工作流已有活跃运行（收敛窗口命中）。
        检查与插入在同一临界区内，并发触发不可能两条都创建成功。
        """
        with self._lock, self._conn:
            if run.idempotency_key is not None:
                r = self._conn.execute(
                    "SELECT * FROM runs WHERE workflow_id=? AND idempotency_key=?",
                    (run.workflow_id, run.idempotency_key)).fetchone()
                if r:
                    return "idempotent_hit", _row_to_run(r)
            if check_active:
                r = self._conn.execute(
                    "SELECT * FROM runs WHERE workflow_id=? "
                    "AND status IN ('PENDING','RUNNING') "
                    "ORDER BY created_at LIMIT 1",
                    (run.workflow_id,)).fetchone()
                if r:
                    return "active_exists", _row_to_run(r)
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
        return "created", run

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

    def touch_run_updated_at(self, run_id: str, ts: float) -> None:
        """改写运行的结束时间（保留策略按 updated_at 判定新旧）。"""
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE runs SET updated_at=? WHERE id=?", (ts, run_id))

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

    def list_all_workflows(self) -> list[Workflow]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM workflows ORDER BY created_at").fetchall()
        return [_row_to_workflow(r) for r in rows]

    # ---- 历史保留与清理 ----
    def list_purge_candidates(
        self, workflow_id: str, retain_last: int | None,
        older_than: float | None, limit: int,
    ) -> list[Run]:
        """列出某工作流中应被清理的已结束运行。

        只考虑终态运行（进行中运行绝不受影响）：
        - older_than 非空时，保留 updated_at >= older_than 的运行（按时间保留）；
        - retain_last 非空时，按结束时间倒序保留最近 N 条（按数量保留）；
        两者同时配置时取交集（更严格的那个生效）。
        """
        q = ("SELECT * FROM runs WHERE workflow_id=? AND status IN "
             "('SUCCEEDED','FAILED','CANCELLED')")
        args: list = [workflow_id]
        if older_than is not None:
            q += " AND updated_at<?"
            args.append(older_than)
        q += " ORDER BY updated_at ASC LIMIT ?"
        args.append(limit)
        with self._lock:
            rows = self._conn.execute(q, args).fetchall()
        runs = [_row_to_run(r) for r in rows]
        if retain_last is not None:
            # 该工作流全部终态运行按结束时间倒序的前 N 条受保护
            with self._lock:
                kept = self._conn.execute(
                    "SELECT id FROM runs WHERE workflow_id=? AND status IN "
                    "('SUCCEEDED','FAILED','CANCELLED') "
                    "ORDER BY updated_at DESC LIMIT ?",
                    (workflow_id, retain_last)).fetchall()
            keep_ids = {r["id"] for r in kept}
            runs = [r for r in runs if r.id not in keep_ids]
        return runs

    def purge_runs_atomic(self, runs: list[Run], reason: str,
                          tombstone_limit: int) -> int:
        """单事务原子清理：运行 + 任务明细 + 执行事件 + 墓碑一起提交。

        要么全部清理成功，要么整体回滚不留半条记录。
        只按主键删除终态运行（进行中运行即使被误传入也不会被删）。
        返回被实际清理的运行数。
        """
        if not runs:
            return 0
        ids = [r.id for r in runs]
        placeholders = ",".join("?" for _ in ids)
        with self._lock, self._conn:
            placeholders_q = placeholders
            cur = self._conn.execute(
                f"DELETE FROM runs WHERE id IN ({placeholders_q}) "
                "AND status IN ('SUCCEEDED','FAILED','CANCELLED')", ids)
            deleted = cur.rowcount
            if deleted == 0:
                return 0
            self._conn.execute(
                f"DELETE FROM task_runs WHERE run_id IN ({placeholders})", ids)
            self._conn.execute(
                f"DELETE FROM events WHERE run_id IN ({placeholders})", ids)
            # 写墓碑（同事务）：墓碑记录与删除同时可见
            now_ts = now()
            self._conn.executemany(
                "INSERT OR IGNORE INTO run_tombstones"
                "(run_id,workflow_id,owner,terminal_status,purged_at,reason)"
                " VALUES(?,?,?,?,?,?)",
                [(r.id, r.workflow_id, r.owner, r.status.value, now_ts, reason)
                 for r in runs])
            # 每个工作流的墓碑数量有上界：只保留最近 tombstone_limit 条
            by_wf: dict[str, list[str]] = {}
            for r in runs:
                by_wf.setdefault(r.workflow_id, []).append(r.id)
            for wf_id in by_wf:
                old = self._conn.execute(
                    "SELECT run_id FROM run_tombstones WHERE workflow_id=? "
                    "ORDER BY purged_at DESC, run_id DESC "
                    "LIMIT -1 OFFSET ?",
                    (wf_id, max(0, tombstone_limit))).fetchall()
                if old:
                    old_ids = [r["run_id"] for r in old]
                    ph = ",".join("?" for _ in old_ids)
                    self._conn.execute(
                        f"DELETE FROM run_tombstones WHERE run_id IN ({ph})",
                        old_ids)
        return deleted

    # ---- 墓碑（区分"被清理"与"从未存在"） ----
    def get_tombstone(self, run_id: str):
        with self._lock:
            r = self._conn.execute(
                "SELECT * FROM run_tombstones WHERE run_id=?",
                (run_id,)).fetchone()
        return dict(r) if r else None

    def list_tombstones(self, owner: str) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM run_tombstones WHERE owner=? "
                "ORDER BY purged_at DESC", (owner,)).fetchall()
        return [dict(r) for r in rows]

    def count_tombstones(self, workflow_id: str) -> int:
        with self._lock:
            (n,) = self._conn.execute(
                "SELECT COUNT(*) FROM run_tombstones WHERE workflow_id=?",
                (workflow_id,)).fetchone()
        return n

    # ---- 可解释审计日志（有界） ----
    def log_audit(self, owner: str, workflow_id: str | None,
                  run_id: str | None, category: str, action: str,
                  scope: str, detail: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO audit_log(owner,workflow_id,run_id,category,"
                "action,scope,detail,ts) VALUES(?,?,?,?,?,?,?,?)",
                (owner, workflow_id, run_id, category, action, scope,
                 detail, now()))

    def list_audit(self, owner: str, limit: int = 200) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM audit_log WHERE owner=? ORDER BY id DESC LIMIT ?",
                (owner, limit)).fetchall()
        return [dict(r) for r in rows]

    def count_audit(self) -> int:
        with self._lock:
            (n,) = self._conn.execute(
                "SELECT COUNT(*) FROM audit_log").fetchone()
        return n

    def trim_audit(self, max_entries: int, max_age_seconds: float) -> int:
        """把审计日志压到上界以内（按条数与保留时长），返回删除条数。"""
        cutoff = now() - max_age_seconds
        with self._lock, self._conn:
            cur = self._conn.execute(
                "DELETE FROM audit_log WHERE ts<? OR id NOT IN "
                "(SELECT id FROM audit_log ORDER BY id DESC LIMIT ?)",
                (cutoff, max(0, max_entries)))
            return cur.rowcount

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
