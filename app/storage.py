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
    ActiveRunPolicy, RetentionPolicy, Run, RunStatus, TaskRun, TaskSpec,
    TaskStatus, Workflow, WorkflowSpec, now,
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
-- 活跃收敛与保留清理都按 (workflow, status, updated_at) 查
CREATE INDEX IF NOT EXISTS idx_runs_wf_status
    ON runs(workflow_id, status, updated_at);
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
-- 历史清理墓碑：运行实体删除后，在 TTL 内仍可回答"曾存在但被保留策略清理"
CREATE TABLE IF NOT EXISTS run_tombstones (
    run_id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    owner TEXT NOT NULL,
    status TEXT NOT NULL,
    trigger TEXT NOT NULL,
    finished_at REAL NOT NULL,
    reaped_at REAL NOT NULL,
    reason TEXT NOT NULL
);
-- 审计记录：触发复用/拒绝、历史清理、审计自身的裁剪，全部可解释且有上界
CREATE TABLE IF NOT EXISTS audit_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    owner TEXT,
    workflow_id TEXT,
    kind TEXT NOT NULL,
    scope_json TEXT NOT NULL,
    message TEXT NOT NULL
);
"""


def _spec_to_json(spec: WorkflowSpec) -> str:
    return json.dumps(dataclasses.asdict(spec))


def _spec_from_json(s: str) -> WorkflowSpec:
    d = json.loads(s)
    tasks = [TaskSpec(**t) for t in d.pop("tasks")]
    # 兼容旧版本写入的 spec：新字段缺省时回到"不收敛、不清理"的历史语义
    d.setdefault("active_run_policy", ActiveRunPolicy.ALLOW.value)
    ret = d.pop("retention", None)
    retention = RetentionPolicy(**ret) if isinstance(ret, dict) \
        else RetentionPolicy()
    return WorkflowSpec(tasks=tasks, retention=retention, **d)


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

    def list_all_workflows(self) -> list[Workflow]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM workflows ORDER BY created_at").fetchall()
        return [_row_to_workflow(r) for r in rows]

    def update_workflow_spec(self, workflow_id: str, spec: WorkflowSpec) -> None:
        """更新工作流规格（仅保留策略/收敛策略等配置，结构不变）。"""
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE workflows SET name=?, spec_json=? WHERE id=?",
                (spec.name, _spec_to_json(spec), workflow_id))

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

    def find_active_run(self, workflow_id: str) -> Run | None:
        with self._lock:
            r = self._conn.execute(
                "SELECT * FROM runs WHERE workflow_id=? "
                "AND status IN ('PENDING','RUNNING') "
                "ORDER BY created_at LIMIT 1",
                (workflow_id,)).fetchone()
        return _row_to_run(r) if r else None

    def create_run_if_no_active(self, run: Run,
                                tasks: list[TaskRun]) -> tuple[bool, Run | None]:
        """原子的"活跃检查 + 创建"。

        在同一事务（单连接 + 锁串行化）内完成：若该工作流已有 PENDING/
        RUNNING 的运行，则不写入任何数据并返回 (False, 已有运行)；
        否则连同 task_runs 一起落库并返回 (True, None)。
        幂等键唯一索引仍然兜底，冲突向上抛 IntegrityError。
        """
        with self._lock, self._conn:
            active = self._conn.execute(
                "SELECT * FROM runs WHERE workflow_id=? "
                "AND status IN ('PENDING','RUNNING') "
                "ORDER BY created_at LIMIT 1",
                (run.workflow_id,)).fetchone()
            if active:
                return False, _row_to_run(active)
            self._conn.execute(
                "INSERT INTO runs(id,workflow_id,owner,status,trigger,"
                "idempotency_key,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                (run.id, run.workflow_id, run.owner, run.status.value,
                 run.trigger, run.idempotency_key, run.created_at,
                 run.updated_at))
            for t in tasks:
                self._conn.execute(
                    "INSERT INTO task_runs(id,run_id,name,status,attempt,"
                    "max_attempts,next_retry_at,error,started_at,finished_at)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (t.id, t.run_id, t.name, t.status.value, t.attempt,
                     t.max_attempts, t.next_retry_at, t.error,
                     t.started_at, t.finished_at))
        return True, None

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

    def list_terminal_runs(self, workflow_id: str) -> list[Run]:
        """按结束时间（updated_at）倒序列出该工作流全部已结束运行。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM runs WHERE workflow_id=? "
                "AND status NOT IN ('PENDING','RUNNING') "
                "ORDER BY updated_at DESC",
                (workflow_id,)).fetchall()
        return [_row_to_run(r) for r in rows]

    def reap_runs(self, run_ids: list[str], reason: str) -> list[dict]:
        """整批删除已结束运行及其任务明细与事件，并写墓碑。

        全部删除在同一事务内完成：要么所有 run（连同 task_runs/events）
        全部清干净，要么整体不生效，绝不留下半条记录。返回写入的墓碑摘要。
        调用方必须保证 run_ids 均为终态运行（活跃运行绝不传入）。
        """
        if not run_ids:
            return []
        marks: list[dict] = []
        with self._lock, self._conn:
            # 再校验一次终态，防止与状态推进竞态时误删活跃运行
            placeholders = ",".join("?" * len(run_ids))
            active = self._conn.execute(
                f"SELECT id FROM runs WHERE id IN ({placeholders}) "
                "AND status IN ('PENDING','RUNNING')", run_ids).fetchall()
            if active:
                raise RuntimeError(
                    "refuse to reap active runs: "
                    f"{[r['id'] for r in active]}")
            rows = self._conn.execute(
                f"SELECT * FROM runs WHERE id IN ({placeholders})",
                run_ids).fetchall()
            reaped_at = now()
            for r in rows:
                rid = r["id"]
                self._conn.execute(
                    "DELETE FROM task_runs WHERE run_id=?", (rid,))
                self._conn.execute(
                    "DELETE FROM events WHERE run_id=?", (rid,))
                # 旧墓碑（TTL 裁剪后重删场景）先清再写
                self._conn.execute(
                    "DELETE FROM run_tombstones WHERE run_id=?", (rid,))
                self._conn.execute(
                    "INSERT INTO run_tombstones(run_id,workflow_id,owner,"
                    "status,trigger,finished_at,reaped_at,reason)"
                    " VALUES(?,?,?,?,?,?,?,?)",
                    (rid, r["workflow_id"], r["owner"], r["status"],
                     r["trigger"], r["updated_at"], reaped_at, reason))
                marks.append({
                    "run_id": rid, "workflow_id": r["workflow_id"],
                    "owner": r["owner"], "status": r["status"],
                    "trigger": r["trigger"],
                    "finished_at": r["updated_at"], "reaped_at": reaped_at,
                    "reason": reason})
            self._conn.execute(
                f"DELETE FROM runs WHERE id IN ({placeholders})", run_ids)
        return marks

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

    # ---- 清理墓碑 ----
    def get_tombstone(self, run_id: str) -> dict | None:
        with self._lock:
            r = self._conn.execute(
                "SELECT * FROM run_tombstones WHERE run_id=?",
                (run_id,)).fetchone()
        return dict(r) if r else None

    def prune_tombstones(self, ttl_seconds: float) -> int:
        """删除超过保留期的墓碑；过期后该运行回到"本来就不存在"的语义。"""
        with self._lock, self._conn:
            cur = self._conn.execute(
                "DELETE FROM run_tombstones WHERE reaped_at < ?",
                (now() - ttl_seconds,))
            return cur.rowcount

    # ---- 审计记录（有界） ----
    def add_audit(self, kind: str, message: str, scope: dict,
                  owner: str | None = None,
                  workflow_id: str | None = None) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO audit_events(ts,owner,workflow_id,kind,"
                "scope_json,message) VALUES(?,?,?,?,?,?)",
                (now(), owner, workflow_id, kind,
                 json.dumps(scope, ensure_ascii=False), message))

    def list_audit(self, owner: str, workflow_id: str | None = None,
                   limit: int = 200) -> list[dict]:
        """只列本 owner 的审计；越权者看不到任何记录。"""
        q = ("SELECT id,ts,owner,workflow_id,kind,scope_json,message "
             "FROM audit_events WHERE owner=?")
        args: list = [owner]
        if workflow_id:
            q += " AND workflow_id=?"
            args.append(workflow_id)
        q += " ORDER BY id DESC LIMIT ?"
        args.append(limit)
        with self._lock:
            rows = self._conn.execute(q, args).fetchall()
        out = [dict(r) for r in rows]
        for r in out:
            try:
                r["scope"] = json.loads(r.pop("scope_json"))
            except json.JSONDecodeError:
                r["scope"] = {}
        return out

    def prune_audit(self, ttl_seconds: float, max_rows: int) -> int:
        """审计表双重上界：超 TTL 删除；总量仍超 max_rows 时再按最旧裁剪。"""
        with self._lock, self._conn:
            cur = self._conn.execute(
                "DELETE FROM audit_events WHERE ts < ?",
                (now() - ttl_seconds,))
            removed = cur.rowcount
            (total,) = self._conn.execute(
                "SELECT COUNT(*) FROM audit_events").fetchone()
            if total > max_rows:
                excess = total - max_rows
                cur2 = self._conn.execute(
                    "DELETE FROM audit_events WHERE id IN ("
                    "SELECT id FROM audit_events ORDER BY id LIMIT ?)",
                    (excess,))
                removed += cur2.rowcount
        return removed

    def close(self) -> None:
        with self._lock:
            self._conn.close()
