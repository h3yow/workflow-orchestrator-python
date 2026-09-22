"""编排引擎门面：组合 Store/Executor/Scheduler。

并发触发策略（同租户同工作流）：
- REJECT：已存在活动运行（PENDING 或 RUNNING）时拒绝，ALREADY_ACTIVE。
- SKIP：去重——不新建运行，直接返回已有活动运行的 run_id（deduped=True）。
- QUEUE：允许排队；活动运行数 > max_queued_runs 时拒绝，QUEUE_FULL。
  同一工作流始终最多只有一个 RUNNING（由调度器 FIFO 放行）。

定时触发：固定间隔 schedule，到点按工作流自身的并发策略处理；
被拒绝时记录决策日志（SCHED_TRIGGER_REJECTED）并推进下一次触发时刻。
"""
from __future__ import annotations

import logging
import os
import threading
from datetime import datetime, timedelta, timezone
from typing import Any

from app.errors import ErrorCode, WorkflowError
from app.executor import Executor
from app.models import (
    ConcurrencyPolicy,
    RunState,
    TaskAttempt,
    TaskInfo,
    WorkflowDef,
)
from app.scheduler import Scheduler
from app.store import Store, utcnow_iso
from app.validation import validate_definition


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


class Engine:
    def __init__(self, db_path: str = "data/workflow.db", max_workers: int = 4,
                 poll_interval: float = 0.05, retain_terminal_runs: int = 100,
                 start_scheduler: bool = True):
        directory = os.path.dirname(db_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        self.logger = logging.getLogger("workflow.engine")
        self.store = Store(db_path)
        self.store.init_schema()
        self.max_workers = max_workers
        self.executor = Executor(self.store, logging.getLogger("workflow.executor"),
                                 max_workers=max_workers)
        self.scheduler = Scheduler(
            self.store, self.executor, logging.getLogger("workflow.scheduler"),
            max_workers=max_workers, poll_interval=poll_interval,
            retain_terminal_runs=retain_terminal_runs,
        )
        self._sched_stop = threading.Event()
        self._sched_thread: threading.Thread | None = None
        self._trigger_lock = threading.Lock()
        if start_scheduler:
            self.start()

    def start(self) -> None:
        self.scheduler.start()
        self._sched_thread = threading.Thread(
            target=self._schedule_loop, name="wf-cron", daemon=True,
        )
        self._sched_thread.start()

    def shutdown(self) -> None:
        self._sched_stop.set()
        if self._sched_thread is not None:
            self._sched_thread.join(timeout=2)
        self.scheduler.stop()
        self.executor.shutdown(wait=True)
        self.store.close()

    def crash(self) -> None:
        """模拟进程崩溃：立即停止调度/定时线程并关闭存储，不等待工作线程。

        旧工作线程的迟到记账会因连接关闭而失败（等同进程被杀死）；
        重启后由 recover_interrupted_tasks 依据仍为 RUNNING 的任务行恢复。
        """
        self._sched_stop.set()
        self.scheduler._stop.set()  # noqa: SLF001
        self.executor._stop.set()  # noqa: SLF001
        self.store.close()

    # ------------------------------------------------------------- 工作流定义
    def register_workflow(self, tenant_id: str, definition: WorkflowDef) -> None:
        validate_definition(definition)
        self.store.upsert_workflow(tenant_id, definition)

    def get_workflow(self, tenant_id: str, name: str) -> WorkflowDef:
        wf = self.store.get_workflow(tenant_id, name)
        if wf is None:
            raise WorkflowError(ErrorCode.WORKFLOW_NOT_FOUND,
                                f"工作流不存在: {name}")
        return wf

    def list_workflows(self, tenant_id: str) -> list[WorkflowDef]:
        return self.store.list_workflows(tenant_id)

    # ----------------------------------------------------------------- 触发
    def trigger_run(self, tenant_id: str, workflow_name: str,
                    trigger: str = "manual") -> tuple[str, bool]:
        """触发一次运行。返回 (run_id, deduped)。

        去重/拒绝依据工作流定义中的 concurrency 与 max_queued_runs。
        “计数 -> 裁决 -> 插入”在进程内互斥中原子完成，SQLite 写事务进一步
        串行化存储层，保证多线程同时触发不会各建一个运行。
        """
        wf = self.get_workflow(tenant_id, workflow_name)
        with self._trigger_lock:
            active = self.store.count_active_runs(tenant_id, workflow_name)
            running = self.store.count_running_runs(tenant_id, workflow_name)
            if active > 0:
                if wf.concurrency == ConcurrencyPolicy.REJECT:
                    self.store.add_decision(
                        tenant_id, "TRIGGER_REJECTED",
                        f"拒绝触发工作流 {workflow_name}：已有 {active} 个活动运行"
                        f"（策略 REJECT）",
                    )
                    raise WorkflowError(
                        ErrorCode.ALREADY_ACTIVE,
                        f"工作流 {workflow_name} 已有活动运行，策略=REJECT",
                    )
                if wf.concurrency == ConcurrencyPolicy.SKIP:
                    existing = self.store.list_runs(tenant_id, workflow_name)
                    run_id = next(
                        r.id for r in existing
                        if r.state in (RunState.PENDING, RunState.RUNNING)
                    )
                    self.store.add_decision(
                        tenant_id, "TRIGGER_DEDUPED",
                        f"触发去重：复用活动运行 {run_id}（策略 SKIP）", run_id,
                    )
                    return run_id, True
                # QUEUE：容量上限 = 1 个运行中 + max_queued_runs 个排队
                if active >= wf.max_queued_runs + 1:
                    self.store.add_decision(
                        tenant_id, "TRIGGER_REJECTED",
                        f"拒绝触发工作流 {workflow_name}：活动运行 {active} 个，"
                        f"超过容量上限（1 运行 + {wf.max_queued_runs} 排队，策略 QUEUE）",
                    )
                    raise WorkflowError(
                        ErrorCode.QUEUE_FULL,
                        f"工作流 {workflow_name} 运行积压已达上限"
                        f"（1 运行 + max_queued_runs={wf.max_queued_runs}）",
                    )
            initial = RunState.PENDING if (
                wf.concurrency == ConcurrencyPolicy.QUEUE and running > 0
            ) else RunState.RUNNING
            run_id = self.store.create_run(tenant_id, wf, trigger, state=initial)
            self.store.add_decision(
                tenant_id, "RUN_CREATED",
                f"创建运行（触发方式={trigger}，并发策略={wf.concurrency.value}，"
                f"初始状态={initial.value}）",
                run_id,
            )
            return run_id, False

    # ----------------------------------------------------------------- 查询
    def get_run(self, tenant_id: str, run_id: str):
        run = self.store.get_run(tenant_id, run_id)
        if run is None:
            raise WorkflowError(ErrorCode.RUN_NOT_FOUND, f"运行不存在: {run_id}")
        return run

    def list_runs(self, tenant_id: str, workflow_name: str | None = None):
        if workflow_name is not None:
            self.get_workflow(tenant_id, workflow_name)
        return self.store.list_runs(tenant_id, workflow_name)

    def list_tasks(self, tenant_id: str, run_id: str) -> list[TaskInfo]:
        self.get_run(tenant_id, run_id)  # 越权/不存在统一 404 语义
        out = []
        for row in self.store.list_task_rows(run_id):
            info = self.store._row_to_task(row)  # noqa: SLF001
            info.attempts = self.store.list_attempts(row["id"])
            out.append(info)
        return out

    def list_history(self, tenant_id: str, run_id: str):
        self.get_run(tenant_id, run_id)
        return self.store.list_decisions(tenant_id, run_id)

    # ------------------------------------------------------------- 定时触发
    def create_schedule(self, tenant_id: str, schedule_name: str,
                        workflow_name: str, interval_seconds: float,
                        first_fire_at: datetime | None = None) -> int:
        if interval_seconds <= 0:
            raise WorkflowError(ErrorCode.INVALID_DEFINITION,
                                "interval_seconds 必须为正数")
        self.get_workflow(tenant_id, workflow_name)
        if self.store.get_schedule(tenant_id, schedule_name) is not None:
            raise WorkflowError(ErrorCode.DUPLICATE_SCHEDULE,
                                f"定时计划已存在: {schedule_name}")
        if first_fire_at is None:
            first_fire_at = datetime.now(timezone.utc) + timedelta(
                seconds=interval_seconds)
        return self.store.create_schedule(
            tenant_id, schedule_name, workflow_name, interval_seconds,
            _iso(first_fire_at),
        )

    def list_schedules(self, tenant_id: str):
        return self.store.list_schedules(tenant_id)

    def delete_schedule(self, tenant_id: str, schedule_name: str) -> None:
        if not self.store.delete_schedule(tenant_id, schedule_name):
            raise WorkflowError(ErrorCode.SCHEDULE_NOT_FOUND,
                                f"定时计划不存在: {schedule_name}")

    def fire_due_schedules(self, now: datetime | None = None) -> int:
        """检查并触发到期的定时计划。返回成功触发（或去重）的计划数。"""
        now = now or datetime.now(timezone.utc)
        fired = 0
        for row in self.store.list_due_schedules(_iso(now)):
            interval = row["interval_seconds"]
            next_fire = _iso(now + timedelta(seconds=interval))
            try:
                run_id, deduped = self.trigger_run(
                    row["tenant_id"], row["workflow_name"],
                    trigger=f"schedule:{row['schedule_name']}",
                )
                self.store.add_decision(
                    row["tenant_id"],
                    "SCHED_TRIGGER_DEDUPED" if deduped else "SCHED_TRIGGERED",
                    f"定时计划 {row['schedule_name']} "
                    f"{'去重复用' if deduped else '触发'}运行 {run_id}",
                    run_id,
                )
                fired += 1
            except WorkflowError as exc:
                self.store.add_decision(
                    row["tenant_id"], "SCHED_TRIGGER_REJECTED",
                    f"定时计划 {row['schedule_name']} 到点但触发被拒绝："
                    f"{exc.code} {exc.message}",
                )
                self.logger.warning(
                    "定时触发被拒绝 tenant=%s schedule=%s code=%s",
                    row["tenant_id"], row["schedule_name"], exc.code,
                )
            finally:
                # 无论触发还是拒绝都推进，避免热循环；下一拍重新评估积压。
                self.store.advance_schedule(row["id"], _iso(now), next_fire)
        return fired

    def _schedule_loop(self) -> None:
        while not self._sched_stop.wait(self.scheduler.poll_interval):
            try:
                self.fire_due_schedules()
            except Exception:  # noqa: BLE001 - 定时循环必须存活
                self.logger.exception("定时触发循环异常")
