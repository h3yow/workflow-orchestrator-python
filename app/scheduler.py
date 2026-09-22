"""调度引擎：触发、依赖解析、重试退避、超时、并发控制与重启恢复。

调度语义：
- 每个运行（Run）内的任务按依赖拓扑调度；前置全部 SUCCEEDED 才置 READY；
  任一前置进入 FAILED/TIMED_OUT/SKIPPED/CANCELLED，则本任务置 SKIPPED 终态。
- 全局并发上限由信号量控制；同一任务在同一运行中由
  UNIQUE(run_id, task_name) 与状态机保证不会被重复执行。
- 失败重试：attempt < max_attempts 时回到 READY 并按指数退避；
  重试耗尽 -> FAILED；执行超时 -> TIMED_OUT（终态，不再重试）。
- 所有状态转移经过状态机校验，非法转移（如迟到结果写回已终态任务）
  会被拒绝并记录事件。
"""
from __future__ import annotations

import logging
import sqlite3
import threading

from .config import Settings
from .executor import Executor, HandlerRegistry
from .models import (
    Rejected, RejectReason, Run, RunStatus, TaskRun, TaskStatus,
    TERMINAL_TASK_STATES, Workflow, WorkflowSpec, check_transition,
    RUN_TRANSITIONS, TASK_TRANSITIONS, new_id, now,
)
from .storage import Storage

logger = logging.getLogger("orchestrator")

# 事件类型（写入 events 表，供历史查询与恢复解释）
EV_RUN_CREATED = "RUN_CREATED"
EV_RUN_DEDUPED = "RUN_DEDUPED"
EV_RUN_REJECTED = "RUN_REJECTED"
EV_RUN_STARTED = "RUN_STARTED"
EV_RUN_FINISHED = "RUN_FINISHED"
EV_RUN_CANCELLED = "RUN_CANCELLED"
EV_TASK_READY = "TASK_READY"
EV_TASK_SKIPPED = "TASK_SKIPPED"
EV_TASK_STARTED = "TASK_STARTED"
EV_TASK_SUCCEEDED = "TASK_SUCCEEDED"
EV_TASK_RETRY = "TASK_RETRY"
EV_TASK_FAILED = "TASK_FAILED"
EV_TASK_TIMED_OUT = "TASK_TIMED_OUT"
EV_RECOVERY = "RECOVERY"
EV_LATE_RESULT = "LATE_RESULT_DISCARDED"


def validate_spec(spec: WorkflowSpec) -> None:
    """校验工作流定义：未知依赖与依赖成环给出可区分分原因。"""
    names = [t.name for t in spec.tasks]
    if len(names) != len(set(names)):
        raise Rejected(RejectReason.UNKNOWN_TASK, "duplicate task name in spec")
    known = set(names)
    for t in spec.tasks:
        for dep in t.depends_on:
            if dep not in known:
                raise Rejected(RejectReason.UNKNOWN_TASK,
                               f"task '{t.name}' depends on unknown task '{dep}'")
    # 拓扑排序检测环（Kahn）
    indeg = {t.name: 0 for t in spec.tasks}
    for t in spec.tasks:
        for _ in t.depends_on:
            indeg[t.name] += 1
    queue = [n for n, d in indeg.items() if d == 0]
    seen = 0
    deps_of = {t.name: t.depends_on for t in spec.tasks}
    dependents: dict[str, list[str]] = {n: [] for n in names}
    for t in spec.tasks:
        for d in t.depends_on:
            dependents[d].append(t.name)
    while queue:
        n = queue.pop()
        seen += 1
        for m in dependents[n]:
            indeg[m] -= 1
            if indeg[m] == 0:
                queue.append(m)
    if seen != len(names):
        cyclic = sorted(n for n, d in indeg.items() if d > 0)
        raise Rejected(RejectReason.DEPENDENCY_CYCLE,
                       f"dependency cycle among: {', '.join(cyclic)}")
    _ = deps_of


class Orchestrator:
    def __init__(self, storage: Storage, executor: Executor,
                 settings: Settings, registry: HandlerRegistry) -> None:
        self.storage = storage
        self.executor = executor
        self.settings = settings
        self.registry = registry
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._slots = threading.Semaphore(settings.max_concurrent_tasks)
        self._spec_cache: dict[str, WorkflowSpec] = {}
        self._sched_last: dict[str, float] = {}

    # ---- 工作流注册 ----
    def register_workflow(self, spec: WorkflowSpec) -> Workflow:
        validate_spec(spec)
        for t in spec.tasks:
            try:
                self.registry.get(t.handler)
            except KeyError:
                raise Rejected(RejectReason.UNKNOWN_TASK,
                               f"handler '{t.handler}' not registered") from None
        if self.storage.get_workflow_by_name(spec.owner, spec.name):
            raise Rejected(RejectReason.DUPLICATE_WORKFLOW,
                           f"workflow '{spec.name}' exists for owner")
        wf = Workflow(id=new_id(), name=spec.name, owner=spec.owner,
                      spec=spec, created_at=now())
        self.storage.save_workflow(wf)
        self._spec_cache[wf.id] = spec
        logger.info("workflow registered: %s owner=%s", spec.name, spec.owner)
        return wf

    # ---- 触发 ----
    def trigger(self, owner: str, workflow_name: str,
                idempotency_key: str | None = None,
                trigger: str = "manual") -> Run:
        wf = self.storage.get_workflow_by_name(owner, workflow_name)
        if not wf:
            raise Rejected(RejectReason.WORKFLOW_NOT_FOUND, workflow_name)
        if idempotency_key:
            existing = self.storage.find_run_by_key(wf.id, idempotency_key)
            if existing:
                self.storage.log_event(
                    existing.id, None, EV_RUN_DEDUPED,
                    f"duplicate trigger key='{idempotency_key}' "
                    f"policy={self.settings.duplicate_run_policy}")
                if self.settings.duplicate_run_policy == "reject":
                    raise Rejected(RejectReason.DUPLICATE_RUN, idempotency_key)
                return existing
        if self.storage.count_active_runs() >= self.settings.max_pending_runs:
            self.storage.log_event(
                None, None, EV_RUN_REJECTED,
                f"queue full ({self.settings.max_pending_runs}), "
                f"reject run of '{workflow_name}'")
            raise Rejected(RejectReason.QUEUE_FULL,
                           f"active runs >= {self.settings.max_pending_runs}")
        run = Run(id=new_id(), workflow_id=wf.id, owner=owner,
                  status=RunStatus.PENDING, trigger=trigger,
                  idempotency_key=idempotency_key,
                  created_at=now(), updated_at=now())
        tasks = [TaskRun(id=new_id(), run_id=run.id, name=t.name,
                         status=TaskStatus.PENDING, attempt=0,
                         max_attempts=max(1, t.max_attempts),
                         next_retry_at=None, error=None,
                         started_at=None, finished_at=None)
                 for t in wf.spec.tasks]
        try:
            self.storage.create_run(run, tasks)
        except sqlite3.IntegrityError:
            # 并发下同键触发：唯一索引兜底，重新读取已有运行并按策略处理
            if idempotency_key:
                existing = self.storage.find_run_by_key(wf.id, idempotency_key)
                if existing:
                    self.storage.log_event(
                        existing.id, None, EV_RUN_DEDUPED,
                        f"concurrent duplicate key='{idempotency_key}' "
                        f"policy={self.settings.duplicate_run_policy}")
                    if self.settings.duplicate_run_policy == "reject":
                        raise Rejected(RejectReason.DUPLICATE_RUN,
                                       idempotency_key) from None
                    return existing
            raise
        self._spec_cache[wf.id] = wf.spec
        self.storage.log_event(run.id, None, EV_RUN_CREATED,
                               f"trigger={trigger} key={idempotency_key}")
        logger.info("run created: %s workflow=%s trigger=%s",
                    run.id, workflow_name, trigger)
        return run

    def cancel(self, owner: str, run_id: str) -> Run:
        run = self._get_owned_run(owner, run_id)
        if run.status in (RunStatus.SUCCEEDED, RunStatus.FAILED,
                          RunStatus.CANCELLED):
            raise Rejected(RejectReason.RUN_NOT_ACTIVE, run.status.value)
        check_transition(RUN_TRANSITIONS, run.status, RunStatus.CANCELLED, "run")
        self.storage.update_run_status(run_id, RunStatus.CANCELLED)
        for t in self.storage.list_task_runs(run_id):
            if t.status not in TERMINAL_TASK_STATES:
                check_transition(TASK_TRANSITIONS, t.status,
                                 TaskStatus.CANCELLED, "task")
                self.storage.update_task(t.id, status=TaskStatus.CANCELLED,
                                         finished_at=now())
        self.storage.log_event(run_id, None, EV_RUN_CANCELLED,
                               f"cancelled by owner={owner}")
        return self.storage.get_run(run_id)

    # ---- 查询（含隔离） ----
    def _get_owned_run(self, owner: str, run_id: str) -> Run:
        run = self.storage.get_run(run_id)
        if not run:
            raise Rejected(RejectReason.RUN_NOT_FOUND, run_id)
        if run.owner != owner:
            raise Rejected(RejectReason.FORBIDDEN,
                           "run belongs to another owner")
        return run

    def get_run_view(self, owner: str, run_id: str) -> dict:
        run = self._get_owned_run(owner, run_id)
        tasks = self.storage.list_task_runs(run_id)
        return {"run": run, "tasks": tasks}

    def get_run_events(self, owner: str, run_id: str) -> list[dict]:
        self._get_owned_run(owner, run_id)
        return self.storage.list_events(run_id)

    # ---- 调度循环 ----
    def start(self) -> None:
        """启动后台调度线程并执行崩溃恢复。"""
        self.recover()
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True,
                                        name="orch-scheduler")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def recover(self) -> None:
        """重启恢复：中断任务按重试规则回到 READY 或置 FAILED。

        已成功任务保持 SUCCEEDED 不动，绝不会被重新触发；
        每条恢复决策都写事件，可由执行历史解释。
        """
        interrupted = self.storage.list_interrupted_tasks()
        for t in interrupted:
            run = self.storage.get_run(t.run_id)
            if run is None or run.status not in (RunStatus.PENDING,
                                                 RunStatus.RUNNING):
                continue
            if t.attempt >= t.max_attempts:
                self.storage.update_task(t.id, status=TaskStatus.FAILED,
                                         error="interrupted by restart; "
                                               "attempts exhausted",
                                         finished_at=now())
                self.storage.log_event(
                    t.run_id, t.name, EV_RECOVERY,
                    f"interrupted at attempt {t.attempt}/{t.max_attempts}; "
                    "no attempts left -> FAILED")
            else:
                backoff = self._backoff_for(t.run_id, t.name, t.attempt)
                self.storage.update_task(t.id, status=TaskStatus.READY,
                                         next_retry_at=now() + backoff,
                                         error="interrupted by restart")
                self.storage.log_event(
                    t.run_id, t.name, EV_RECOVERY,
                    f"interrupted at attempt {t.attempt}/{t.max_attempts}; "
                    f"requeue with backoff {backoff:.2f}s")
        for run in self.storage.list_active_runs():
            self.storage.log_event(
                run.id, None, EV_RECOVERY,
                f"run resumed in status {run.status.value} after restart")
        if interrupted:
            logger.info("recovery: %d interrupted tasks re-queued/failed",
                        len(interrupted))

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._tick()
            except Exception:
                logger.exception("scheduler tick failed")
            self._stop.wait(self.settings.tick_interval_seconds)

    def _tick(self) -> None:
        if self.settings.schedule_enabled:
            self._fire_schedules()
        for run in self.storage.list_active_runs():
            self._advance_run(run)
        # 领取 READY 任务直到并发槽位耗尽
        while self._slots.acquire(blocking=False):
            claimed = self.storage.claim_ready_task()
            if claimed is None:
                self._slots.release()
                break
            task, run = claimed
            threading.Thread(target=self._execute, args=(task, run),
                             daemon=True, name=f"orch-run-{task.name}"
                             ).start()

    # ---- 运行推进 ----
    def _spec_for(self, run: Run) -> WorkflowSpec:
        spec = self._spec_cache.get(run.workflow_id)
        if spec is None:
            wf = self.storage.get_workflow(run.workflow_id)
            spec = wf.spec
            self._spec_cache[run.workflow_id] = spec
        return spec

    def _advance_run(self, run: Run) -> None:
        if run.status == RunStatus.PENDING:
            check_transition(RUN_TRANSITIONS, run.status, RunStatus.RUNNING,
                             "run")
            self.storage.update_run_status(run.id, RunStatus.RUNNING)
            self.storage.log_event(run.id, None, EV_RUN_STARTED, "run started")
            run.status = RunStatus.RUNNING
        spec = self._spec_for(run)
        deps = {t.name: t.depends_on for t in spec.tasks}
        tasks = {t.name: t for t in self.storage.list_task_runs(run.id)}
        for name, t in tasks.items():
            if t.status != TaskStatus.PENDING:
                continue
            dep_states = [tasks[d].status for d in deps[name]]
            if all(s == TaskStatus.SUCCEEDED for s in dep_states):
                check_transition(TASK_TRANSITIONS, t.status, TaskStatus.READY,
                                 "task")
                self.storage.update_task(t.id, status=TaskStatus.READY)
                self.storage.log_event(run.id, name, EV_TASK_READY,
                                       "all dependencies succeeded")
            elif any(s in TERMINAL_TASK_STATES and s != TaskStatus.SUCCEEDED
                     for s in dep_states):
                check_transition(TASK_TRANSITIONS, t.status,
                                 TaskStatus.SKIPPED, "task")
                self.storage.update_task(t.id, status=TaskStatus.SKIPPED,
                                         finished_at=now(),
                                         error="dependency not succeeded")
                failed = [d for d, s in zip(deps[name], dep_states)
                          if s != TaskStatus.SUCCEEDED]
                self.storage.log_event(
                    run.id, name, EV_TASK_SKIPPED,
                    f"skipped: dependencies not succeeded: {failed}")
        # 全部任务终态 -> 运行收尾
        tasks = {t.name: t for t in self.storage.list_task_runs(run.id)}
        if tasks and all(t.status in TERMINAL_TASK_STATES
                         for t in tasks.values()):
            ok = all(t.status == TaskStatus.SUCCEEDED for t in tasks.values())
            final = RunStatus.SUCCEEDED if ok else RunStatus.FAILED
            check_transition(RUN_TRANSITIONS, RunStatus.RUNNING, final, "run")
            self.storage.update_run_status(run.id, final)
            bad = [n for n, t in tasks.items()
                   if t.status != TaskStatus.SUCCEEDED]
            self.storage.log_event(
                run.id, None, EV_RUN_FINISHED,
                f"run {final.value}" + (f"; unsuccessful tasks: {bad}"
                                        if bad else ""))

    # ---- 任务执行 ----
    def _backoff_for(self, run_id: str, task_name: str, attempt: int) -> float:
        run = self.storage.get_run(run_id)
        spec = self._spec_for(run)
        base = next(t.backoff_seconds for t in spec.tasks
                    if t.name == task_name)
        return base * (2 ** max(0, attempt - 1))

    def _execute(self, task: TaskRun, run: Run) -> None:
        try:
            spec = self._spec_for(run)
            tspec = next(t for t in spec.tasks if t.name == task.name)
            handler = self.registry.get(tspec.handler)
            self.storage.log_event(
                run.id, task.name, EV_TASK_STARTED,
                f"attempt {task.attempt}/{task.max_attempts}")
            payload = {"run_id": run.id, "task_name": task.name,
                       "attempt": task.attempt, "owner": run.owner}
            try:
                self.executor.run(handler, payload, tspec.timeout_seconds)
            except TimeoutError as e:
                self._finish_task(task, TaskStatus.TIMED_OUT, str(e),
                                  EV_TASK_TIMED_OUT)
                return
            except Exception as e:  # 业务失败：按重试策略处理
                if task.attempt < task.max_attempts:
                    backoff = tspec.backoff_seconds * (
                        2 ** max(0, task.attempt - 1))
                    self.storage.update_task(
                        task.id, status=TaskStatus.READY,
                        next_retry_at=now() + backoff, error=str(e))
                    self.storage.log_event(
                        run.id, task.name, EV_TASK_RETRY,
                        f"attempt {task.attempt} failed: {e!r}; "
                        f"retry in {backoff:.2f}s "
                        f"({task.attempt}/{task.max_attempts})")
                else:
                    self._finish_task(task, TaskStatus.FAILED, repr(e),
                                      EV_TASK_FAILED)
                return
            self._finish_task(task, TaskStatus.SUCCEEDED, None,
                              EV_TASK_SUCCEEDED)
        finally:
            self._slots.release()

    def _finish_task(self, task: TaskRun, status: TaskStatus,
                     error: str | None, event: str) -> None:
        try:
            current = self.storage.get_task_run(task.run_id, task.name)
            check_transition(TASK_TRANSITIONS, current.status, status, "task")
            self.storage.update_task(task.id, status=status, error=error,
                                     finished_at=now())
            self.storage.log_event(task.run_id, task.name, event,
                                   error or status.value)
        except Exception as e:
            # 迟到/非法写回（如任务已被取消）：拒绝并记录
            self.storage.log_event(task.run_id, task.name, EV_LATE_RESULT,
                                   f"rejected transition to {status.value}: {e}")

    # ---- 定时触发 ----
    def _fire_schedules(self) -> None:
        for wf in self.storage.list_scheduled_workflows():
            interval = wf.spec.schedule_interval_seconds
            slot = int(now() // interval)
            key = f"sched-{wf.id}-{slot}"
            last = self._sched_last.get(wf.id)
            if last is not None and now() - last < interval:
                continue
            try:
                self.trigger(wf.owner, wf.name, idempotency_key=key,
                             trigger="schedule")
                self._sched_last[wf.id] = now()
            except Rejected as e:
                if e.reason == RejectReason.DUPLICATE_RUN:
                    self._sched_last[wf.id] = now()  # 本槽位已触发过
                else:  # 积压等：按策略跳过本槽位，下周期再试
                    self.storage.log_event(
                        None, None, EV_RUN_REJECTED,
                        f"scheduled trigger of '{wf.name}' skipped: "
                        f"{e.reason.value}")
                    self._sched_last[wf.id] = now()
