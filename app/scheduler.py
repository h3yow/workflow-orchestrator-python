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
import threading

from .config import Settings
from .executor import Executor, HandlerRegistry
from .models import (
    Rejected, RejectReason, Run, RunStatus, TaskRun, TaskStatus,
    TERMINAL_TASK_STATES, Workflow, WorkflowSpec, check_transition,
    RUN_TRANSITIONS, TASK_TRANSITIONS, new_id, now,
    ACTIVE_POLICIES, ACTIVE_POLICY_ALLOW, ACTIVE_POLICY_REUSE,
    ACTIVE_POLICY_REJECT, TRIGGER_CREATED, TRIGGER_REUSED, TriggerOutcome,
)
from .storage import Storage

# 审计分类
AUDIT_CONVERGENCE = "convergence"
AUDIT_RETENTION = "retention"

logger = logging.getLogger("orchestrator")

# 事件类型（写入 events 表，供历史查询与恢复解释）
EV_RUN_CREATED = "RUN_CREATED"
EV_RUN_DEDUPED = "RUN_DEDUPED"
EV_RUN_REJECTED = "RUN_REJECTED"
EV_RUN_REUSED = "RUN_REUSED"                  # 并发收敛：复用了活跃运行
EV_RUN_CONFLICT_REJECTED = "RUN_CONFLICT_REJECTED"  # 并发收敛：拒绝
EV_RETENTION_PURGED = "RETENTION_PURGED"        # 历史保留清理批次
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
        self._maint_thread: threading.Thread | None = None
        self._slots = threading.Semaphore(settings.max_concurrent_tasks)
        self._spec_cache: dict[str, WorkflowSpec] = {}
        self._sched_last: dict[str, float] = {}

    # ---- 工作流注册 ----
    def register_workflow(self, spec: WorkflowSpec) -> Workflow:
        validate_spec(spec)
        if spec.active_run_policy not in ACTIVE_POLICIES:
            raise Rejected(
                RejectReason.INVALID_POLICY,
                f"active_run_policy must be one of {sorted(ACTIVE_POLICIES)}")
        self.validate_retention(spec.retain_last_runs, spec.retain_seconds,
                                  spec.tombstone_limit)
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
    def active_policy_for(self, wf: Workflow) -> str:
        policy = wf.spec.active_run_policy or self.settings.default_active_run_policy
        if policy not in ACTIVE_POLICIES:
            raise Rejected(
                RejectReason.INVALID_POLICY,
                f"active_run_policy must be one of {sorted(ACTIVE_POLICIES)}")
        return policy

    def trigger(self, owner: str, workflow_name: str,
                idempotency_key: str | None = None,
                trigger: str = "manual") -> Run:
        """兼容入口：返回最终代表本次触发的运行（新建或复用）。"""
        return self.trigger_detailed(
            owner, workflow_name, idempotency_key, trigger).run

    def trigger_detailed(self, owner: str, workflow_name: str,
                          idempotency_key: str | None = None,
                          trigger: str = "manual") -> TriggerOutcome:
        """触发运行，返回可解释的收敛结果（新建/复用）。

        收敛顺序：先按幂等键去重（历史语义不变），再做积压上限检查，
        最后在单个临界区内按工作流的 active_run_policy 收敛活跃运行。
        手动触发与定时触发走同一套规则。
        """
        wf = self.storage.get_workflow_by_name(owner, workflow_name)
        if not wf:
            raise Rejected(RejectReason.WORKFLOW_NOT_FOUND, workflow_name)
        policy = self.active_policy_for(wf)

        # 幂等键预检（仅为早返回；原子方法内仍会再查一次）
        idem_hit = False
        if idempotency_key:
            existing = self.storage.find_run_by_key(wf.id, idempotency_key)
            if existing:
                return self._handle_idempotent_hit(
                    wf, existing, idempotency_key, trigger)

        if self.storage.count_active_runs() >= self.settings.max_pending_runs:
            self._audit(wf, None, "QUEUE_FULL",
                         f"global active runs >= {self.settings.max_pending_runs}",
                         f"trigger={trigger} key={idempotency_key}")
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
        outcome, got = self.storage.find_or_create_run(
            run, tasks, check_active=(policy != ACTIVE_POLICY_ALLOW))
        if outcome == "idempotent_hit":
            return self._handle_idempotent_hit(
                wf, got, idempotency_key, trigger)
        if outcome == "active_exists":
            return self._handle_active_conflict(wf, got, policy, trigger)
        self._spec_cache[wf.id] = wf.spec
        self.storage.log_event(run.id, None, EV_RUN_CREATED,
                             f"trigger={trigger} key={idempotency_key} "
                             f"active_policy={policy}")
        logger.info("run created: %s workflow=%s trigger=%s policy=%s",
                    run.id, workflow_name, trigger, policy)
        return TriggerOutcome(run=run, outcome=TRIGGER_CREATED,
                              reason=f"new run created under policy={policy}")

    def _handle_idempotent_hit(self, wf: Workflow, existing: Run,
                                idempotency_key: str,
                                trigger: str) -> TriggerOutcome:
        policy = self.settings.duplicate_run_policy
        self.storage.log_event(
            existing.id, None, EV_RUN_DEDUPED,
            f"duplicate trigger key='{idempotency_key}' policy={policy}")
        if policy == "reject":
            self._audit(wf, existing, "DUPLICATE_RUN",
                        f"idempotency_key={idempotency_key}",
                        f"trigger={trigger} rejected by dup policy=reject")
            raise Rejected(RejectReason.DUPLICATE_RUN, idempotency_key,
                           extra={"existing_run_id": existing.id})
        reason = (f"reused run {existing.id}: idempotency_key "
                  f"'{idempotency_key}' already exists")
        self._audit(wf, existing, "REUSED_BY_KEY",
                    f"idempotency_key={idempotency_key}", reason)
        return TriggerOutcome(run=existing, outcome=TRIGGER_REUSED,
                             reason=reason, idempotency_hit=True)

    def _handle_active_conflict(self, wf: Workflow, active: Run, policy: str,
                                 trigger: str) -> TriggerOutcome:
        scope = (f"workflow={wf.name} active_run={active.id} "
                 f"active_status={active.status.value} "
                 f"active_trigger={active.trigger}")
        if policy == ACTIVE_POLICY_REJECT:
            self.storage.log_event(
                active.id, None, EV_RUN_CONFLICT_REJECTED,
                f"new {trigger} trigger rejected; {scope}")
            self._audit(wf, active, "ACTIVE_CONFLICT_REJECTED", scope,
                        f"trigger={trigger} rejected by active_run_policy=reject")
            raise Rejected(
                RejectReason.ACTIVE_RUN_CONFLICT,
                f"workflow '{wf.name}' already has active run {active.id}",
                extra={"active_run_id": active.id,
                       "active_status": active.status.value})
        self.storage.log_event(
            active.id, None, EV_RUN_REUSED,
            f"new {trigger} trigger converged to existing active run; {scope}")
        reason = (f"reused active run {active.id}: workflow already has a "
                  f"{active.status.value} run (trigger={active.trigger})")
        self._audit(wf, active, "REUSED_ACTIVE", scope,
                    f"trigger={trigger} converged by active_run_policy=reuse")
        return TriggerOutcome(run=active, outcome=TRIGGER_REUSED, reason=reason)

    def _audit(self, wf: Workflow | None, run: Run | None, action: str,
                scope: str, detail: str) -> None:
        owner = (run.owner if run else wf.owner if wf else "?")
        wf_id = (run.workflow_id if run else wf.id if wf else None)
        run_id = run.id if run else None
        category = (AUDIT_CONVERGENCE if action in (
            "REUSED_BY_KEY", "REUSED_ACTIVE", "DUPLICATE_RUN",
            "ACTIVE_CONFLICT_REJECTED", "SCHEDULED_SLOT_SKIPPED",
            "QUEUE_FULL") else AUDIT_RETENTION)
        self.storage.log_audit(owner, wf_id, run_id, category, action,
                              scope, detail)

    # ---- 历史保留策略 ----
    @staticmethod
    def validate_retention(retain_last_runs: int | None,
                              retain_seconds: float | None,
                              tombstone_limit: int) -> None:
        if retain_last_runs is not None and retain_last_runs < 0:
            raise Rejected(RejectReason.INVALID_POLICY,
                           "retain_last_runs must be >= 0 or null")
        if retain_seconds is not None and retain_seconds <= 0:
            raise Rejected(RejectReason.INVALID_POLICY,
                           "retain_seconds must be > 0 or null")
        if tombstone_limit < 0:
            raise Rejected(RejectReason.INVALID_POLICY,
                           "tombstone_limit must be >= 0")

    def retention_for(self, wf: Workflow):
        """生效的保留策略：工作流配置优先，否则取全局默认。"""
        spec = wf.spec
        last_n = (spec.retain_last_runs
                  if spec.retain_last_runs is not None
                  else self.settings.default_retain_last_runs)
        seconds = (spec.retain_seconds
                  if spec.retain_seconds is not None
                  else self.settings.default_retain_seconds)
        tomb = (spec.tombstone_limit if spec.tombstone_limit
                else self.settings.default_tombstone_limit)
        return last_n, seconds, tomb

    def update_retention_policy(self, owner: str, workflow_name: str,
                                  retain_last_runs, retain_seconds,
                                  tombstone_limit=None) -> Workflow:
        wf = self.storage.get_workflow_by_name(owner, workflow_name)
        if not wf:
            raise Rejected(RejectReason.WORKFLOW_NOT_FOUND, workflow_name)
        if tombstone_limit is None:
            tombstone_limit = (wf.spec.tombstone_limit
                                 or self.settings.default_tombstone_limit)
        self.validate_retention(retain_last_runs, retain_seconds,
                                  tombstone_limit)
        spec = wf.spec
        spec.retain_last_runs = retain_last_runs
        spec.retain_seconds = retain_seconds
        spec.tombstone_limit = tombstone_limit
        self.storage.update_workflow_spec(wf)
        self._spec_cache[wf.id] = spec
        self.storage.log_audit(
            owner, wf.id, None, AUDIT_RETENTION, "POLICY_UPDATED",
            f"workflow={workflow_name}",
            f"retain_last_runs={retain_last_runs} "
            f"retain_seconds={retain_seconds} tombstone_limit={tombstone_limit}")
        logger.info("retention policy updated: %s last=%s seconds=%s",
                    workflow_name, retain_last_runs, retain_seconds)
        return wf

    def enforce_retention(self, wf: Workflow) -> int:
        """对单个工作流执行一轮保留清理（后台调用）。

        只选终态运行；单批最多 batch_size 条，由单事务原子删除：
        运行 + 任务明细 + 执行事件同生共死，不留半条记录。
        """
        last_n, seconds, tomb = self.retention_for(wf)
        if last_n is None and seconds is None:
            return 0  # 未配置保留策略：存储不上界由调用方决定，这里不清理
        older_than = now() - seconds if seconds is not None else None
        candidates = self.storage.list_purge_candidates(
            wf.id, last_n, older_than,
            self.settings.retention_batch_size)
        if not candidates:
            return 0
        run_ids = [r.id for r in candidates]
        scope = (f"workflow={wf.name} batch={len(candidates)} "
                 f"runs={run_ids[:5]}{'...' if len(run_ids) > 5 else ''}")
        reason = (f"retention policy: retain_last_runs={last_n}, "
                  f"retain_seconds={seconds}")
        deleted = self.storage.purge_runs_atomic(candidates, reason, tomb)
        if deleted:
            statuses = sorted({r.status.value for r in candidates})
            self.storage.log_audit(
                wf.owner, wf.id, None, AUDIT_RETENTION, "PURGED",
                scope,
                f"deleted={deleted} terminal_statuses={statuses}; {reason}")
            logger.info("retention purge: %s deleted %d runs",
                        wf.name, deleted)
        return deleted

    def enforce_retention_once(self) -> int:
        """遍历所有工作流执行一轮清理，返回总删除条数（分批，避免长时间持锁）。"""
        total = 0
        for wf in self.storage.list_all_workflows():
            total += self.enforce_retention(wf)
        return total

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

    # ---- 查询（含隔离与清理后语义） ----
    def _resolve_owned_run(self, owner: str, run_id: str) -> Run:
        """解析 run 的可见性，三种语义严格区分：

        - run 存在且属于 owner           -> 返回 run；
        - run 属于他人 / 不存在且无墓碑   -> 统一 FORBIDDEN（404，不泄露存在性）；
        - run 不存在但有自己的墓碑      -> RUN_PURGED（410，明确告知已被清理）。
        关键：先查墓碑也要校验 owner，他人的墓碑同样返回 FORBIDDEN，
        越权方无法区分"被清理"与"不存在"。
        """
        run = self.storage.get_run(run_id)
        if run is not None:
            if run.owner != owner:
                raise Rejected(RejectReason.FORBIDDEN,
                               "run belongs to another owner")
            return run
        tomb = self.storage.get_tombstone(run_id)
        if tomb is not None and tomb["owner"] == owner:
            raise Rejected(
                RejectReason.RUN_PURGED,
                f"run {run_id} existed but was purged by retention policy "
                f"at {tomb['purged_at']:.3f} ({tomb['reason']})",
                extra={"purged_at": tomb["purged_at"],
                       "terminal_status": tomb["terminal_status"],
                       "reason": tomb["reason"]})
        # 不存在，或墓碑属于他人：统一按越权/不存在处理，不泄露
        raise Rejected(RejectReason.RUN_NOT_FOUND, run_id)

    def _get_owned_run(self, owner: str, run_id: str) -> Run:
        return self._resolve_owned_run(owner, run_id)

    def get_run_view(self, owner: str, run_id: str) -> dict:
        run = self._resolve_owned_run(owner, run_id)
        tasks = self.storage.list_task_runs(run_id)
        return {"run": run, "tasks": tasks}

    def get_run_events(self, owner: str, run_id: str) -> list[dict]:
        self._resolve_owned_run(owner, run_id)
        return self.storage.list_events(run_id)

    def list_audit(self, owner: str, limit: int = 200) -> list[dict]:
        """查询本调用方自己的收敛/拒绝/清理可解释记录。"""
        return self.storage.list_audit(owner, limit)

    def list_tombstones(self, owner: str) -> list[dict]:
        """列出本调用方被保留策略清理的运行墓碑。"""
        return self.storage.list_tombstones(owner)

    # ---- 调度循环 ----
    def start(self) -> None:
        """启动后台调度线程、维护线程并执行崩溃恢复。"""
        self.recover()
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True,
                                        name="orch-scheduler")
        self._thread.start()
        # 保留清理/审计自收缩跑在独立的慢速守护线程，
        # 不占用调度 tick，避免拖慢触发与查询路径
        self._maint_thread = threading.Thread(
            target=self._maintenance_loop, daemon=True,
            name="orch-maintenance")
        self._maint_thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        if self._maint_thread:
            self._maint_thread.join(timeout=5)

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

    def _maintenance_loop(self) -> None:
        """独立慢速线程：保留清理与审计自收缩，与调度/触发解耦。

        清理分批、每批为独立短事务，因此不会长时间持锁卡住新触发与查询。
        """
        interval = max(0.01, min(self.settings.retention_interval_seconds,
                                  self.settings.purge_interval_seconds))
        last_retention = 0.0
        last_audit = 0.0
        while not self._stop.is_set():
            if self._stop.wait(interval):
                return
            t = now()
            if t - last_retention >= self.settings.retention_interval_seconds:
                last_retention = t
                try:
                    self.enforce_retention_once()
                except Exception:
                    logger.exception("retention sweep failed")
            if t - last_audit >= self.settings.purge_interval_seconds:
                last_audit = t
                try:
                    self.storage.trim_audit(
                        self.settings.audit_max_entries,
                        self.settings.audit_max_age_seconds)
                except Exception:
                    logger.exception("audit trim failed")

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
                outcome = self.trigger_detailed(
                    wf.owner, wf.name, idempotency_key=key,
                    trigger="schedule")
                self._sched_last[wf.id] = now()
                if outcome.outcome == TRIGGER_REUSED:
                    logger.info("scheduled trigger of %s converged: %s",
                                wf.name, outcome.reason)
            except Rejected as e:
                if e.reason == RejectReason.DUPLICATE_RUN:
                    self._sched_last[wf.id] = now()  # 本槽位已触发过
                elif e.reason == RejectReason.ACTIVE_RUN_CONFLICT:
                    # 与活跃运行（可能是手动触发）撞车：按同一套收敛规则拒绝，
                    # 本槽位跳过，原因已在 trigger_detailed 内写审计
                    self._sched_last[wf.id] = now()
                else:  # 积压等：按策略跳过本槽位，下周期再试
                    self.storage.log_event(
                        None, None, EV_RUN_REJECTED,
                        f"scheduled trigger of '{wf.name}' skipped: "
                        f"{e.reason.value}")
                    self._audit(wf, None, "SCHEDULED_SLOT_SKIPPED",
                                f"workflow={wf.name} slot={slot}",
                                f"scheduled trigger skipped: {e.reason.value}")
                    self._sched_last[wf.id] = now()
