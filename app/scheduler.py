"""调度器：后台线程周期性驱动整个状态机。

每一拍（tick）严格按以下顺序推进，且所有状态裁决都落在 SQLite 条件更新上：

1. 排队放行：PENDING run 按 FIFO 在同工作流无 RUNNING run 时转为 RUNNING。
2. 退避到期：RETRY_WAIT 任务按指数退避计算 next_run_at，到期转回 READY。
3. 依赖传播：PENDING 任务的全部依赖 SUCCEEDED 才 READY；
   任一依赖进入 FAILED/TIMED_OUT/SKIPPED 则该任务 SKIPPED（级联）。
4. 有界派发：READY 任务仅在工作线程有空位时提交（背压），
   READY->RUNNING 的原子领取保证同一任务绝不重复执行。
5. 终态收敛：所有任务到达终态后，run 转 SUCCEEDED 或 FAILED（原因可区分）。

重启恢复：启动时把上次崩溃时处于 RUNNING 的任务补记为 INTERRUPTED 尝试，
尚有重试余量则 RETRY_WAIT，否则 FAILED；RUNNING run 保持 RUNNING 继续调度，
PENDING run 继续排队；SUCCEEDED 任务不被触碰（绝不重复触发）。
"""
from __future__ import annotations

import json
import logging
import threading
from datetime import timedelta, timezone

from app.models import RunState, TaskState
from app.store import Store, utcnow_iso

_TERMINAL_TASK = {
    TaskState.SUCCEEDED, TaskState.FAILED,
    TaskState.TIMED_OUT, TaskState.SKIPPED,
}
_FAIL_TERMINAL = {TaskState.FAILED, TaskState.TIMED_OUT, TaskState.SKIPPED}


class Scheduler:
    def __init__(self, store: Store, executor, logger: logging.Logger,
                 max_workers: int = 4, poll_interval: float = 0.05,
                 retain_terminal_runs: int = 100):
        self.store = store
        self.executor = executor
        self.logger = logger
        self.max_workers = max_workers
        self.poll_interval = poll_interval
        self.retain_terminal_runs = retain_terminal_runs
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._tick_lock = threading.Lock()

    # -------------------------------------------------------------- 生命周期
    def start(self) -> None:
        self.recover()
        self._thread = threading.Thread(target=self._loop, name="wf-scheduler",
                                        daemon=True)
        self._thread.start()

    def stop(self, timeout: float | None = 5) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def _loop(self) -> None:
        while not self._stop.wait(self.poll_interval):
            try:
                self.tick()
            except Exception:  # noqa: BLE001 - 调度循环必须存活
                self.logger.exception("调度 tick 异常")

    def tick(self) -> None:
        with self._tick_lock:
            self._promote_queued_runs()
            self._schedule_retries()
            self._propagate_dependencies()
            self._dispatch_ready()
            self._finalize_runs()

    # ------------------------------------------------------------- 重启恢复
    def recover(self) -> None:
        interrupted = self.store.recover_interrupted_tasks()
        for task_id, tenant_id, run_id, attempt_no in interrupted:
            self.store.add_decision(
                tenant_id, "RECOVER_INTERRUPTED_TASK",
                f"重启恢复：任务在第 {attempt_no} 次尝试执行期间中断，"
                f"已补记尝试历史；若仍有重试余量则退避后重试，否则置为 FAILED",
                run_id, task_id,
            )
            self.logger.warning(
                "重启恢复中断任务 tenant=%s run=%s task=%s attempt=%d",
                tenant_id, run_id, task_id, attempt_no,
            )
        for run in self.store.list_queued_runs():
            self.store.add_decision(
                run.tenant_id, "RECOVER_QUEUED_RUN",
                "重启恢复：排队中的运行继续保留在队列中", run.id,
            )
        if interrupted:
            self.logger.info("重启恢复完成，中断任务数=%d", len(interrupted))

    # ----------------------------------------------------------- 1. 排队放行
    def _promote_queued_runs(self) -> None:
        for run in self.store.list_queued_runs(limit=100):
            if self.store.count_running_runs(run.tenant_id, run.workflow_name) == 0:
                self.store.mark_run_started(run.id)
                self.store.add_decision(
                    run.tenant_id, "RUN_STARTED",
                    f"排队放行，运行开始（触发方式={run.trigger}）", run.id,
                )
                self.logger.info("运行开始 tenant=%s run=%s wf=%s trigger=%s",
                                 run.tenant_id, run.id, run.workflow_name, run.trigger)

    # ------------------------------------------------------------- 2. 重试
    def _schedule_retries(self) -> None:
        now = utcnow_iso()
        for r in self.store.list_retry_wait_rows():
            definition = self.store.get_run_definition_snapshot(r["run_id"])
            task_def = next(
                (t for t in definition.tasks if t.key == r["task_key"]), None
            )
            delay = (
                task_def.retry.delay_for(r["attempt_count"])
                if task_def and task_def.retry else 0.0
            )
            base = _parse_iso(r["finished_at"] or now)
            nxt = (base + timedelta(seconds=delay)).astimezone(timezone.utc).isoformat()
            self.store.set_task_next_run_at(r["id"], nxt)
            self.store.add_decision(
                r["tenant_id"], "TASK_BACKOFF",
                f"任务 {r['task_key']} 退避 {delay:.3f}s 后重试，计划时刻={nxt}",
                r["run_id"], r["id"],
            )
        due = self.store.get_retry_due_task_ids(now)
        if due:
            self.store.mark_tasks_ready(due)
            for tid in due:
                row = self.store.get_task_row(tid)
                self.store.add_decision(
                    row["tenant_id"], "TASK_DISPATCH_CANDIDATE",
                    "退避结束，任务重新进入 READY 等待派发", row["run_id"], tid,
                )

    # -------------------------------------------------------- 3. 依赖传播
    def _propagate_dependencies(self) -> None:
        for run_id, tenant_id in self.store.list_runs_with_pending_tasks():
            run = self.store.get_run(tenant_id, run_id)
            if run is None or run.state != RunState.RUNNING:
                continue
            tasks = self.store.list_task_rows(run.id)
            states = {t["task_key"]: TaskState(t["state"]) for t in tasks}
            ready: list[str] = []
            skipped: list[tuple[str, str]] = []
            for t in tasks:
                if states[t["task_key"]] != TaskState.PENDING:
                    continue
                deps = json.loads(t["depends_json"])
                dep_states = [states.get(d) for d in deps]
                if any(s in _FAIL_TERMINAL for s in dep_states):
                    failed_deps = [d for d, s in zip(deps, dep_states)
                                   if s in _FAIL_TERMINAL]
                    skipped.append((t["id"],
                                    f"前置任务失败，跳过：{', '.join(failed_deps)}"))
                elif all(s == TaskState.SUCCEEDED for s in dep_states):
                    # 无依赖时 dep_states 为空，all(...) 也为 True
                    ready.append(t["id"])
            if ready:
                self.store.mark_tasks_ready(ready)
                for tid in ready:
                    self.store.add_decision(
                        run.tenant_id, "TASK_DISPATCH_CANDIDATE",
                        "全部前置任务成功，任务进入 READY 等待派发", run.id, tid,
                    )
            for tid, reason in skipped:
                self.store.skip_task(tid, reason)
                self.store.add_decision(
                    run.tenant_id, "TASK_SKIPPED", reason, run.id, tid,
                )
                self.logger.info("任务因前置失败被跳过 tenant=%s run=%s task=%s",
                                 run.tenant_id, run.id, tid)

    # ----------------------------------------------------------- 4. 有界派发
    def _dispatch_ready(self) -> None:
        free = self.max_workers - self.executor.active_count()
        if free <= 0:
            return
        for r in self.store.list_ready_task_rows(free):
            self.executor.submit_ready(r["id"])

    # ----------------------------------------------------------- 5. 终态收敛
    def _finalize_runs(self) -> None:
        for run_id, tenant_id in self.store.list_all_run_ids():
            run = self.store.get_run(tenant_id, run_id)
            if run is None or run.state != RunState.RUNNING:
                continue
            tasks = self.store.list_task_rows(run.id)
            states = [TaskState(t["state"]) for t in tasks]
            if not states or any(s not in _TERMINAL_TASK for s in states):
                continue
            if any(s in _FAIL_TERMINAL for s in states):
                # 根因优先：FAILED/TIMED_OUT（真实失败）优先于 SKIPPED（连带跳过）
                bad = next(
                    (t for t in tasks
                     if TaskState(t["state"]) in
                     (TaskState.FAILED, TaskState.TIMED_OUT)),
                    None,
                )
                if bad is None:  # 只有 SKIPPED 的极端情况（理论上不会发生）
                    bad = next(t for t in tasks
                               if TaskState(t["state"]) in _FAIL_TERMINAL)
                code = bad["error_code"] or "FAILED"
                message = (f"任务 {bad['task_key']} 终态={bad['state']}："
                           f"{bad['error_message'] or ''}")
                if self.store.transition_run(
                    run.id, {RunState.RUNNING}, RunState.FAILED, code, message,
                ):
                    self.store.add_decision(
                        run.tenant_id, "RUN_FAILED",
                        f"运行失败：{message}（code={code}）", run.id,
                    )
                    self.logger.error("运行失败 tenant=%s run=%s code=%s",
                                      run.tenant_id, run.id, code)
            elif self.store.transition_run(
                run.id, {RunState.RUNNING}, RunState.SUCCEEDED,
            ):
                self.store.add_decision(
                    run.tenant_id, "RUN_SUCCEEDED", "全部任务成功，运行完成", run.id,
                )
                self.logger.info("运行成功 tenant=%s run=%s", run.tenant_id, run.id)
            self.store.prune_history(self.retain_terminal_runs)


def _parse_iso(s: str):
    from datetime import datetime

    return datetime.fromisoformat(s)
