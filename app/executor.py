"""任务执行器：有界线程池中执行单个任务。

- store.claim_ready_task 的条件 UPDATE 原子领取（READY -> RUNNING），配合进程内
  _active 集合，保证同一任务同一时刻只会被执行一次（跨线程/跨调度循环成立）。
- 单一收割线程（drain loop）是唯一记账方：任务完成则记 SUCCEEDED/FAILED；
  超过 timeout_seconds 则记 TIMED_OUT，并把后续迟到结果丢弃。
  Python 无法强杀线程，超时任务的线程可能仍在后台运行（协作式超时限制）。
- 每次尝试都写 task_attempts；执行期间崩溃则由 Store.recover_interrupted_tasks
  在重启时补记 INTERRUPTED，历史完整可解释。
- 幂等：同一任务的同一次尝试拥有稳定 idempotency_key（run|task|attempt），
  供有副作用的处理器自身去重；调度层保证不会对同一 attempt 重复 submit。
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
import logging
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor

from app.errors import WorkflowError
from app.handlers import get_handler
from app.models import TaskState
from app.store import Store, utcnow_iso


@dataclasses.dataclass
class _Active:
    future: Future
    row: object
    attempt_no: int
    started_iso: str
    started_mono: float
    timeout: float | None


class Executor:
    def __init__(self, store: Store, logger: logging.Logger, max_workers: int = 4,
                 poll_interval: float = 0.02):
        self.store = store
        self.logger = logger
        self.pool = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="wf-task"
        )
        self._active: dict[str, _Active] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._poll_interval = poll_interval
        self._drain = threading.Thread(target=self._drain_loop, name="wf-drain",
                                       daemon=True)
        self._drain.start()

    # ---------------------------------------------------------------- 提交
    def submit_ready(self, task_id: str) -> bool:
        """提交一个 READY 任务；已在执行或领取失败时返回 False。"""
        with self._lock:
            if task_id in self._active:
                return False
        row = self.store.claim_ready_task(task_id)
        if row is None:
            return False
        attempt_no = row["attempt_count"]
        idem = hashlib.sha256(
            f"{row['run_id']}|{row['task_key']}|{attempt_no}".encode()
        ).hexdigest()
        context = {
            "run_id": row["run_id"],
            "task_key": row["task_key"],
            "tenant_id": row["tenant_id"],
            "attempt_no": attempt_no,
            "idempotency_key": idem,
            "previous_attempt": attempt_no > 1,
        }
        started_iso = utcnow_iso()
        future = self.pool.submit(_run_handler, row["handler"], row["params_json"], context)
        with self._lock:
            # 双检：理论上 claim 已排他，这里仅防御
            if task_id in self._active:  # pragma: no cover - 防御分支
                return False
            self._active[task_id] = _Active(
                future=future, row=row, attempt_no=attempt_no,
                started_iso=started_iso, started_mono=time.monotonic(),
                timeout=row["timeout_seconds"],
            )
        self.logger.info(
            "任务开始执行 tenant=%s run=%s task=%s attempt=%d timeout=%s",
            row["tenant_id"], row["run_id"], row["task_key"], attempt_no,
            row["timeout_seconds"],
        )
        return True

    def active_count(self) -> int:
        with self._lock:
            return len(self._active)

    # ------------------------------------------------------------- 收割循环
    def _drain_loop(self) -> None:
        while not self._stop.wait(self._poll_interval):
            try:
                self.reap()
            except Exception:  # noqa: BLE001 - 收割循环不能退出
                self.logger.exception("收割循环异常")

    def reap(self) -> None:
        now = time.monotonic()
        done_ids: list[str] = []
        timed_out_ids: list[str] = []
        with self._lock:
            for task_id, act in list(self._active.items()):
                if act.future.done():
                    done_ids.append(task_id)
                elif act.timeout is not None and now - act.started_mono >= act.timeout:
                    timed_out_ids.append(task_id)
        for task_id in done_ids:
            with self._lock:
                act = self._active.pop(task_id, None)
            if act is not None:
                self._resolve_done(act)
        for task_id in timed_out_ids:
            with self._lock:
                act = self._active.pop(task_id, None)
            if act is not None:
                self._resolve_timeout(act)

    def _resolve_done(self, act: _Active) -> None:
        row = act.row
        finished = utcnow_iso()
        try:
            act.future.result(0)
        except Exception as exc:  # noqa: BLE001 - 任务异常需落库并区分
            code = getattr(exc, "code", "TASK_FAILED")
            self._record_failure(row, act.attempt_no, act.started_iso, finished,
                                 code, str(exc), timed_out=False)
            return
        self._record_success(row, act.attempt_no, act.started_iso, finished)

    def _resolve_timeout(self, act: _Active) -> None:
        row = act.row
        finished = utcnow_iso()
        self.logger.warning(
            "任务超时 tenant=%s run=%s task=%s attempt=%d timeout=%s",
            row["tenant_id"], row["run_id"], row["task_key"], act.attempt_no,
            act.timeout,
        )
        self._record_failure(
            row, act.attempt_no, act.started_iso, finished,
            "TIMED_OUT", f"执行超过 {act.timeout}s 超时", timed_out=True,
        )
        # 迟到的完成结果会被丢弃：future 已不在 _active 中，收割线程不再记账。
        act.future.add_done_callback(lambda f: _swallow(f))

    # --------------------------------------------------------------- 记账
    def _record_success(self, row, attempt_no, started, finished) -> None:
        self.store.insert_attempt(
            row["id"], attempt_no, TaskState.SUCCEEDED, started, finished,
            None, None, False,
        )
        self.store.update_task_after_attempt(
            row["id"], new_state=TaskState.SUCCEEDED, finished_at=finished,
            next_run_at=None, error_code=None, error_message=None,
        )
        self.store.add_decision(
            row["tenant_id"], "TASK_SUCCEEDED",
            f"任务 {row['task_key']} 第 {attempt_no} 次尝试成功",
            row["run_id"], row["id"],
        )
        self.logger.info("任务成功 tenant=%s run=%s task=%s attempt=%d",
                         row["tenant_id"], row["run_id"], row["task_key"], attempt_no)

    def _record_failure(self, row, attempt_no, started, finished,
                        code, message, timed_out) -> None:
        attempt_state = TaskState.TIMED_OUT if timed_out else TaskState.FAILED
        self.store.insert_attempt(
            row["id"], attempt_no, attempt_state, started, finished,
            code, message, timed_out,
        )
        if attempt_no < row["max_attempts"]:
            new_state = TaskState.RETRY_WAIT
            detail = (f"任务 {row['task_key']} 第 {attempt_no} 次尝试"
                      f"{'超时' if timed_out else '失败'}({code}: {message})，"
                      f"将按退避策略重试（剩余 {row['max_attempts'] - attempt_no} 次）")
            self.store.add_decision(row["tenant_id"], "TASK_RETRY_SCHEDULED", detail,
                                    row["run_id"], row["id"])
        else:
            new_state = TaskState.TIMED_OUT if timed_out else TaskState.FAILED
            detail = (f"任务 {row['task_key']} 第 {attempt_no} 次尝试"
                      f"{'超时' if timed_out else '失败'}({code}: {message})，"
                      f"重试已耗尽，进入终态 {new_state.value}")
            self.store.add_decision(row["tenant_id"], "TASK_TERMINAL_FAILURE", detail,
                                    row["run_id"], row["id"])
            self.logger.error("任务终态失败 tenant=%s run=%s task=%s code=%s",
                              row["tenant_id"], row["run_id"], row["task_key"], code)
        self.store.update_task_after_attempt(
            row["id"], new_state=new_state, finished_at=finished,
            next_run_at=None, error_code=code, error_message=message,
        )

    # ---------------------------------------------------------------- 生命周期
    def wait_idle(self, timeout: float | None = None) -> bool:
        """等待在途任务全部被记账，返回是否已空闲。"""
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            self.reap()
            if self.active_count() == 0:
                return True
            if deadline is not None and time.monotonic() >= deadline:
                return False
            time.sleep(min(self._poll_interval, 0.01))

    def shutdown(self, wait: bool = True) -> None:
        self._stop.set()
        if wait:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and self.active_count() > 0:
                self.reap()
                time.sleep(0.01)
        self.pool.shutdown(wait=False)


def _swallow(future: Future) -> None:
    try:
        future.result(0)
    except Exception:  # noqa: BLE001 - 超时任务的迟到异常必须吞掉
        pass


def _run_handler(handler_name: str, params_json: str, context: dict):
    handler = get_handler(handler_name)
    if handler is None:
        raise WorkflowError("HANDLER_NOT_FOUND", f"未注册的处理器: {handler_name}")
    params = json.loads(params_json)
    return handler(params, context)
