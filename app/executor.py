"""任务执行器：在本地线程池中运行已注册的处理器，支持超时。

副作用安全约定：处理器接收的 payload 含 run_id/task_name/attempt，
处理器应据此做到幂等；调度器保证同一任务同一时刻只有一个尝试在执行，
重试只在前一次尝试明确结束后发起。
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout
from typing import Any, Callable

Handler = Callable[[dict[str, Any]], Any]


class HandlerRegistry:
    """处理器注册表（进程内，可测试替换）。"""

    def __init__(self) -> None:
        self._handlers: dict[str, Handler] = {}

    def register(self, name: str, fn: Handler) -> None:
        self._handlers[name] = fn

    def get(self, name: str) -> Handler:
        if name not in self._handlers:
            raise KeyError(f"handler not registered: {name}")
        return self._handlers[name]


class Executor:
    """提交任务到线程池并以超时等待结果。"""

    def __init__(self, max_workers: int) -> None:
        self._pool = ThreadPoolExecutor(max_workers=max_workers,
                                        thread_name_prefix="orch-task")

    def run(self, handler: Handler, payload: dict[str, Any],
            timeout_seconds: float | None) -> Any:
        """同步执行处理器；超时抛出 TimeoutError。"""
        future = self._pool.submit(handler, payload)
        try:
            return future.result(timeout=timeout_seconds)
        except FutureTimeout:
            # 线程无法被强杀，但调度器已将任务标记为 TIMED_OUT 终态，
            # 迟到的结果会被状态机拒绝（RUNNING 已离开），不会写脏状态。
            future.cancel()
            raise TimeoutError(
                f"task exceeded timeout of {timeout_seconds}s") from None

    def shutdown(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=True)
