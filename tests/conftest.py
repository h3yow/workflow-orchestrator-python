"""测试共享夹具：临时数据库的引擎、可编排行为的示例处理器。

约定：每个测试的输入与判定依据都会 print 到 stdout（pytest -s 可见）。
"""
from __future__ import annotations

import logging
import os
import tempfile
import time

import pytest

from app.engine import Engine
from app.handlers import clear_handlers, register_handler

logging.basicConfig(level=logging.WARNING,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")


class Counters:
    """跨“重启”共享的处理器行为与调用计数（模块级单例）。"""

    def __init__(self):
        self.calls: dict[str, int] = {}
        self.fail_times: dict[str, int] = {}
        self.seen_idempotency_keys: dict[str, list[str]] = {}
        self.lock_log: list[str] = []
        self.reset()

    def reset(self):
        self.calls.clear()
        self.fail_times.clear()
        self.seen_idempotency_keys.clear()
        self.lock_log.clear()


COUNTERS = Counters()


def _counting_handler(task: str, slow: float = 0.0, fail_code: str = "TASK_FAILED"):
    def handler(params, context):
        COUNTERS.calls[task] = COUNTERS.calls.get(task, 0) + 1
        COUNTERS.seen_idempotency_keys.setdefault(
            context["idempotency_key"], []
        ).append(task)
        print(f"[handler] 输入 task={task} params={params} attempt="
              f"{context['attempt_no']} idem={context['idempotency_key'][:8]} "
              f"判定依据=第{COUNTERS.calls[task]}次调用")
        if slow:
            time.sleep(slow)
        if COUNTERS.calls[task] <= COUNTERS.fail_times.get(task, 0):
            raise RuntimeError(f"{fail_code}:{task} 第{COUNTERS.calls[task]}次故意失败")
        return {"task": task, "calls": COUNTERS.calls[task]}
    return handler


@pytest.fixture(autouse=True)
def _reset_counters():
    COUNTERS.reset()
    yield


@pytest.fixture
def handlers():
    clear_handlers()
    register_handler("instant", _counting_handler("instant"))
    register_handler("flaky", _counting_handler("flaky"))
    register_handler("flaky2", _counting_handler("flaky2"))
    register_handler("slow", _counting_handler("slow", slow=0.5))
    register_handler("medium", _counting_handler("medium", slow=0.15))
    register_handler("quick", _counting_handler("quick", slow=0.02))
    yield
    clear_handlers()


@pytest.fixture
def db_path():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    os.unlink(path)  # Engine 自己创建
    yield path
    for suffix in ("", "-wal", "-shm"):
        try:
            os.unlink(path + suffix)
        except FileNotFoundError:
            pass


@pytest.fixture
def engine(db_path, handlers):
    eng = Engine(db_path=db_path, max_workers=4, poll_interval=0.01,
                 retain_terminal_runs=50, start_scheduler=True)
    yield eng
    eng.shutdown()


def make_engine(db_path, **kw):
    defaults = dict(max_workers=4, poll_interval=0.01, retain_terminal_runs=50,
                    start_scheduler=True)
    defaults.update(kw)
    return Engine(db_path=db_path, **defaults)


def wait_terminal(eng, run_id, tenant="t1", timeout=5.0, interval=0.01):
    """轮询直到 run 进入终态，返回最终 RunInfo。"""
    from app.models import RunState
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = eng.get_run(tenant, run_id)
        print(f"[poll] run={run_id[:8]} state={last.state.value} "
              f"判定依据=等待终态")
        if last.state in (RunState.SUCCEEDED, RunState.FAILED):
            return last
        time.sleep(interval)
    raise AssertionError(f"运行未在 {timeout}s 内终结，最后状态={last}")
