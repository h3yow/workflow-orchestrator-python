"""无依赖任务并行执行；超时前置同样会阻断下游。"""
from __future__ import annotations

import threading
import time

from app.models import (
    RetryPolicy,
    RunState,
    TaskDef,
    TaskState,
    WorkflowDef,
)
from tests.conftest import wait_terminal


def test_independent_tasks_run_in_parallel(engine):
    print("[输入] 3 个互不依赖、各睡 0.2s 的任务，4 个工作线程；"
          "判定依据=总耗时显著小于串行的 0.6s（并行），且全部成功")
    import app.handlers as handlers_mod

    active = {"n": 0, "max": 0}
    lock = threading.Lock()

    def slow(params, context):
        with lock:
            active["n"] += 1
            active["max"] = max(active["max"], active["n"])
        time.sleep(0.2)
        with lock:
            active["n"] -= 1
        return "ok"

    handlers_mod.register_handler("par", slow)
    wf = WorkflowDef(name="w", version=1, tasks=tuple(
        TaskDef(key=f"t{i}", handler="par") for i in range(3)
    ))
    engine.register_workflow("t1", wf)
    rid, _ = engine.trigger_run("t1", "w")
    start = time.monotonic()
    run = wait_terminal(engine, rid)
    elapsed = time.monotonic() - start
    print(f"[判定] 总耗时={elapsed:.2f}s，观测峰值并行度={active['max']}")
    assert run.state == RunState.SUCCEEDED
    assert active["max"] >= 2, "无依赖任务未被并行调度"
    assert elapsed < 0.5, "耗时接近串行，疑似被错误地串行化"


def test_timeout_blocks_downstream(engine):
    print("[输入] a 超时（无重试）-> b 依赖 a；"
          "判定依据=a=TIMED_OUT，b=SKIPPED，run FAILED 码=TIMED_OUT")
    wf = WorkflowDef(name="w", version=1, tasks=(
        TaskDef(key="a", handler="slow", timeout_seconds=0.05),
        TaskDef(key="b", handler="instant", depends_on=("a",)),
    ))
    engine.register_workflow("t1", wf)
    rid, _ = engine.trigger_run("t1", "w")
    run = wait_terminal(engine, rid, timeout=3.0)
    assert run.state == RunState.FAILED
    assert run.error_code == "TIMED_OUT"
    states = {t.task_key: t.state for t in engine.list_tasks("t1", rid)}
    print(f"[判定] 状态={ {k: v.value for k, v in states.items()} }")
    assert states["a"] == TaskState.TIMED_OUT
    assert states["b"] == TaskState.SKIPPED
