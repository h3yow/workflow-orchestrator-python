"""失败重试、退避、执行超时、下游隔离的状态一致性测试。"""
from __future__ import annotations

import time
from datetime import datetime as _dt

from app.models import (
    RunState,
    RetryPolicy,
    TaskDef,
    TaskState,
    WorkflowDef,
)
from tests.conftest import COUNTERS, wait_terminal


def _task_states(eng, run_id, tenant="t1"):
    return {t.task_key: t.state for t in eng.list_tasks(tenant, run_id)}


def test_retry_then_success(engine):
    print("[输入] flaky 前2次失败、第3次成功，max_retries=3 退避0.01s；"
          "判定依据=最终 SUCCEEDED，attempt_count=3，handler 恰被调用3次")
    COUNTERS.fail_times["flaky"] = 2
    wf = WorkflowDef(name="w", version=1, tasks=(
        TaskDef(key="a", handler="flaky",
                retry=RetryPolicy(max_retries=3, backoff_seconds=0.01)),
    ))
    engine.register_workflow("t1", wf)
    run_id, _ = engine.trigger_run("t1", "w")
    run = wait_terminal(engine, run_id)
    assert run.state == RunState.SUCCEEDED
    task = engine.list_tasks("t1", run_id)[0]
    assert task.state == TaskState.SUCCEEDED
    assert task.attempt_count == 3
    assert COUNTERS.calls["flaky"] == 3
    attempts = task.attempts
    assert [a.state for a in attempts] == ["FAILED", "FAILED", "SUCCEEDED"]
    print(f"[判定] 尝试序列={[a.state for a in attempts]} 与预期一致")


def test_retry_exhausted_enters_failed(engine):
    print("[输入] 任务恒失败，max_retries=1（共2次尝试）；"
          "判定依据=run 与 task 均 FAILED，error_code=TASK_FAILED，恰好调用2次")
    COUNTERS.fail_times["flaky"] = 99
    wf = WorkflowDef(name="w", version=1, tasks=(
        TaskDef(key="a", handler="flaky",
                retry=RetryPolicy(max_retries=1, backoff_seconds=0.0)),
    ))
    engine.register_workflow("t1", wf)
    run_id, _ = engine.trigger_run("t1", "w")
    run = wait_terminal(engine, run_id)
    assert run.state == RunState.FAILED
    assert run.error_code == "TASK_FAILED"
    task = engine.list_tasks("t1", run_id)[0]
    assert task.state == TaskState.FAILED
    assert task.attempt_count == 2
    assert COUNTERS.calls["flaky"] == 2
    print("[判定] 重试耗尽后没有第3次调用，终态原因可区分")


def test_backoff_delays_retries(engine):
    print("[输入] flaky 首次失败，退避 0.2s；"
          "判定依据=第二次开始与第一次结束之间至少经过 0.2s")
    COUNTERS.fail_times["flaky"] = 1
    wf = WorkflowDef(name="w", version=1, tasks=(
        TaskDef(key="a", handler="flaky",
                retry=RetryPolicy(max_retries=2, backoff_seconds=0.2,
                                  backoff_multiplier=2.0)),
    ))
    engine.register_workflow("t1", wf)
    run_id, _ = engine.trigger_run("t1", "w")
    run = wait_terminal(engine, run_id)
    assert run.state == RunState.SUCCEEDED
    task = engine.list_tasks("t1", run_id)[0]
    first_end = task.attempts[0].finished_at
    second_start = task.attempts[1].started_at
    delta = (_dt.fromisoformat(second_start) - _dt.fromisoformat(first_end)).total_seconds()
    print(f"[判定] 首次失败结束到第二次开始间隔={delta:.3f}s，应 >=0.2s")
    assert delta >= 0.2 - 0.05  # 允许调度节拍误差


def test_timeout_enters_distinguishable_terminal(engine):
    print("[输入] slow 执行 0.5s，timeout=0.1s，不重试；"
          "判定依据=task TIMED_OUT、run FAILED 且 error_code=TIMED_OUT，"
          "与普通 TASK_FAILED 可区分")
    wf = WorkflowDef(name="w", version=1, tasks=(
        TaskDef(key="a", handler="slow", timeout_seconds=0.1),
    ))
    engine.register_workflow("t1", wf)
    run_id, _ = engine.trigger_run("t1", "w")
    run = wait_terminal(engine, run_id, timeout=3.0)
    assert run.state == RunState.FAILED
    assert run.error_code == "TIMED_OUT"
    task = engine.list_tasks("t1", run_id)[0]
    assert task.state == TaskState.TIMED_OUT
    assert task.attempts[0].timed_out is True
    print("[判定] 超时终态 TIMED_OUT 与失败终态 FAILED 可区分")


def test_downstream_not_executed_when_upstream_fails(engine):
    print("[输入] a 恒失败（无重试），b 依赖 a，c 依赖 b；"
          "判定依据=a=FAILED，b/c=SKIPPED 且 handler 从未被调用")
    COUNTERS.fail_times["flaky"] = 99
    wf = WorkflowDef(name="w", version=1, tasks=(
        TaskDef(key="a", handler="flaky"),
        TaskDef(key="b", handler="instant", depends_on=("a",)),
        TaskDef(key="c", handler="instant", depends_on=("b",)),
    ))
    engine.register_workflow("t1", wf)
    run_id, _ = engine.trigger_run("t1", "w")
    run = wait_terminal(engine, run_id)
    assert run.state == RunState.FAILED
    states = _task_states(engine, run_id)
    print(f"[判定] 任务状态={ {k: v.value for k, v in states.items()} }")
    assert states == {
        "a": TaskState.FAILED,
        "b": TaskState.SKIPPED,
        "c": TaskState.SKIPPED,
    }
    assert COUNTERS.calls.get("instant", 0) == 0


def test_timeout_then_retry_within_limit_succeeds(engine):
    print("[输入] 任务首次超时（sleep 0.15, timeout 0.05），第二次正常很快；"
          "判定依据=首次 attempt timed_out=True，最终 SUCCEEDED")
    import app.handlers as handlers_mod

    state = {"slow": True}

    def handler(params, context):
        if state["slow"]:
            state["slow"] = False
            time.sleep(0.15)
        return "fast-now"

    handlers_mod.register_handler("once_slow", handler)
    wf = WorkflowDef(name="w", version=1, tasks=(
        TaskDef(key="a", handler="once_slow", timeout_seconds=0.05,
                retry=RetryPolicy(max_retries=2, backoff_seconds=0.0)),
    ))
    engine.register_workflow("t1", wf)
    run_id, _ = engine.trigger_run("t1", "w")
    run = wait_terminal(engine, run_id, timeout=3.0)
    assert run.state == RunState.SUCCEEDED
    task = engine.list_tasks("t1", run_id)[0]
    assert task.attempts[0].timed_out is True
    assert task.state == TaskState.SUCCEEDED
    print(f"[判定] 超时后重试成功，attempts={len(task.attempts)}")


def test_idempotency_key_stable_per_attempt(engine):
    print("[输入] flaky 重试2次；判定依据=同一次尝试的 idempotency_key 稳定，"
          "不同尝试的 key 不同，可供处理器做副作用去重")
    COUNTERS.fail_times["flaky"] = 2
    wf = WorkflowDef(name="w", version=1, tasks=(
        TaskDef(key="a", handler="flaky",
                retry=RetryPolicy(max_retries=3, backoff_seconds=0.0)),
    ))
    engine.register_workflow("t1", wf)
    run_id, _ = engine.trigger_run("t1", "w")
    wait_terminal(engine, run_id)
    keys = list(COUNTERS.seen_idempotency_keys)
    print(f"[判定] 观测到 {len(keys)} 个不同幂等键，每个恰好对应一次尝试")
    assert len(keys) == 3
    assert all(len(v) == 1 for v in COUNTERS.seen_idempotency_keys.values())
