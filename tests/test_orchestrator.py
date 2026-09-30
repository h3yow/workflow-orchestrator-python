"""调度引擎核心场景：成环、并发去重、重试、超时、重启恢复。"""
import threading
import time

import pytest

from app.executor import HandlerRegistry
from app.models import (
    InvalidTransition, Rejected, RejectReason, RunStatus, TaskSpec,
    TaskStatus, RUN_TRANSITIONS, WorkflowSpec, check_transition,
)
from app.scheduler import Orchestrator
from app.storage import Storage
from app.config import Settings
from app.executor import Executor

from .conftest import explain


def ok_handler(payload):
    return "ok"


def wait_run(storage, run_id, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        run = storage.get_run(run_id)
        if run.status in (RunStatus.SUCCEEDED, RunStatus.FAILED,
                          RunStatus.CANCELLED):
            return run
        time.sleep(0.01)
    raise AssertionError(f"run {run_id} did not finish in {timeout}s")


def test_unknown_dependency_rejected(make_orch):
    orch = make_orch(registry=_reg())
    spec = WorkflowSpec(name="w", owner="o", tasks=[
        TaskSpec(name="a", handler="ok", depends_on=["ghost"])])
    explain(f"输入: 任务 a 依赖不存在的 'ghost'; 期望: UNKNOWN_TASK")
    with pytest.raises(Rejected) as ei:
        orch.register_workflow(spec)
    explain(f"判定依据: reason={ei.value.reason}, detail={ei.value.detail}")
    assert ei.value.reason == RejectReason.UNKNOWN_TASK


def test_dependency_cycle_rejected(make_orch):
    orch = make_orch(registry=_reg())
    spec = WorkflowSpec(name="w", owner="o", tasks=[
        TaskSpec(name="a", handler="ok", depends_on=["b"]),
        TaskSpec(name="b", handler="ok", depends_on=["a"])])
    explain("输入: a<->b 互相依赖; 期望: DEPENDENCY_CYCLE")
    with pytest.raises(Rejected) as ei:
        orch.register_workflow(spec)
    explain(f"判定依据: reason={ei.value.reason}, detail={ei.value.detail}")
    assert ei.value.reason == RejectReason.DEPENDENCY_CYCLE
    assert "cycle" in ei.value.detail


def test_concurrent_duplicate_trigger_dedup(make_orch):
    orch = make_orch(registry=_reg(), duplicate_run_policy="dedup")
    orch.register_workflow(WorkflowSpec(name="w", owner="o", tasks=[
        TaskSpec(name="a", handler="ok")]))
    orch.start()
    results, errors = [], []

    def fire():
        try:
            results.append(orch.trigger("o", "w", idempotency_key="same").id)
        except Rejected as e:
            errors.append(e)

    threads = [threading.Thread(target=fire) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    explain(f"输入: 20 个线程以同一幂等键并发触发; "
            f"结果: {len(results)} 次返回, run_ids 去重后={set(results)}")
    assert not errors
    assert len(set(results)) == 1, "同一幂等键必须去重为同一次运行"
    runs = orch.storage.list_runs("o")
    explain(f"判定依据: 存储中运行数={len(runs)}（应为 1）")
    assert len(runs) == 1
    orch.stop()


def test_concurrent_duplicate_trigger_reject_policy(make_orch):
    orch = make_orch(registry=_reg(), duplicate_run_policy="reject")
    orch.register_workflow(WorkflowSpec(name="w", owner="o", tasks=[
        TaskSpec(name="a", handler="ok")]))
    orch.start()
    ok, rejected = [], []

    def fire():
        try:
            ok.append(orch.trigger("o", "w", idempotency_key="k").id)
        except Rejected as e:
            rejected.append(e.reason)

    threads = [threading.Thread(target=fire) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    explain(f"输入: reject 策略下 10 线程同键触发; "
            f"结果: 成功={len(ok)}, 拒绝={len(rejected)}")
    assert len(ok) == 1
    assert rejected == [RejectReason.DUPLICATE_RUN] * 9
    orch.stop()


def test_retry_with_backoff_then_success(make_orch):
    attempts = []

    def flaky(payload):
        attempts.append(payload["attempt"])
        if payload["attempt"] < 3:
            raise ValueError(f"boom at attempt {payload['attempt']}")

    reg = _reg()
    reg.register("flaky", flaky)
    orch = make_orch(registry=reg)
    orch.register_workflow(WorkflowSpec(name="w", owner="o", tasks=[
        TaskSpec(name="t", handler="flaky", max_attempts=3,
                 backoff_seconds=0.01)]))
    orch.start()
    run = orch.trigger("o", "w")
    final = wait_run(orch.storage, run.id)
    explain(f"输入: 前 2 次失败第 3 次成功, max_attempts=3; "
            f"尝试序列={attempts}, 最终状态={final.status}")
    assert final.status == RunStatus.SUCCEEDED
    assert attempts == [1, 2, 3], "重试不得跳过或重复尝试序号"
    events = [e for e in orch.storage.list_events(run.id)
              if e["kind"] == "TASK_RETRY"]
    explain(f"判定依据: 重试事件数={len(events)}, 原因={[e['message'] for e in events]}")
    assert len(events) == 2
    orch.stop()


def test_retry_exhausted_fails_and_skips_downstream(make_orch):
    reg = _reg()
    reg.register("bad", lambda p: (_ for _ in ()).throw(RuntimeError("always")))
    orch = make_orch(registry=reg)
    orch.register_workflow(WorkflowSpec(name="w", owner="o", tasks=[
        TaskSpec(name="a", handler="bad", max_attempts=2,
                 backoff_seconds=0.01),
        TaskSpec(name="b", handler="ok", depends_on=["a"])]))
    orch.start()
    run = orch.trigger("o", "w")
    final = wait_run(orch.storage, run.id)
    tasks = {t.name: t for t in orch.storage.list_task_runs(run.id)}
    explain(f"输入: a 永远失败(max_attempts=2), b 依赖 a; "
            f"结果: run={final.status}, a={tasks['a'].status}, b={tasks['b'].status}")
    assert final.status == RunStatus.FAILED
    assert tasks["a"].status == TaskStatus.FAILED
    assert tasks["a"].attempt == 2
    assert tasks["b"].status == TaskStatus.SKIPPED, "前置失败下游不得执行"
    orch.stop()


def test_timeout_enters_timed_out_terminal(make_orch):
    reg = _reg()
    reg.register("slow", lambda p: time.sleep(2))
    orch = make_orch(registry=reg)
    orch.register_workflow(WorkflowSpec(name="w", owner="o", tasks=[
        TaskSpec(name="t", handler="slow", timeout_seconds=0.05,
                 max_attempts=3),
        TaskSpec(name="d", handler="ok", depends_on=["t"])]))
    orch.start()
    run = orch.trigger("o", "w")
    final = wait_run(orch.storage, run.id)
    tasks = {t.name: t for t in orch.storage.list_task_runs(run.id)}
    explain(f"输入: 任务睡眠 2s 但超时 0.05s; "
            f"结果: run={final.status}, t={tasks['t'].status}, d={tasks['d'].status}")
    assert final.status == RunStatus.FAILED
    assert tasks["t"].status == TaskStatus.TIMED_OUT, "超时是可区分终态"
    assert tasks["t"].attempt == 1, "超时不再重试"
    assert tasks["d"].status == TaskStatus.SKIPPED
    orch.stop()


def test_restart_recovery(make_orch, tmp_path):
    """进程重启：已成功任务不重复执行，中断任务恢复，历史可解释。"""
    db = str(tmp_path / "crash.db")
    calls = {"fast": 0, "block": 0}
    release = threading.Event()
    started = threading.Event()

    def fast(p):
        calls["fast"] += 1

    def block(p):
        calls["block"] += 1
        started.set()
        release.wait(timeout=5)

    def build():
        reg = _reg()
        reg.register("fast", fast)
        reg.register("block", block)
        storage = Storage(db)
        return Orchestrator(storage, Executor(2),
                            Settings(db_path=db, tick_interval_seconds=0.01),
                            reg)

    spec = WorkflowSpec(name="w", owner="o", tasks=[
        TaskSpec(name="fast", handler="fast"),
        TaskSpec(name="block", handler="block", depends_on=["fast"],
                 max_attempts=2, backoff_seconds=0.01)])
    orch1 = build()
    orch1.register_workflow(spec)
    orch1.start()
    run = orch1.trigger("o", "w")
    assert started.wait(timeout=5), "block 任务应已开始执行"
    # 等待 fast 成功落库，然后模拟崩溃（不等待 block 完成）
    deadline = time.time() + 5
    while time.time() < deadline:
        t = orch1.storage.get_task_run(run.id, "fast")
        if t.status == TaskStatus.SUCCEEDED:
            break
        time.sleep(0.01)
    orch1.stop()  # 调度线程停止；block 的执行线程仍挂起 => 模拟崩溃
    explain(f"崩溃时点: fast 调用={calls['fast']}(已成功), "
            f"block 调用={calls['block']}(执行中中断)")

    # 重启：新的 Orchestrator 实例加载同一数据库
    orch2 = build()
    orch2.start()
    release.set()  # 放行旧线程，其迟到结果不得写脏状态
    final = wait_run(orch2.storage, run.id)
    tasks = {t.name: t for t in orch2.storage.list_task_runs(run.id)}
    explain(f"恢复后: run={final.status}, fast={tasks['fast'].status}"
            f"(调用 {calls['fast']} 次), block={tasks['block'].status}"
            f"(调用 {calls['block']} 次)")
    assert final.status == RunStatus.SUCCEEDED
    assert calls["fast"] == 1, "已成功任务不得被重复触发"
    assert calls["block"] == 2, "中断任务应被恢复重试而非丢弃"
    recovery = [e for e in orch2.storage.list_events(run.id)
                if e["kind"] == "RECOVERY"]
    explain(f"判定依据: 恢复事件={[e['message'] for e in recovery]}")
    assert any("block" in (e["task"] or "") for e in recovery)
    orch2.stop()


def test_queue_full_rejects_new_runs(make_orch):
    reg = _reg()
    reg.register("slow", lambda p: time.sleep(1))
    orch = make_orch(registry=reg, max_pending_runs=1)
    orch.register_workflow(WorkflowSpec(name="w", owner="o", tasks=[
        TaskSpec(name="t", handler="slow")]))
    orch.start()
    orch.trigger("o", "w", idempotency_key="first")
    explain("输入: 积压上限=1 且已有 1 个活跃运行; 期望: QUEUE_FULL")
    with pytest.raises(Rejected) as ei:
        orch.trigger("o", "w", idempotency_key="second")
    explain(f"判定依据: reason={ei.value.reason}")
    assert ei.value.reason == RejectReason.QUEUE_FULL
    orch.stop()


def test_illegal_transition_rejected():
    explain("输入: 终态 SUCCEEDED -> RUNNING; 期望: InvalidTransition")
    with pytest.raises(InvalidTransition) as ei:
        check_transition(RUN_TRANSITIONS, RunStatus.SUCCEEDED,
                         RunStatus.RUNNING, "run")
    explain(f"判定依据: {ei.value}")
    assert "SUCCEEDED" in str(ei.value) and "RUNNING" in str(ei.value)


def test_owner_isolation(make_orch):
    orch = make_orch(registry=_reg())
    orch.register_workflow(WorkflowSpec(name="w", owner="alice", tasks=[
        TaskSpec(name="a", handler="ok")]))
    orch.start()
    run = orch.trigger("alice", "w")
    explain("输入: bob 读取 alice 的运行; 期望: 与'不存在'完全一致的 "
            "RUN_NOT_FOUND（不泄露存在性）")
    with pytest.raises(Rejected) as ei:
        orch.get_run_view("bob", run.id)
    explain(f"判定依据: reason={ei.value.reason} detail={ei.value.detail!r} "
            f"extra={ei.value.extra}")
    assert ei.value.reason == RejectReason.RUN_NOT_FOUND
    # 与"从未存在"的响应逐项一致：reason/detail/extra 无任何差别
    with pytest.raises(Rejected) as ei2:
        orch.get_run_view("bob", "never-existed-id")
    assert (ei2.value.reason, ei2.value.detail, ei2.value.extra) == \
        (ei.value.reason, ei.value.detail, ei.value.extra)
    assert orch.storage.list_runs("bob") == []
    orch.stop()


def _reg() -> HandlerRegistry:
    reg = HandlerRegistry()
    reg.register("ok", ok_handler)
    return reg


def test_scheduled_trigger_fires_and_dedups(make_orch):
    orch = make_orch(registry=_reg())
    orch.register_workflow(WorkflowSpec(
        name="cron", owner="o", schedule_interval_seconds=0.05,
        tasks=[TaskSpec(name="a", handler="ok")]))
    orch.start()
    deadline = time.time() + 5
    while time.time() < deadline:
        runs = [r for r in orch.storage.list_runs("o")
                if r.trigger == "schedule"]
        if len(runs) >= 2:
            break
        time.sleep(0.02)
    time.sleep(0.1)
    # 停止调度前，等待已触发的定时运行全部进入终态，避免与 tick 的时序竞态
    settle = time.time() + 5
    while time.time() < settle:
        runs = [r for r in orch.storage.list_runs("o")
                if r.trigger == "schedule"]
        if runs and all(r.status in (RunStatus.SUCCEEDED, RunStatus.FAILED,
                                      RunStatus.CANCELLED)
                          for r in runs):
            break
        time.sleep(0.01)
    orch.stop()
    runs = [r for r in orch.storage.list_runs("o") if r.trigger == "schedule"]
    explain(f"输入: 间隔 0.05s 的定时工作流运行 0.3s; "
            f"结果: 产生 {len(runs)} 次定时运行, 键={[r.idempotency_key for r in runs]}")
    assert 2 <= len(runs) <= 10, "应按间隔触发且不爆炸式增长"
    keys = [r.idempotency_key for r in runs]
    assert len(keys) == len(set(keys)), "同一时间槽不得重复触发"
    for r in runs:
        assert r.status == RunStatus.SUCCEEDED
