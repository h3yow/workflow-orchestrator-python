"""多个调用方同时触发同一工作流：去重/拒绝策略与队列上限。"""
from __future__ import annotations

import threading

import pytest

from app.errors import WorkflowError
from app.models import ConcurrencyPolicy, RunState, TaskDef, WorkflowDef
from tests.conftest import wait_terminal


def _wf(policy: ConcurrencyPolicy, max_queued: int = 0, handler="medium"):
    return WorkflowDef(
        name="w", version=1,
        tasks=(TaskDef(key="a", handler=handler),),
        concurrency=policy, max_queued_runs=max_queued,
    )


def test_concurrent_trigger_reject_runs_only_once(engine):
    print("[输入] 20 个线程同时触发 REJECT 工作流（任务耗时 0.15s）；"
          "判定依据=恰好 1 个运行被创建，其余全部 ALREADY_ACTIVE，任务只执行 1 次")
    engine.register_workflow("t1", _wf(ConcurrencyPolicy.REJECT))
    results: list[tuple[str, object]] = []
    barrier = threading.Barrier(20)

    def fire():
        barrier.wait()
        try:
            run_id, deduped = engine.trigger_run("t1", "w")
            results.append(("ok", run_id))
        except WorkflowError as exc:
            results.append(("err", exc.code))

    threads = [threading.Thread(target=fire) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    ok = [r for r in results if r[0] == "ok"]
    errs = [r for r in results if r[0] == "err"]
    print(f"[判定] 成功 {len(ok)} 个，拒绝 {len(errs)} 个，拒绝码集合="
          f"{ {c for _, c in errs} }")
    assert len(ok) == 1
    assert all(code == "ALREADY_ACTIVE" for _, code in errs)
    run = wait_terminal(engine, ok[0][1])
    assert run.state == RunState.SUCCEEDED
    # 同一 run 中任务只执行一次
    tasks = engine.list_tasks("t1", ok[0][1])
    assert tasks[0].attempt_count == 1


def test_concurrent_trigger_skip_dedups_to_single_run(engine):
    print("[输入] 20 个线程同时触发 SKIP 工作流；"
          "判定依据=只有 1 个 run_id，所有调用要么创建要么去重，绝不重复执行")
    engine.register_workflow("t1", _wf(ConcurrencyPolicy.SKIP))
    run_ids: list[str] = []
    lock = threading.Lock()
    barrier = threading.Barrier(20)

    def fire():
        barrier.wait()
        rid, _ = engine.trigger_run("t1", "w")
        with lock:
            run_ids.append(rid)

    threads = [threading.Thread(target=fire) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    unique = set(run_ids)
    print(f"[判定] 20 次触发落到 {len(unique)} 个运行上")
    assert len(unique) == 1
    run = wait_terminal(engine, unique.pop())
    assert run.state == RunState.SUCCEEDED
    assert engine.list_tasks("t1", run.id)[0].attempt_count == 1


def test_queue_policy_bounded_and_fifo(engine):
    print("[输入] QUEUE 策略 max_queued_runs=1，连续触发 3 次；"
          "判定依据=前 2 次接受（1 运行 + 1 排队），第 3 次 QUEUE_FULL；"
          "两个运行最终依次 SUCCEEDED")
    engine.register_workflow("t1", _wf(ConcurrencyPolicy.QUEUE, max_queued=1))
    r1, d1 = engine.trigger_run("t1", "w")
    r2, d2 = engine.trigger_run("t1", "w")
    assert d1 is False and d2 is False
    with pytest.raises(WorkflowError) as exc:
        engine.trigger_run("t1", "w")
    assert exc.value.code == "QUEUE_FULL"
    print(f"[判定] 接受 run1={r1[:8]} run2={r2[:8]}，第3次拒绝 QUEUE_FULL")

    run1 = wait_terminal(engine, r1, timeout=3.0)
    run2 = wait_terminal(engine, r2, timeout=3.0)
    assert run1.state == RunState.SUCCEEDED
    assert run2.state == RunState.SUCCEEDED
    # 串行执行：r1 结束时间不晚于 r2 开始时间（FIFO 放行）
    assert run1.finished_at <= run2.started_at
    print(f"[判定] FIFO 顺序成立：r1 完成 {run1.finished_at} <= r2 开始 {run2.started_at}")


def test_no_concurrent_running_for_same_workflow(engine):
    print("[输入] QUEUE 策略排队两个运行，任务都耗时；"
          "判定依据=任一时刻同工作流 RUNNING 数 <= 1")
    engine.register_workflow("t1", _wf(ConcurrencyPolicy.QUEUE, max_queued=2))
    ids = [engine.trigger_run("t1", "w")[0] for _ in range(3)]
    import time
    running_seen = 0
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        n = engine.store.count_running_runs("t1", "w")
        running_seen = max(running_seen, n)
        assert n <= 1, "同一工作流出现并发 RUNNING"
        if all(engine.get_run("t1", i).state == RunState.SUCCEEDED for i in ids):
            break
        time.sleep(0.005)
    assert all(engine.get_run("t1", i).state == RunState.SUCCEEDED for i in ids)
    print(f"[判定] 观测到的最大并发 RUNNING 数={running_seen}")


def test_queue_releases_capacity_after_completion(engine):
    print("[输入] 队列占满被拒后，等待运行完成可再次触发；"
          "判定依据=拒绝发生，积压消化后新触发被接受并成功")
    engine.register_workflow("t1", _wf(ConcurrencyPolicy.QUEUE, max_queued=0))
    r1, _ = engine.trigger_run("t1", "w")
    with pytest.raises(WorkflowError) as exc:
        engine.trigger_run("t1", "w")
    assert exc.value.code == "QUEUE_FULL"
    wait_terminal(engine, r1)
    r2, _ = engine.trigger_run("t1", "w")
    run2 = wait_terminal(engine, r2)
    assert run2.state == RunState.SUCCEEDED
    print("[判定] 容量随终态释放，未永久拒绝")
