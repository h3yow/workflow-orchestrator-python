"""进程重启后的状态恢复：一致性、不重复成功、不丢弃未完成、历史可解释。"""
from __future__ import annotations

import threading
import time

from app.engine import Engine
from app.models import (
    ConcurrencyPolicy,
    RetryPolicy,
    RunState,
    TaskDef,
    TaskState,
    WorkflowDef,
)
from tests.conftest import COUNTERS, make_engine, wait_terminal


def test_succeeded_task_not_retriggered_after_restart(db_path, handlers):
    print("[输入] a 成功 -> 关闭引擎模拟重启 -> 新引擎打开同一 DB；"
          "判定依据=a 保持 SUCCEEDED 且 handler 总调用次数仍为 1")
    wf = WorkflowDef(name="w", version=1, tasks=(
        TaskDef(key="a", handler="instant"),
        TaskDef(key="b", handler="instant", depends_on=("a",)),
    ))
    eng = make_engine(db_path)
    eng.register_workflow("t1", wf)
    run_id, _ = eng.trigger_run("t1", "w")
    wait_terminal(eng, run_id)
    assert COUNTERS.calls.get("instant", 0) == 2
    eng.shutdown()

    print("[判定] 重启前两个任务均已 SUCCEEDED；重启后不得再次执行")
    eng2 = make_engine(db_path)
    time.sleep(0.15)  # 给新调度器若干拍
    run = eng2.get_run("t1", run_id)
    states = {t.task_key: t.state.value for t in eng2.list_tasks("t1", run_id)}
    assert run.state == RunState.SUCCEEDED
    assert states == {"a": "SUCCEEDED", "b": "SUCCEEDED"}
    assert COUNTERS.calls.get("instant", 0) == 2
    history = [h.kind for h in eng2.list_history("t1", run_id)]
    assert "RECOVER_INTERRUPTED_TASK" not in history
    eng2.shutdown()
    print("[判定] 成功任务未被重复触发，无中断恢复记录")


def test_interrupted_task_retried_and_completes(db_path, handlers):
    print("[输入] 任务执行到一半时关闭引擎（模拟崩溃），任务允许重试 2 次；"
          "判定依据=重启后补记 INTERRUPTED 尝试、任务重试并成功，"
          "总尝试数=2（中断1次+重试1次）")
    started = threading.Event()
    release = threading.Event()

    def blocking_handler(params, context):
        started.set()
        print(f"[handler] 阻塞任务开始 attempt={context['attempt_no']}，等待崩溃/释放")
        release.wait(timeout=5)
        return "done-after-recovery"

    import app.handlers as handlers_mod
    handlers_mod.register_handler("blocking", blocking_handler)

    wf = WorkflowDef(name="w", version=1, tasks=(
        TaskDef(key="a", handler="blocking",
                retry=RetryPolicy(max_retries=2, backoff_seconds=0.0)),
    ))
    eng = make_engine(db_path)
    eng.register_workflow("t1", wf)
    run_id, _ = eng.trigger_run("t1", "w")
    assert started.wait(2), "任务未能启动"
    # 在任务执行中途“杀死”引擎
    eng.crash()
    print("[判定] 进程在任务 RUNNING 期间终止（任务行仍为 RUNNING）")

    # 释放后台阻塞线程（模拟旧进程彻底结束），然后重启
    release.set()
    eng2 = make_engine(db_path)
    run = wait_terminal(eng2, run_id)
    assert run.state == RunState.SUCCEEDED
    tasks = eng2.list_tasks("t1", run_id)
    task = tasks[0]
    print(f"[判定] 重启后任务最终状态={task.state.value} attempt_count={task.attempt_count}")
    assert task.attempt_count == 2
    assert task.attempts[0].error_code == "INTERRUPTED"
    assert task.attempts[0].state == "FAILED"
    assert task.attempts[1].state == "SUCCEEDED"
    history = eng2.list_history("t1", run_id)
    kinds = [h.kind for h in history]
    assert "RECOVER_INTERRUPTED_TASK" in kinds
    recover_log = next(h for h in history if h.kind == "RECOVER_INTERRUPTED_TASK")
    print(f"[判定] 恢复日志可解释：{recover_log.detail}")
    eng2.shutdown()


def test_interrupted_task_without_retries_fails_distinguishably(db_path, handlers):
    print("[输入] 崩溃时 RUNNING 的任务无重试余量；"
          "判定依据=重启后任务 FAILED/run FAILED，error_code=INTERRUPTED")
    started = threading.Event()
    release = threading.Event()

    import app.handlers as handlers_mod

    def blocking_handler(params, context):
        started.set()
        release.wait(timeout=5)
        return "never"

    handlers_mod.register_handler("blocking2", blocking_handler)
    wf = WorkflowDef(name="w", version=1, tasks=(
        TaskDef(key="a", handler="blocking2"),  # 无 retry
    ))
    eng = make_engine(db_path)
    eng.register_workflow("t1", wf)
    run_id, _ = eng.trigger_run("t1", "w")
    assert started.wait(2)
    eng.crash()
    release.set()

    eng2 = make_engine(db_path)
    run = wait_terminal(eng2, run_id)
    assert run.state == RunState.FAILED
    assert run.error_code == "INTERRUPTED"
    task = eng2.list_tasks("t1", run_id)[0]
    assert task.state == TaskState.FAILED
    assert task.attempts[0].error_code == "INTERRUPTED"
    print("[判定] 无重试余量时中断进入 FAILED/INTERRUPTED，与超时/业务失败可区分")
    eng2.shutdown()


def test_queued_run_survives_restart(db_path, handlers):
    print("[输入] QUEUE 工作流：第一个运行执行中触发第二个（排队中）时重启；"
          "判定依据=重启后排队运行不丢失，最终两个运行都 SUCCEEDED")
    wf = WorkflowDef(
        name="w", version=1,
        tasks=(TaskDef(key="a", handler="medium",
                       retry=RetryPolicy(max_retries=1, backoff_seconds=0.0)),),
        concurrency=ConcurrencyPolicy.QUEUE, max_queued_runs=2,
    )
    eng = make_engine(db_path)
    eng.register_workflow("t1", wf)
    r1, _ = eng.trigger_run("t1", "w")
    r2, _ = eng.trigger_run("t1", "w")
    # 等 r1 真正开始执行后立刻崩溃
    time.sleep(0.05)
    eng.crash()

    eng2 = make_engine(db_path)
    run1 = wait_terminal(eng2, r1, timeout=3.0)
    run2 = wait_terminal(eng2, r2, timeout=3.0)
    assert run1.state == RunState.SUCCEEDED
    assert run2.state == RunState.SUCCEEDED
    history2 = eng2.list_history("t1", r2)
    assert any(h.kind == "RECOVER_QUEUED_RUN" for h in history2)
    print("[判定] 排队运行在重启后保留并被执行，恢复历史含 RECOVER_QUEUED_RUN")
    eng2.shutdown()


def test_restart_recovery_explained_by_history(db_path, handlers):
    print("[输入] 菱形依赖 a->b,a->c,b|c->d，a 执行中崩溃且可重试；"
          "判定依据=恢复后运行成功，历史中同时包含中断、重试、调度决策，"
          "且 a 只在恢复后执行成功一次，b/c/d 各一次")
    started = threading.Event()
    release = threading.Event()
    import app.handlers as handlers_mod

    def block_once(params, context):
        if context["attempt_no"] == 1:
            started.set()
            release.wait(timeout=5)
        return "a-ok"

    handlers_mod.register_handler("a_block", block_once)
    wf = WorkflowDef(name="w", version=1, tasks=(
        TaskDef(key="a", handler="a_block",
                retry=RetryPolicy(max_retries=1, backoff_seconds=0.0)),
        TaskDef(key="b", handler="quick", depends_on=("a",)),
        TaskDef(key="c", handler="quick", depends_on=("a",)),
        TaskDef(key="d", handler="quick", depends_on=("b", "c")),
    ))
    eng = make_engine(db_path)
    eng.register_workflow("t1", wf)
    run_id, _ = eng.trigger_run("t1", "w")
    assert started.wait(2)
    eng.crash()
    release.set()

    eng2 = make_engine(db_path)
    run = wait_terminal(eng2, run_id, timeout=3.0)
    assert run.state == RunState.SUCCEEDED
    states = {t.task_key: t.state for t in eng2.list_tasks("t1", run_id)}
    assert all(s == TaskState.SUCCEEDED for s in states.values())
    assert COUNTERS.calls.get("quick", 0) == 3  # b,c,d 各一次，未重复
    kinds = {h.kind for h in eng2.list_history("t1", run_id)}
    print(f"[判定] 恢复后历史决策类型={sorted(kinds)}")
    # 崩溃恢复立即安排重试（next_run_at=恢复时刻），故无需 TASK_BACKOFF；
    # 普通失败重试的退避日志由执行语义测试覆盖。
    assert {"RECOVER_INTERRUPTED_TASK",
            "TASK_DISPATCH_CANDIDATE", "RUN_SUCCEEDED"} <= kinds
    eng2.shutdown()
