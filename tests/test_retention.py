"""历史保留策略：按条数/时间清理终态运行，进行中不受影响，原子清理，
重启后策略继续生效；清理后查询语义区分"已清理/不存在/越权"；审计记录有界。"""
import threading
import time

import pytest

from app.config import Settings
from app.executor import Executor, HandlerRegistry
from app.models import (
    Rejected, RejectReason, Run, RunStatus, TaskRun, TaskSpec,
    TaskStatus, WorkflowSpec, new_id,
)
from app.scheduler import Orchestrator
from app.storage import Storage

from .conftest import explain


def _reg() -> HandlerRegistry:
    reg = HandlerRegistry()
    reg.register("ok", lambda p: "ok")
    reg.register("block", lambda p: threading.Event().wait(timeout=10))
    return reg


def _make_terminal_run(storage, wf_id, owner, status, updated_at):
    """直接构造一条已结束运行（含 1 条任务明细与事件），返回 run_id。"""
    run = Run(id=new_id(), workflow_id=wf_id, owner=owner,
              status=status, trigger="manual", idempotency_key=None,
              created_at=updated_at - 1, updated_at=updated_at)
    task = TaskRun(id=new_id(), run_id=run.id, name="t",
                  status=TaskStatus.SUCCEEDED, attempt=1, max_attempts=1,
                  next_retry_at=None, error=None,
                  started_at=updated_at - 1, finished_at=updated_at)
    storage.create_run(run, [task])
    storage.log_event(run.id, "t", "RUN_FINISHED", "seed terminal run")
    return run.id


def test_retain_last_runs_boundary_deletes_only_excess(make_orch):
    orch = make_orch(registry=_reg())
    wf = orch.register_workflow(WorkflowSpec(
        name="w", owner="alice",
        tasks=[TaskSpec(name="t", handler="ok")],
        retain_last_runs=3))
    now = time.time()
    ids = [_make_terminal_run(orch.storage, wf.id, "alice",
                                RunStatus.SUCCEEDED, now - 50 + i)
           for i in range(5)]
    deleted = orch.enforce_retention(wf)
    explain(f"输入: retain_last_runs=3, 5 条终态运行; 清理条数={deleted}")
    assert deleted == 2
    remaining = orch.storage.list_runs("alice")
    explain(f"判定依据: 剩余={sorted(r.id[:6] for r in remaining)}, "
            f"期望保留最新 3 条")
    assert {r.id for r in remaining} == set(ids[2:])
    for rid in ids[:2]:
        assert orch.storage.list_task_runs(rid) == [], "任务明细必须一起清掉"
        assert orch.storage.list_events(rid) == [], "执行事件必须一起清掉"
        assert orch.storage.get_tombstone(rid) is not None, "清理要写墓碑"
    assert orch.enforce_retention(wf) == 0, "重复清理幂等"


def test_retain_seconds_boundary_keeps_fresh_runs(make_orch, monkeypatch):
    orch = make_orch(registry=_reg())
    wf = orch.register_workflow(WorkflowSpec(
        name="w", owner="alice",
        tasks=[TaskSpec(name="t", handler="ok")],
        retain_seconds=100))
    base = 1_000_000.0  # 固定基准时间，避免真实毫秒漂移影响边界
    monkeypatch.setattr("app.scheduler.now", lambda: base)
    monkeypatch.setattr("app.storage.now", lambda: base)
    old1 = _make_terminal_run(orch.storage, wf.id, "alice",
                               RunStatus.FAILED, base - 200)
    old2 = _make_terminal_run(orch.storage, wf.id, "alice",
                               RunStatus.CANCELLED, base - 101)
    boundary = _make_terminal_run(orch.storage, wf.id, "alice",
                                  RunStatus.SUCCEEDED, base - 100)
    fresh = _make_terminal_run(orch.storage, wf.id, "alice",
                                RunStatus.SUCCEEDED, base - 10)
    deleted = orch.enforce_retention(wf)
    explain(f"输入: retain_seconds=100, now 固定; 年龄 200/101/100/10 秒; 清理={deleted}")
    assert deleted == 2
    assert orch.storage.get_run(old1) is None
    assert orch.storage.get_run(old2) is None
    assert orch.storage.get_run(boundary) is not None, "边界(恰好100s)必须保留"
    assert orch.storage.get_run(fresh) is not None


def test_active_run_never_purged(make_orch):
    reg = HandlerRegistry()
    started, release = threading.Event(), threading.Event()

    def block(_):
        started.set()
        release.wait(timeout=10)

    reg.register("block", block)
    orch = make_orch(registry=reg, retention_interval_seconds=10)
    wf = orch.register_workflow(WorkflowSpec(
        name="w", owner="alice",
        tasks=[TaskSpec(name="t", handler="block")],
        retain_last_runs=0))
    orch.start()
    active = orch.trigger("alice", "w")
    assert started.wait(timeout=5)
    old = _make_terminal_run(orch.storage, wf.id, "alice",
                               RunStatus.SUCCEEDED, time.time() - 1000)
    deleted = orch.enforce_retention(wf)
    explain(f"输入: retain_last_runs=0, 1 终态 + 1 活跃; 清理终态={deleted}, "
            f"活跃状态={orch.storage.get_run(active.id).status}")
    assert deleted == 1
    assert orch.storage.get_run(old) is None
    got = orch.storage.get_run(active.id)
    assert got.status == RunStatus.RUNNING, "进行中的运行绝不能被清理"
    assert orch.storage.list_task_runs(active.id), "活跃运行明细必须完好"
    release.set()
    orch.stop()


def test_purge_batched_and_all_or_nothing(make_orch):
    orch = make_orch(registry=_reg(), retention_batch_size=2)
    wf = orch.register_workflow(WorkflowSpec(
        name="w", owner="alice",
        tasks=[TaskSpec(name="t", handler="ok")],
        retain_last_runs=0))
    now = time.time()
    ids = [_make_terminal_run(orch.storage, wf.id, "alice",
                                RunStatus.SUCCEEDED, now - 10 + i)
           for i in range(5)]
    batches = [orch.enforce_retention(wf) for _ in range(3)]
    explain(f"输入: batch_size=2, 5 条终态; 分批清理={batches}")
    assert batches == [2, 2, 1]
    assert all(orch.storage.get_run(i) is None for i in ids)
    assert all(orch.storage.get_tombstone(i) is not None for i in ids)
    assert orch.storage.list_runs("alice") == []


def test_policy_persists_across_restart(tmp_path):
    db = str(tmp_path / "restart.db")

    def build():
        storage = Storage(db)
        return Orchestrator(storage, Executor(2),
                            Settings(db_path=db, tick_interval_seconds=0.01,
                                     retention_interval_seconds=10),
                            _reg())

    orch1 = build()
    wf = orch1.register_workflow(WorkflowSpec(
        name="w", owner="alice",
        tasks=[TaskSpec(name="t", handler="ok")],
        active_run_policy="reject", retain_last_runs=1,
        retain_seconds=3600, tombstone_limit=7))
    explain("注册: active=reject retain_last_runs=1 retain_seconds=3600 "
            "tombstone_limit=7；重启后应原样生效")
    orch1.stop()

    orch2 = build()
    wf2 = orch2.storage.get_workflow(wf.id)
    assert wf2.spec.active_run_policy == "reject"
    assert wf2.spec.retain_last_runs == 1
    assert wf2.spec.retain_seconds == 3600
    assert wf2.spec.tombstone_limit == 7
    # 两条策略同时生效：retain_last_runs=1 且 retain_seconds=3600
    # 造 3 条都超过 3600s 的终态运行，数量策略应保留最新 1 条
    now = time.time()
    ids = [_make_terminal_run(orch2.storage, wf.id, "alice",
                               RunStatus.SUCCEEDED, now - 4000 + i)
           for i in range(3)]
    deleted = orch2.enforce_retention(wf2)
    remaining = orch2.storage.list_runs("alice")
    explain(f"判定依据: 重启后清理={deleted}, 剩余={len(remaining)}")
    assert deleted == 2 and len(remaining) == 1
    assert remaining[0].id == ids[2]
    orch2.stop()


def test_update_retention_policy_persists_and_audits(make_orch):
    orch = make_orch(registry=_reg())
    wf = orch.register_workflow(WorkflowSpec(
        name="w", owner="alice",
        tasks=[TaskSpec(name="t", handler="ok")]))
    assert orch.retention_for(wf)[0] is None
    orch.update_retention_policy("alice", "w", retain_last_runs=2,
                                  retain_seconds=None)
    reloaded = orch.storage.get_workflow(wf.id)
    assert reloaded.spec.retain_last_runs == 2
    assert any(a["action"] == "POLICY_UPDATED"
               for a in orch.storage.list_audit("alice"))
    with pytest.raises(Rejected) as ei:
        orch.update_retention_policy("alice", "w", retain_last_runs=-1,
                                       retain_seconds=None)
    assert ei.value.reason == RejectReason.INVALID_POLICY


def test_invalid_retention_values_rejected(make_orch):
    orch = make_orch(registry=_reg())
    explain("输入: retain_last_runs=-1 / retain_seconds=0; 期望 INVALID_POLICY")
    with pytest.raises(Rejected) as ei:
        orch.register_workflow(WorkflowSpec(
            name="bad", owner="alice",
            tasks=[TaskSpec(name="t", handler="ok")],
            retain_last_runs=-1))
    assert ei.value.reason == RejectReason.INVALID_POLICY
    with pytest.raises(Rejected) as ei:
        orch.register_workflow(WorkflowSpec(
            name="bad2", owner="alice",
            tasks=[TaskSpec(name="t", handler="ok")],
            retain_seconds=0))
    assert ei.value.reason == RejectReason.INVALID_POLICY
