"""清理后的查询语义与审计记录：

- 自己的运行被清理 -> RUN_PURGED（410，可区分），与"从未存在"(404) 区分；
- 越权查询他人（无论存活还是已清理）一律 404，不泄露存在性；
- 复用/拒绝/清理都留下可解释、按 owner 隔离且有界的审计记录。
"""
import threading
import time

from app.executor import HandlerRegistry
from app.models import (
    Rejected, RejectReason, Run, RunStatus, TaskRun, TaskSpec,
    TaskStatus, WorkflowSpec, new_id,
)

from .conftest import explain


def _reg() -> HandlerRegistry:
    reg = HandlerRegistry()
    reg.register("ok", lambda p: "ok")
    return reg


def _terminal_run(storage, wf_id, owner, age=1000.0):
    ts = time.time() - age
    run = Run(id=new_id(), workflow_id=wf_id, owner=owner,
              status=RunStatus.SUCCEEDED, trigger="manual",
              idempotency_key=None, created_at=ts - 1, updated_at=ts)
    task = TaskRun(id=new_id(), run_id=run.id, name="t",
                  status=TaskStatus.SUCCEEDED, attempt=1, max_attempts=1,
                  next_retry_at=None, error=None,
                  started_at=ts - 1, finished_at=ts)
    storage.create_run(run, [task])
    return run.id


def test_purged_run_distinguishable_from_never_existed(make_orch):
    orch = make_orch(registry=_reg())
    wf = orch.register_workflow(WorkflowSpec(
        name="w", owner="alice",
        tasks=[TaskSpec(name="t", handler="ok")],
        retain_last_runs=0, tombstone_limit=50))
    rid = _terminal_run(orch.storage, wf.id, "alice")
    deleted = orch.enforce_retention(wf)
    explain(f"输入: 清理 {deleted} 条运行后查询；期望已清理=RUN_PURGED, "
            "不存在=RUN_NOT_FOUND")
    with __import__("pytest").raises(Rejected) as ei:
        orch.get_run_view("alice", rid)
    explain(f"判定依据(已清理): reason={ei.value.reason}, extra={ei.value.extra}")
    assert ei.value.reason == RejectReason.RUN_PURGED
    assert ei.value.extra["terminal_status"] == "SUCCEEDED"
    with __import__("pytest").raises(Rejected) as ei:
        orch.get_run_view("alice", "never-existed-id")
    explain(f"判定依据(从未存在): reason={ei.value.reason}")
    assert ei.value.reason == RejectReason.RUN_NOT_FOUND
    # 事件查询同样区分
    with __import__("pytest").raises(Rejected) as ei:
        orch.get_run_events("alice", rid)
    assert ei.value.reason == RejectReason.RUN_PURGED
    # 墓碑列表能看到被清理的运行
    stones = orch.list_tombstones("alice")
    assert [s["run_id"] for s in stones] == [rid]


def test_cross_owner_query_never_reveals_existence(make_orch):
    orch = make_orch(registry=_reg())
    wf_a = orch.register_workflow(WorkflowSpec(
        name="wa", owner="alice",
        tasks=[TaskSpec(name="t", handler="ok")], retain_last_runs=0))
    wf_b = orch.register_workflow(WorkflowSpec(
        name="wb", owner="carol",
        tasks=[TaskSpec(name="t", handler="ok")]))
    alive_id = _terminal_run(orch.storage, wf_b.id, "carol", age=10)
    purged_id = _terminal_run(orch.storage, wf_a.id, "alice", age=1000)
    orch.enforce_retention(wf_a)
    explain("输入: bob 查询 alice 已清理运行、carol 存活运行、随机不存在 id；"
            "期望三者返回完全一致的 RUN_NOT_FOUND（不泄露存在性）")
    seen = []
    for target, label in ((purged_id, "他人-已清理"),
                            (alive_id, "他人-存活"),
                            ("nope", "从未存在")):
        try:
            orch.get_run_view("bob", target)
            raise AssertionError(f"{label} 不应读取成功")
        except Rejected as e:
            explain(f"  {label} -> reason={e.reason.value} "
                    f"detail={e.detail!r} extra={e.extra}")
            seen.append((e.reason, e.detail, e.extra))
            assert e.reason == RejectReason.RUN_NOT_FOUND
    # 三种情况的 reason/detail/extra 逐项一致，无任何可区分信息
    assert all(s == seen[0] for s in seen), f"越权响应可区分: {seen}"
    assert orch.list_tombstones("bob") == []
    assert orch.list_audit("bob") == []


def test_convergence_and_purge_leave_bounded_audit(make_orch):
    reg, release, started = _blocking_registry()
    orch = make_orch(registry=reg, audit_max_entries=50,
                         audit_max_age_seconds=10 ** 9)
    wf = orch.register_workflow(WorkflowSpec(
        name="w", owner="alice",
        tasks=[TaskSpec(name="t", handler="block")],
        active_run_policy="reject"))
    orch.start()
    threads = []
    for _ in range(30):
        t = threading.Thread(target=lambda: _safe_trigger(orch))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    release.set()
    orch.stop()
    audit = orch.list_audit("alice", limit=1000)
    explain(f"输入: 30 无标识并发 reject; 审计条目={len(audit)}")
    rejects = [a for a in audit if a["action"] == "ACTIVE_CONFLICT_REJECTED"]
    assert len(rejects) == 29
    # 每条审计都带范围与原因
    for a in rejects:
        assert a["scope"] and a["detail"] and a["run_id"]
        assert a["category"] == "convergence"

    # 清理也写审计
    wf2 = orch.register_workflow(WorkflowSpec(
        name="w2", owner="alice",
        tasks=[TaskSpec(name="t", handler="ok")], retain_last_runs=0))
    _terminal_run(orch.storage, wf2.id, "alice")
    orch.enforce_retention(wf2)
    purged_audit = [a for a in orch.list_audit("alice", 1000)
                    if a["action"] == "PURGED"]
    explain(f"判定依据: 清理审计={[(a['scope'][:30], a['detail'][:30]) for a in purged_audit]}")
    assert purged_audit and purged_audit[0]["category"] == "retention"

    # 审计日志有界：超过上限后收缩
    for i in range(200):
        orch.storage.log_audit(
            "alice", wf2.id, None, "retention", "PURGED",
            f"scope{i}", f"detail{i}")
    removed = orch.storage.trim_audit(max_entries=50, max_age_seconds=10 ** 9)
    kept = orch.storage.count_audit()
    explain(f"输入: 灌入大量审计后 trim; 删除={removed}, 保留={kept}(<=50)")
    assert removed > 0 and kept <= 50


def test_background_sweeper_purges_automatically(make_orch):
    orch = make_orch(registry=_reg(), retention_interval_seconds=0.02,
                         retention_batch_size=10)
    wf = orch.register_workflow(WorkflowSpec(
        name="w", owner="alice",
        tasks=[TaskSpec(name="t", handler="ok")], retain_last_runs=0))
    rid = _terminal_run(orch.storage, wf.id, "alice")
    orch.start()
    deadline = time.time() + 5
    while time.time() < deadline:
        if orch.storage.get_run(rid) is None:
            break
        time.sleep(0.01)
    orch.stop()
    explain(f"输入: 保留策略 retain_last_runs=0 的终态运行，等待后台清理; "
            f"run 仍存在={orch.storage.get_run(rid) is not None}")
    assert orch.storage.get_run(rid) is None
    assert orch.storage.get_tombstone(rid) is not None


def _safe_trigger(orch):
    try:
        orch.trigger_detailed("alice", "w")
    except Rejected:
        pass


def _blocking_registry():
    release = threading.Event()
    started = threading.Event()

    def block(_):
        started.set()
        release.wait(timeout=10)

    reg = HandlerRegistry()
    reg.register("block", block)
    reg.register("ok", lambda p: "ok")
    return reg, release, started
