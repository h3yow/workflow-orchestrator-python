"""清理后的查询语义：区分"被清理"与"不存在"，且越权仍不泄露。"""
import time

from app.executor import HandlerRegistry
from app.models import (
    Rejected, RejectReason, RetentionPolicy, RunStatus, TaskSpec, WorkflowSpec,
)

from .conftest import explain


def _reg():
    reg = HandlerRegistry()
    reg.register("ok", lambda p: "ok")
    return reg


def _complete(orch, owner, name):
    r = orch.trigger(owner, name)
    deadline = time.time() + 5
    while time.time() < deadline:
        if orch.storage.get_run(r.run.id).status in (
                RunStatus.SUCCEEDED, RunStatus.FAILED):
            return r
        time.sleep(0.01)
    raise AssertionError("run not finished")


def test_reaped_vs_missing_distinguishable_for_owner(make_orch):
    orch = make_orch(registry=_reg())
    wf = orch.register_workflow(WorkflowSpec(
        name="w", owner="alice", tasks=[TaskSpec(name="t", handler="ok")],
        retention=RetentionPolicy(keep_last=1)))
    orch.start()
    old = _complete(orch, "alice", "w")
    new = _complete(orch, "alice", "w")
    reaped_n = orch.enforce_retention(wf)
    assert reaped_n == 1
    explain(f"输入: 旧运行 {old.run.id[:6]} 被 keep_last=1 清理, "
            f"新运行 {new.run.id[:6]} 保留, 另查一个随机不存在 id")
    # 被清理：owner 自己能区分
    with __import__("pytest").raises(Rejected) as ei:
        orch.get_run_view("alice", old.run.id)
    explain(f"被清理运行查询 -> {ei.value.reason.value}")
    assert ei.value.reason == RejectReason.RUN_REAPED
    # 不存在：另一种原因
    import uuid
    with __import__("pytest").raises(Rejected) as ei2:
        orch.get_run_view("alice", uuid.uuid4().hex)
    explain(f"不存在运行查询 -> {ei2.value.reason.value}")
    assert ei2.value.reason == RejectReason.RUN_NOT_FOUND
    # 保留的正常可查
    view = orch.get_run_view("alice", new.run.id)
    assert view["run"].status == RunStatus.SUCCEEDED
    orch.stop()


def test_cross_owner_cannot_distinguish_anything(make_orch):
    """越权者眼里：存在的、被清理的、不存在的运行全都不可区分。"""
    orch = make_orch(registry=_reg())
    wf = orch.register_workflow(WorkflowSpec(
        name="w", owner="alice", tasks=[TaskSpec(name="t", handler="ok")],
        retention=RetentionPolicy(keep_last=1)))
    orch.start()
    old = _complete(orch, "alice", "w")
    new = _complete(orch, "alice", "w")
    orch.enforce_retention(wf)
    import uuid
    missing = uuid.uuid4().hex

    def as_bob(rid):
        try:
            orch.get_run_view("bob", rid)
            return "OK"
        except Rejected as e:
            return e.reason

    r_reaped = as_bob(old.run.id)
    r_existing = as_bob(new.run.id)
    r_missing = as_bob(missing)
    explain(f"bob 视角: 被清理={r_reaped}, 存在={r_existing}, 不存在={r_missing}")
    # 核心层对存在但越权的资源给 FORBIDDEN（用于服务端诊断），
    # 被清理/不存在一律 RUN_NOT_FOUND；关键是 bob 读不到任何数据。
    assert orch.storage.list_runs("bob") == []
    assert r_existing == RejectReason.FORBIDDEN
    assert r_reaped == RejectReason.RUN_NOT_FOUND
    assert r_missing == RejectReason.RUN_NOT_FOUND
    # bob 也读不到 alice 的审计
    assert orch.list_audit("bob") == []
    explain("bob 既读不到运行数据，也读不到审计，且无法区分三种 id")
    orch.stop()


def test_audit_records_explain_reuse_reject_and_reap(make_orch):
    """每次复用/拒绝/清理都留下带范围的、可解释的审计。"""
    orch = make_orch(registry=_reg())
    from app.models import ActiveRunPolicy
    import threading
    hold = threading.Event()
    started = threading.Event()
    orch.registry.register("block", lambda p: (started.set(), hold.wait(10))[1])
    wf = orch.register_workflow(WorkflowSpec(
        name="w", owner="o", tasks=[TaskSpec(name="t", handler="block")],
        active_run_policy=ActiveRunPolicy.REUSE.value,
        retention=RetentionPolicy(keep_last=1)))
    orch.start()
    first = orch.trigger("o", "w")
    assert started.wait(5)
    second = orch.trigger("o", "w")
    third = orch.trigger("o", "w", trigger="schedule",
                         idempotency_key="sched-collide")
    assert second.outcome.value == "REUSED_ACTIVE"
    assert third.outcome.value == "REUSED_ACTIVE"
    hold.set()
    # 再补运行制造清理
    deadline = time.time() + 5
    while time.time() < deadline and orch.storage.get_run(
            first.run.id).status != RunStatus.SUCCEEDED:
        time.sleep(0.01)
    extra = _complete(orch, "o", "w")
    orch.enforce_retention(wf)
    audit = orch.list_audit("o", "w")
    kinds = [a["kind"] for a in audit]
    explain(f"输入: 2 次复用 + 1 次清理; 审计种类序列={kinds}")
    assert kinds.count("TRIGGER_REUSED") == 2
    assert kinds.count("RETENTION_REAPED") == 1
    reused = [a for a in audit if a["kind"] == "TRIGGER_REUSED"]
    assert reused[0]["scope"]["active_run_id"] == first.run.id
    assert {a["scope"]["incoming_trigger"] for a in reused} == {
        "manual", "schedule"}
    reap = [a for a in audit if a["kind"] == "RETENTION_REAPED"][0]
    explain(f"清理审计范围={reap['scope']}")
    assert reap["scope"]["reaped_count"] == 1
    assert reap["scope"]["keep_last"] == 1
    assert first.run.id in reap["scope"]["reaped_run_ids"]
    orch.stop()


def test_audit_and_tombstones_are_bounded(make_orch):
    """审计记录与墓碑自身不能无限膨胀：受行数与 TTL 双重约束。"""
    orch = make_orch(registry=_reg(), audit_max_rows=10,
                     audit_ttl_seconds=10 ** 6,
                     tombstone_ttl_seconds=10 ** 6)
    for i in range(25):
        orch.storage.add_audit("TRIGGER_REUSED", f"m{i}", {"i": i}, owner="o")
    removed = orch.storage.prune_audit(ttl_seconds=10 ** 6, max_rows=10)
    rows = orch.storage.list_audit("o", limit=1000)
    explain(f"输入: 25 条审计, 行数上界 10; 裁剪={removed}, 剩余={len(rows)}")
    assert len(rows) == 10
    # 墓碑：TTL 裁剪后，被清理运行回到"本来就不存在"
    wf = orch.register_workflow(WorkflowSpec(
        name="w", owner="o", tasks=[TaskSpec(name="t", handler="ok")],
        retention=RetentionPolicy(keep_last=1)))
    orch.start()
    old = _complete(orch, "o", "w")
    _complete(orch, "o", "w")
    orch.enforce_retention(wf)
    with __import__("pytest").raises(Rejected) as ei:
        orch.get_run_view("o", old.run.id)
    assert ei.value.reason == RejectReason.RUN_REAPED
    orch.storage.prune_tombstones(ttl_seconds=0.0)
    with __import__("pytest").raises(Rejected) as ei2:
        orch.get_run_view("o", old.run.id)
    explain(f"墓碑 TTL 裁剪后查询 -> {ei2.value.reason.value}（等同从未存在）")
    assert ei2.value.reason == RejectReason.RUN_NOT_FOUND
    orch.stop()
