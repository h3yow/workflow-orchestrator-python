"""并发收敛：无业务去重标识时同一工作流同一时刻仅允许一条活跃运行。

覆盖：
- 无标识并发触发：reuse 恰好一条活跃，其余复用结果可区分（手动/手动撞车）；
- reject 策略：恰好一条成功，其余明确拒绝并携带冲突的活跃 run_id；
- 手动触发与定时触发撞车走同一套规则，收敛原因可在审计记录中查到；
- allow（默认）不收敛，保持历史语义；
- 活跃运行结束后新的触发可以创建新运行。
"""
import threading
import time

import pytest

from app.executor import HandlerRegistry
from app.models import (
    Rejected, RejectReason, RunStatus, TaskSpec, TRIGGER_CREATED,
    TRIGGER_REUSED, WorkflowSpec,
)

from .conftest import explain


def _reg() -> HandlerRegistry:
    reg = HandlerRegistry()
    reg.register("ok", lambda p: "ok")
    return reg


def _blocking_registry():
    """返回 (registry, release_event, started_event)，任务会阻塞直到 release。"""
    release = threading.Event()
    started = threading.Event()

    def block(_):
        started.set()
        release.wait(timeout=10)

    reg = HandlerRegistry()
    reg.register("block", block)
    return reg, release, started


def _spec(name="w", owner="o", handler="block", **policy):
    return WorkflowSpec(name=name, owner=owner,
                      tasks=[TaskSpec(name="t", handler=handler)], **policy)


def test_concurrent_triggers_without_key_reuse_converge(make_orch):
    reg, release, started = _blocking_registry()
    orch = make_orch(registry=reg)
    orch.register_workflow(_spec(active_run_policy="reuse"))
    orch.start()
    outcomes, errors = [], []
    lock = threading.Lock()

    def fire():
        try:
            r = orch.trigger_detailed("o", "w")  # 全部无幂等键
            with lock:
                outcomes.append((r.outcome, r.run.id))
        except Rejected as e:
            with lock:
                errors.append(e)

    threads = [threading.Thread(target=fire) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    created = [oid for o, oid in outcomes if o == TRIGGER_CREATED]
    reused = [oid for o, oid in outcomes if o == TRIGGER_REUSED]
    explain(f"输入: 20 线程无去重标识并发触发 reuse 策略工作流; "
            f"新建={len(created)} 复用={len(reused)} 拒绝={len(errors)}")
    assert not errors
    assert len(created) == 1, "同一时刻只能有一条活跃运行被创建"
    assert len(reused) == 19, "其余触发必须被明确告知复用"
    assert set(created) | set(reused) == {created[0]}, "全部收敛到同一条运行"
    active = [r for r in orch.storage.list_runs("o")
              if r.status in (RunStatus.PENDING, RunStatus.RUNNING)]
    assert len(active) == 1
    sample = next(o for o in outcomes if o[0] == TRIGGER_REUSED)
    assert sample[1] == created[0]
    explain(f"判定依据: 活跃运行={active[0].id}, 复用原因可区分 outcome=reused")
    release.set()
    orch.stop()


def test_concurrent_triggers_without_key_reject_policy(make_orch):
    reg, release, started = _blocking_registry()
    orch = make_orch(registry=reg)
    orch.register_workflow(_spec(active_run_policy="reject"))
    orch.start()
    accepted, rejected = [], []
    lock = threading.Lock()

    def fire():
        try:
            r = orch.trigger_detailed("o", "w")
            with lock:
                accepted.append(r.run.id)
        except Rejected as e:
            with lock:
                rejected.append(e)

    threads = [threading.Thread(target=fire) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    explain(f"输入: 20 线程无标识并发触发 reject 策略; "
            f"接受={len(accepted)} 拒绝={len(rejected)}")
    assert len(set(accepted)) == 1 and len(accepted) == 1
    assert len(rejected) == 19
    active_id = accepted[0]
    for e in rejected:
        assert e.reason == RejectReason.ACTIVE_RUN_CONFLICT
        assert e.extra.get("active_run_id") == active_id, "拒绝必须指明与谁冲突"
    active = [r for r in orch.storage.list_runs("o")
              if r.status in (RunStatus.PENDING, RunStatus.RUNNING)]
    assert len(active) == 1
    explain(f"判定依据: 全部拒绝码=ACTIVE_RUN_CONFLICT, active_run_id={active_id}")
    release.set()
    orch.stop()


def test_manual_and_scheduled_collision_share_one_rule(make_orch):
    """手动触发占着活跃运行时，定时触发必须复用（reuse），且审计可查原因。"""
    reg, release, started = _blocking_registry()
    orch = make_orch(registry=reg)
    orch.register_workflow(_spec(
        name="cron", active_run_policy="reuse",
        schedule_interval_seconds=0.05))
    orch.start()
    manual = orch.trigger_detailed("o", "cron", trigger="manual")
    assert started.wait(timeout=5), "手动运行应已进入执行"
    # 等待若干调度周期
    time.sleep(0.3)
    runs = orch.storage.list_runs("o")
    active = [r for r in runs
              if r.status in (RunStatus.PENDING, RunStatus.RUNNING)]
    explain(f"输入: 手动运行活跃期间定时触发撞车; "
            f"运行数={len(runs)}, 活跃数={len(active)}, "
            f"触发器={sorted({r.trigger for r in runs})}")
    assert len(active) == 1, "手动/定时撞车也不得出现第二条活跃运行"
    assert active[0].id == manual.run.id
    # 定时触发的复用结果与原因可查
    audits = orch.storage.list_audit("o")
    reused = [a for a in audits if a["action"] == "REUSED_ACTIVE"]
    explain(f"判定依据: 收敛审计条目数={len(reused)}, "
            f"scope={[a['scope'] for a in reused[:2]]}")
    assert reused, "复用必须留下可解释记录"
    assert all(active[0].id in (a["run_id"] or "") for a in reused)
    release.set()
    orch.stop()


def test_allow_policy_keeps_legacy_parallel_semantics(make_orch):
    reg, release, _ = _blocking_registry()
    orch = make_orch(registry=reg, max_pending_runs=10)
    orch.register_workflow(_spec(active_run_policy="allow"))  # 默认/显式允许
    orch.start()
    ids = {orch.trigger_detailed("o", "w").run.id for _ in range(3)}
    active = [r for r in orch.storage.list_runs("o")
              if r.status in (RunStatus.PENDING, RunStatus.RUNNING)]
    explain(f"输入: allow 策略下连续 3 次无标识触发; 活跃数={len(active)}")
    assert len(ids) == 3 and len(active) == 3, "allow 不收敛（历史语义）"
    release.set()
    orch.stop()


def test_new_trigger_after_active_finishes(make_orch):
    orch = make_orch(registry=_reg())
    orch.register_workflow(_spec(active_run_policy="reuse", handler="ok"))
    orch.start()
    first = orch.trigger_detailed("o", "w")
    deadline = time.time() + 5
    while time.time() < deadline:
        if orch.storage.get_run(first.run.id).status == RunStatus.SUCCEEDED:
            break
        time.sleep(0.01)
    second = orch.trigger_detailed("o", "w")
    explain(f"输入: reuse 策略，第一条结束后再次触发; "
            f"第一次 outcome={first.outcome}, 第二次 outcome={second.outcome}")
    assert second.outcome == TRIGGER_CREATED
    assert second.run.id != first.run.id
    orch.stop()


def test_invalid_active_run_policy_rejected(make_orch):
    orch = make_orch(registry=_reg())
    explain("输入: active_run_policy='bogus'; 期望: INVALID_POLICY")
    with pytest.raises(Rejected) as ei:
        orch.register_workflow(_spec(active_run_policy="bogus"))
    explain(f"判定依据: reason={ei.value.reason}")
    assert ei.value.reason == RejectReason.INVALID_POLICY
