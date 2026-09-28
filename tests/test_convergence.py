"""并发收敛：无业务去重标识时，同一工作流同一时刻最多一条活跃运行。

覆盖：
- 多上游无键并发触发：reuse 策略只产生一条活跃运行，其余可区分是"复用"；
- reject 策略：只一条创建成功，其余得到可区分的 ACTIVE_RUN_EXISTS；
- 手动触发与定时触发撞车，走同一套规则且原因可审计；
- 活跃运行结束后，新触发可以正常进入（收敛只针对"同一时刻"）。
"""
import threading
import time

from app.executor import HandlerRegistry
from app.models import (
    ActiveRunPolicy, Rejected, RejectReason, RunStatus, TaskSpec,
    TriggerOutcome, WorkflowSpec,
)

from .conftest import explain


def _blocking_registry(handler="block"):
    reg = HandlerRegistry()
    started = threading.Event()
    release = threading.Event()

    def block(_payload):
        started.set()
        release.wait(timeout=10)
        return "done"

    reg.register(handler, block)
    return reg, started, release


def _wait_terminal(storage, run_id, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        run = storage.get_run(run_id)
        if run.status in (RunStatus.SUCCEEDED, RunStatus.FAILED,
                          RunStatus.CANCELLED):
            return run
        time.sleep(0.01)
    raise AssertionError("run did not finish")


def test_keyless_concurrent_triggers_reuse_single_active(make_orch):
    reg, started, release = _blocking_registry()
    orch = make_orch(registry=reg)
    orch.register_workflow(WorkflowSpec(
        name="w", owner="upstream", tasks=[TaskSpec(name="t", handler="block")],
        active_run_policy=ActiveRunPolicy.REUSE.value))
    orch.start()
    outcomes, run_ids, errors = [], [], []
    lock = threading.Lock()

    def fire():
        # 多个上游都给不出业务去重标识
        try:
            result = orch.trigger("upstream", "w")
            with lock:
                outcomes.append(result.outcome)
                run_ids.append(result.run.id)
        except Rejected as e:
            with lock:
                errors.append(e)

    threads = [threading.Thread(target=fire) for _ in range(16)]
    for t in threads:
        t.start()
    assert started.wait(timeout=5), "首个任务应已开始执行"
    for t in threads:
        t.join()
    explain(f"输入: 16 个无键并发触发 reuse 策略; "
            f"判定分布: {sorted(o.value for o in outcomes)}, 错误数={len(errors)}")
    assert errors == []
    assert len(set(run_ids)) == 1, "并发窗口内绝不允许两条活跃运行"
    created = [o for o in outcomes if o == TriggerOutcome.CREATED]
    reused = [o for o in outcomes if o == TriggerOutcome.REUSED_ACTIVE]
    assert len(created) == 1 and len(reused) == 15
    wf_active = [r for r in orch.storage.list_active_runs()
                 if r.owner == "upstream"]
    explain(f"判定依据: 存储中该工作流活跃运行数={len(wf_active)}（必须为 1）")
    assert len(wf_active) == 1
    # 复用方应能查到自己复用了哪条运行及原因
    audit = orch.list_audit("upstream", "w")
    reuse_audit = [a for a in audit if a["kind"] == "TRIGGER_REUSED"]
    explain(f"审计: {len(reuse_audit)} 条复用记录, 示例范围="
            f"{reuse_audit[0]['scope'] if reuse_audit else None}")
    assert len(reuse_audit) == 15
    assert all(a["scope"]["active_run_id"] == run_ids[0]
               for a in reuse_audit)
    release.set()
    orch.stop()


def test_keyless_concurrent_triggers_reject_distinguishable(make_orch):
    reg, started, release = _blocking_registry()
    orch = make_orch(registry=reg)
    orch.register_workflow(WorkflowSpec(
        name="w", owner="o", tasks=[TaskSpec(name="t", handler="block")],
        active_run_policy=ActiveRunPolicy.REJECT.value))
    orch.start()
    created, rejected = [], []
    lock = threading.Lock()

    def fire():
        try:
            r = orch.trigger("o", "w")
            with lock:
                created.append(r.run.id)
        except Rejected as e:
            with lock:
                rejected.append(e)

    threads = [threading.Thread(target=fire) for _ in range(12)]
    for t in threads:
        t.start()
    assert started.wait(timeout=5)
    for t in threads:
        t.join()
    explain(f"输入: 12 个无键并发触发 reject 策略; "
            f"成功={len(created)}, 拒绝={len(rejected)}")
    assert len(created) == 1
    assert len(rejected) == 11
    assert all(e.reason == RejectReason.ACTIVE_RUN_EXISTS for e in rejected)
    # 拒绝要明确告知是哪条活跃运行占了位
    assert all(created[0] in e.detail for e in rejected)
    assert len(orch.storage.list_runs("o")) == 1, "拒绝方不得落库新运行"
    rejected_audit = [a for a in orch.list_audit("o", "w")
                      if a["kind"] == "TRIGGER_REJECTED"]
    assert len(rejected_audit) == 11
    release.set()
    orch.stop()


def test_manual_and_schedule_collision_share_rules(make_orch):
    """手动触发与定时触发撞在一起：同一套活跃收敛规则，结果可查原因。

    关闭后台自动定时（schedule_enabled=False），由测试显式扮演两种触发源，
    避免"首个 tick 自动触发"造成的时序竞态。
    """
    reg, started, release = _blocking_registry()
    orch = make_orch(registry=reg, schedule_enabled=False)
    orch.register_workflow(WorkflowSpec(
        name="cron", owner="o", tasks=[TaskSpec(name="t", handler="block")],
        schedule_interval_seconds=0.05,
        active_run_policy=ActiveRunPolicy.REUSE.value))
    orch.start()
    manual = orch.trigger("o", "cron")  # 手动先占活跃
    assert started.wait(timeout=5)
    # 定时触发立即到来，撞上同一条活跃运行
    sched = orch.trigger("o", "cron", trigger="schedule",
                         idempotency_key="sched-manual-collision")
    explain(f"输入: 手动活跃运行 {manual.run.id} 期间定时触发; "
            f"定时判定={sched.outcome.value}, 复用={sched.run.id}")
    assert sched.outcome == TriggerOutcome.REUSED_ACTIVE
    assert sched.run.id == manual.run.id
    assert manual.outcome == TriggerOutcome.CREATED
    assert len(orch.storage.list_runs("o")) == 1, "撞车不得产生第二条运行"
    collision = [a for a in orch.list_audit("o", "cron")
                 if a["kind"] == "TRIGGER_REUSED"][0]
    explain(f"判定依据: 审计范围={collision['scope']}")
    assert collision["scope"]["incoming_trigger"] == "schedule"
    assert collision["scope"]["active_trigger"] == "manual"
    release.set()
    _wait_terminal(orch.storage, manual.run.id)

    # 反向撞车：定时先占活跃，手动触发也必须复用同一条
    reg2, started2, release2 = _blocking_registry("block2")
    orch2 = make_orch(registry=reg2, schedule_enabled=False)
    wf2 = orch2.register_workflow(WorkflowSpec(
        name="cron2", owner="o", tasks=[TaskSpec(name="t", handler="block2")],
        schedule_interval_seconds=0.05,
        active_run_policy=ActiveRunPolicy.REUSE.value))
    wf2_id = wf2.id
    orch2.start()
    sched_run = orch2.trigger("o", "cron2", trigger="schedule",
                              idempotency_key="sched-first")
    assert started2.wait(timeout=5)
    manual2 = orch2.trigger("o", "cron2")
    explain(f"反向撞车: 定时活跃 {sched_run.run.id}, 手动判定="
            f"{manual2.outcome.value}")
    assert sched_run.outcome == TriggerOutcome.CREATED
    assert manual2.outcome == TriggerOutcome.REUSED_ACTIVE
    assert manual2.run.id == sched_run.run.id
    assert len(orch2.storage.list_runs("o", workflow_id=wf2_id)) == 1
    back_audit = [a for a in orch2.list_audit("o", "cron2")
                  if a["kind"] == "TRIGGER_REUSED"][0]
    assert back_audit["scope"]["incoming_trigger"] == "manual"
    assert back_audit["scope"]["active_trigger"] == "schedule"
    release2.set()
    orch2.stop()
    orch.stop()


def test_new_trigger_allowed_after_active_finishes(make_orch):
    """收敛只约束同一时刻：活跃运行结束后必须能再触发新运行。"""
    reg = HandlerRegistry()
    reg.register("ok", lambda p: "ok")
    orch = make_orch(registry=reg)
    orch.register_workflow(WorkflowSpec(
        name="w", owner="o", tasks=[TaskSpec(name="t", handler="ok")],
        active_run_policy=ActiveRunPolicy.REJECT.value))
    orch.start()
    r1 = orch.trigger("o", "w")
    _wait_terminal(orch.storage, r1.run.id)
    r2 = orch.trigger("o", "w")
    explain(f"输入: reject 策略下首条结束后再次触发; "
            f"第一次={r1.outcome.value}, 第二次={r2.outcome.value}")
    assert r1.outcome == TriggerOutcome.CREATED
    assert r2.outcome == TriggerOutcome.CREATED
    assert r1.run.id != r2.run.id
    orch.stop()
