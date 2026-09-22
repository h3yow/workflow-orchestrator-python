"""状态机非法转移拒绝、历史有界裁剪、定时触发与隔离查询。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.models import RunState, TaskDef, TaskState, WorkflowDef
from app.store import Store
from tests.conftest import wait_terminal


def _wf(handler="instant"):
    return WorkflowDef(name="w", version=1,
                       tasks=(TaskDef(key="a", handler=handler),))


def test_run_illegal_transition_rejected(tmp_path):
    print("[输入] 对 SUCCEEDED 的 run 尝试再置 SUCCEEDED；"
          "判定依据=条件 UPDATE 返回 False，状态机拒绝非法转移")
    db = str(tmp_path / "s.db")
    s = Store(db)
    s.init_schema()
    wf = _wf()
    rid = s.create_run("t1", wf, "manual", RunState.SUCCEEDED)
    ok = s.transition_run(rid, {RunState.RUNNING}, RunState.FAILED)
    assert ok is False
    ok2 = s.transition_run(rid, {RunState.PENDING}, RunState.RUNNING)
    assert ok2 is False
    row = s.get_run("t1", rid)
    assert row.state == RunState.SUCCEEDED
    print("[判定] 两次非法转移均被拒绝，终态保持 SUCCEEDED")
    s.close()


def test_task_double_claim_rejected(tmp_path):
    print("[输入] READY 任务被两个领取方并发领取；"
          "判定依据=只有一个领取成功（原子条件 UPDATE），杜绝重复执行")
    db = str(tmp_path / "s.db")
    s = Store(db)
    s.init_schema()
    rid = s.create_run("t1", _wf(), "manual", RunState.RUNNING)
    tid = s.list_task_rows(rid)[0]["id"]
    s.mark_tasks_ready([tid])
    first = s.claim_ready_task(tid)
    second = s.claim_ready_task(tid)
    assert first is not None and first["state"] == "RUNNING"
    assert second is None
    print("[判定] 第二次领取返回 None，同一任务不会被执行两次")
    s.close()


def test_history_pruning_is_bounded(engine):
    print("[输入] retain=50 下连续完成 55 个运行（QUEUE 策略避免触发互拒）；"
          "判定依据=终态运行仅保留最近 50 个，存储不无界增长")
    from app.models import ConcurrencyPolicy
    wf = WorkflowDef(name="wq", version=1,
                     tasks=(TaskDef(key="a", handler="instant"),),
                     concurrency=ConcurrencyPolicy.QUEUE, max_queued_runs=100)
    engine.register_workflow("t1", wf)
    ids = [engine.trigger_run("t1", "wq")[0] for _ in range(55)]
    for rid in ids:
        wait_terminal(engine, rid, timeout=3.0)
    engine.scheduler.tick()  # 触发裁剪
    kept = [r for r in engine.list_runs("t1", "wq")
            if r.state in (RunState.SUCCEEDED, RunState.FAILED)]
    print(f"[判定] 终态运行保留数量={len(kept)}（应<=50）")
    assert len(kept) <= 50
    assert engine.store.get_run("t1", ids[0]) is None
    assert engine.store.get_run("t1", ids[-1]) is not None


def test_scheduled_trigger_and_backpressure(engine):
    print("[输入] REJECT 工作流 + 每 0.01s 定时；"
          "判定依据=运行期间的定时触发被拒绝并记录 SCHED_TRIGGER_REJECTED，"
          "完成后下一次定时可再触发")
    wf = WorkflowDef(name="w", version=1,
                     tasks=(TaskDef(key="a", handler="medium"),))
    engine.register_workflow("t1", wf)
    # 暂停后台定时循环，使本测试对“到点”时刻具有确定性
    engine._sched_stop.set()
    # 间隔 3600s、首次立即到点
    engine.create_schedule(
        "t1", "cron1", "w", interval_seconds=3600,
        first_fire_at=datetime.now(timezone.utc) - timedelta(seconds=1),
    )

    # 手动制造一个活动运行，再让定时计划“到点”
    rid, _ = engine.trigger_run("t1", "w")
    fired = engine.fire_due_schedules(datetime.now(timezone.utc))
    assert fired == 0  # 被 REJECT，不计入成功触发
    logs = [d.detail for d in engine.store.list_decisions("t1")
            if d.kind == "SCHED_TRIGGER_REJECTED"]
    assert logs and "到点但触发被拒绝" in logs[-1]
    # 触发层本身也留有可区分的拒绝日志
    trig_logs = [d.kind for d in engine.store.list_decisions("t1")
                 if d.kind == "TRIGGER_REJECTED"]
    assert trig_logs
    print(f"[判定] 积压时定时触发被拒绝并记录：{logs[-1][:60]}")

    wait_terminal(engine, rid)
    # 把下一次到点时刻拨回当前，模拟下一个周期到来
    from app.store import utcnow_iso
    sched = engine.store.get_schedule("t1", "cron1")
    engine.store.advance_schedule(sched["id"], utcnow_iso(), utcnow_iso())
    fired = engine.fire_due_schedules(datetime.now(timezone.utc))
    assert fired == 1
    runs = engine.list_runs("t1", "w")
    assert any(r.trigger.startswith("schedule:cron1") for r in runs)
    print("[判定] 积压消化后定时触发成功，trigger 来源可识别")


def test_tenant_isolation_across_queries(engine):
    print("[输入] 租户 t1 建工作流并运行，租户 t2 尝试读取/触发/查历史；"
          "判定依据=t2 全部得到 NOT_FOUND，数据不可跨租户读写")
    engine.register_workflow("t1", _wf())
    rid, _ = engine.trigger_run("t1", "w")
    wait_terminal(engine, rid)

    from app.errors import WorkflowError
    for action in (
        lambda: engine.get_run("t2", rid),
        lambda: engine.list_tasks("t2", rid),
        lambda: engine.list_history("t2", rid),
        lambda: engine.trigger_run("t2", "w"),
    ):
        try:
            action()
            raise AssertionError("应当因隔离而失败")
        except WorkflowError as exc:
            assert exc.code in ("RUN_NOT_FOUND", "WORKFLOW_NOT_FOUND"), exc.code
    print("[判定] t2 无法读取/触发 t1 的任何资源，错误码为 NOT_FOUND 不泄漏存在性")
