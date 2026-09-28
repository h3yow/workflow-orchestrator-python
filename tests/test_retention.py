"""历史保留策略：边界数量/时间、进行中保护、原子性、重启后继续生效。"""
import threading
import time

from app.config import Settings
from app.executor import Executor, HandlerRegistry
from app.models import (
    ActiveRunPolicy, RetentionPolicy, RunStatus, TaskSpec, WorkflowSpec,
)
from app.scheduler import Orchestrator
from app.storage import Storage

from .conftest import explain


def _ok_registry():
    reg = HandlerRegistry()
    reg.register("ok", lambda p: "ok")
    return reg


def _finish_n_runs(orch, owner, name, n):
    ids = []
    for _ in range(n):
        r = orch.trigger(owner, name)
        ids.append(r.run.id)
    deadline = time.time() + 5
    while time.time() < deadline:
        if all(orch.storage.get_run(i).status in
               (RunStatus.SUCCEEDED, RunStatus.FAILED) for i in ids):
            break
        time.sleep(0.01)
    return ids


def test_keep_last_boundary_reaps_only_excess(make_orch):
    """keep_last=N：恰在边界的保留，超出的连同明细一起删除。"""
    orch = make_orch(registry=_ok_registry())
    wf = orch.register_workflow(WorkflowSpec(
        name="w", owner="o", tasks=[TaskSpec(name="t", handler="ok")],
        retention=RetentionPolicy(keep_last=3)))
    orch.start()
    ids = _finish_n_runs(orch, "o", "w", 5)
    explain(f"输入: 连续完成 5 条运行, keep_last=3; 运行 id 顺序(旧->新)="
            f"{[i[:6] for i in ids]}")
    reaped = orch.enforce_retention(wf)
    explain(f"判定: 清理 {reaped} 条; 期望删除最旧的 {ids[:2]}")
    assert reaped == 2
    remaining = {r.id for r in orch.storage.list_terminal_runs(wf.id)}
    assert remaining == set(ids[2:]), "必须保留最近 3 条，删除最旧 2 条"
    # 被删运行的任务明细与事件必须一起清干净
    for rid in ids[:2]:
        assert orch.storage.list_task_runs(rid) == []
        assert orch.storage.list_events(rid) == []
    # 再次执行清理应是幂等空操作
    assert orch.enforce_retention(
        orch.storage.get_workflow(wf.id)) == 0
    orch.stop()


def test_keep_last_exactly_at_boundary_reaps_nothing(make_orch):
    orch = make_orch(registry=_ok_registry())
    wf = orch.register_workflow(WorkflowSpec(
        name="w", owner="o", tasks=[TaskSpec(name="t", handler="ok")],
        retention=RetentionPolicy(keep_last=3)))
    orch.start()
    ids = _finish_n_runs(orch, "o", "w", 3)  # 恰好等于边界
    assert orch.enforce_retention(wf) == 0
    assert len(orch.storage.list_terminal_runs(wf.id)) == 3
    explain(f"输入: 完成数={len(ids)} 恰好等于 keep_last=3; 结果: 不清理")
    orch.stop()


def test_keep_seconds_boundary(make_orch):
    """keep_seconds：只清理结束时间早于截止线的运行，边界内外可区分。"""
    reg = HandlerRegistry()
    block_started = threading.Event()
    block_release = threading.Event()
    reg.register("ok", lambda p: "ok")
    reg.register("block", lambda p: (block_started.set(),
                                     block_release.wait(10))[1])
    orch = make_orch(registry=reg)
    wf = orch.register_workflow(WorkflowSpec(
        name="w", owner="o", tasks=[TaskSpec(name="t", handler="ok")],
        retention=RetentionPolicy(keep_seconds=0.3)))
    orch.start()
    old_ids = _finish_n_runs(orch, "o", "w", 2)
    explain(f"输入: 先完成 2 条运行 {[i[:6] for i in old_ids]}, 等待超龄")
    time.sleep(0.4)
    new_ids = _finish_n_runs(orch, "o", "w", 2)
    explain(f"再完成 2 条新运行 {[i[:6] for i in new_ids]}, keep_seconds=0.3")
    reaped = orch.enforce_retention(wf)
    assert reaped == 2
    remaining = {r.id for r in orch.storage.list_terminal_runs(wf.id)}
    assert remaining == set(new_ids), "超龄运行清理，未超龄的必须保留"
    explain("判定依据: 2 条超龄被清，2 条在窗口内保留")
    # 进行中的工作流不受时间策略影响（用另一条工作流持有活跃运行）
    wf2 = orch.register_workflow(WorkflowSpec(
        name="live", owner="o", tasks=[TaskSpec(name="t", handler="block")],
        retention=RetentionPolicy(keep_seconds=0.01)))
    active = orch.trigger("o", "live")
    assert block_started.wait(timeout=5)
    assert orch.enforce_retention(
        orch.storage.get_workflow(wf2.id)) == 0
    assert orch.storage.get_run(active.run.id).status == RunStatus.RUNNING
    explain("进行中运行即使按年龄'超龄'也绝不被清理")
    block_release.set()
    orch.stop()


def test_count_and_age_intersection(make_orch):
    """同时配置数量与时间：只有同时满足保留条件才留下（取交集）。"""
    orch = make_orch(registry=_ok_registry())
    wf = orch.register_workflow(WorkflowSpec(
        name="w", owner="o", tasks=[TaskSpec(name="t", handler="ok")],
        retention=RetentionPolicy(keep_last=10, keep_seconds=0.2)))
    orch.start()
    old = _finish_n_runs(orch, "o", "w", 10)  # 数量内但会超龄
    time.sleep(0.3)
    new = _finish_n_runs(orch, "o", "w", 2)
    reaped = orch.enforce_retention(wf)
    explain(f"输入: keep_last=10 且 keep_seconds=0.2; 10 条超龄 + 2 条新; "
            f"清理={reaped}")
    assert reaped == 10, "数量保留不能救回超龄运行"
    assert {r.id for r in orch.storage.list_terminal_runs(wf.id)} == set(new)
    orch.stop()


def test_active_run_never_reaped_even_when_over_limit(make_orch):
    """积压的活跃运行再多也不被保留清理触碰。"""
    reg = HandlerRegistry()
    hold = threading.Event()
    started = threading.Event()
    counter = {"n": 0}
    lock = threading.Lock()

    def block(_p):
        with lock:
            counter["n"] += 1
            if counter["n"] == 1:
                started.set()
        hold.wait(timeout=10)

    reg.register("block", block)
    orch = make_orch(registry=reg, max_pending_runs=10)
    wf = orch.register_workflow(WorkflowSpec(
        name="w", owner="o", tasks=[TaskSpec(name="t", handler="block")],
        active_run_policy=ActiveRunPolicy.ALLOW.value,
        retention=RetentionPolicy(keep_last=1)))
    orch.start()
    r = orch.trigger("o", "w")
    assert started.wait(5)
    assert orch.enforce_retention(wf) == 0
    live = orch.storage.get_run(r.run.id)
    assert live.status == RunStatus.RUNNING
    assert orch.storage.list_task_runs(r.run.id)[0].status.name == "RUNNING"
    explain(f"进行中运行 {r.run.id[:6]} 在 keep_last=1 下清理后仍完整存在")
    hold.set()
    orch.stop()


def test_reap_is_all_or_nothing_per_run_and_batch(make_orch):
    """要么整条运行连同明细清干净，要么本次清理完全不生效（无半条记录）。"""
    orch = make_orch(registry=_ok_registry())
    wf = orch.register_workflow(WorkflowSpec(
        name="w", owner="o", tasks=[TaskSpec(name="t", handler="ok")],
        retention=RetentionPolicy(keep_last=1)))
    orch.start()
    ids = _finish_n_runs(orch, "o", "w", 3)
    # 在数据库层制造"删除任务明细必失败"：触发器抛错使整个 reap 事务回滚，
    # 以此验证不会留下半条记录（run 删了但 task_runs 还在）。
    with orch.storage._lock, orch.storage._conn:
        orch.storage._conn.execute(
            "CREATE TEMP TRIGGER boom BEFORE DELETE ON task_runs "
            "BEGIN SELECT RAISE(ABORT, 'simulated crash mid-cleanup'); END")
    try:
        orch.enforce_retention(wf)
        raise AssertionError("cleanup should have failed")
    except Exception as e:
        assert "simulated crash" in str(e)
    finally:
        with orch.storage._lock, orch.storage._conn:
            orch.storage._conn.execute("DROP TRIGGER IF EXISTS boom")
    explain("模拟清理中途故障；期望全部运行/明细都仍然存在")
    for rid in ids:
        assert orch.storage.get_run(rid) is not None
        assert len(orch.storage.list_task_runs(rid)) == 1
    # 故障恢复后重新清理可正常完成
    reaped = orch.enforce_retention(orch.storage.get_workflow(wf.id))
    assert reaped == 2
    for rid in ids[:2]:
        assert orch.storage.get_run(rid) is None
        assert orch.storage.list_task_runs(rid) == []
    orch.stop()


def test_retention_persists_across_restart(tmp_path):
    """策略持久化：用旧库重启后仍按原策略清理。"""
    db = str(tmp_path / "restart.db")
    reg = _ok_registry()

    def build():
        st = Storage(db)
        return st, Orchestrator(st, Executor(2),
                                Settings(db_path=db,
                                         tick_interval_seconds=0.01), reg)

    st1, o1 = build()
    wf = o1.register_workflow(WorkflowSpec(
        name="w", owner="o", tasks=[TaskSpec(name="t", handler="ok")],
        retention=RetentionPolicy(keep_last=2)))
    o1.start()
    ids = []
    for _ in range(4):
        ids.append(o1.trigger("o", "w").run.id)
    deadline = time.time() + 5
    while time.time() < deadline:
        if all(st1.get_run(i).status == RunStatus.SUCCEEDED for i in ids):
            break
        time.sleep(0.01)
    o1.stop()
    explain(f"首次进程: 完成 4 条, keep_last=2 已随 spec 落库")

    # 重启：不重新注册，直接从磁盘加载工作流与其策略
    st2, o2 = build()
    loaded = st2.get_workflow(wf.id)
    assert loaded.spec.retention.keep_last == 2
    reaped = o2.enforce_retention(loaded)
    explain(f"重启后策略仍生效: 清理 {reaped} 条（应为 2）")
    assert reaped == 2
    assert len(st2.list_terminal_runs(wf.id)) == 2
    # 重启后更新策略并再次持久化
    o2.update_policies("o", "w", retention=RetentionPolicy(keep_last=1))
    assert o2.enforce_retention(st2.get_workflow(wf.id)) == 1
    o2.stop()

    st3, o3 = build()
    assert st3.get_workflow(wf.id).spec.retention.keep_last == 1
    explain("策略更新在再次重启后仍保持 keep_last=1")
    o3.stop()


def test_retention_scoped_per_workflow(make_orch):
    """清理只影响配置了策略的工作流，绝不波及其它工作流（含他人）。"""
    orch = make_orch(registry=_ok_registry())
    a = orch.register_workflow(WorkflowSpec(
        name="a", owner="o", tasks=[TaskSpec(name="t", handler="ok")],
        retention=RetentionPolicy(keep_last=1)))
    b = orch.register_workflow(WorkflowSpec(
        name="b", owner="o", tasks=[TaskSpec(name="t", handler="ok")]))  # 无策略
    c = orch.register_workflow(WorkflowSpec(
        name="c", owner="other", tasks=[TaskSpec(name="t", handler="ok")],
        retention=RetentionPolicy(keep_last=1)))
    orch.start()
    a_ids = _finish_n_runs(orch, "o", "a", 3)
    b_ids = _finish_n_runs(orch, "o", "b", 3)
    c_ids = _finish_n_runs(orch, "other", "c", 3)
    report = orch.enforce_all_retention()
    explain(f"输入: a(keep_last=1) 3条, b 无策略 3条, c(他人 keep_last=1) 3条; "
            f"清理报告={report}")
    assert set(report) == {a.id, c.id}
    assert len(orch.storage.list_terminal_runs(a.id)) == 1
    assert len(orch.storage.list_terminal_runs(b.id)) == 3, "无策略工作流不动"
    assert len(orch.storage.list_terminal_runs(c.id)) == 1
    orch.stop()


def test_background_cleanup_runs_periodically_without_blocking(tmp_path):
    """长期运行场景：后台线程按间隔自动清理，期间触发/查询不被长时间卡住。"""
    db = str(tmp_path / "bg.db")
    settings = Settings(db_path=db, tick_interval_seconds=0.01,
                        cleanup_interval_seconds=0.05)
    reg = _ok_registry()
    storage = Storage(db)
    orch = Orchestrator(storage, Executor(2), settings, reg)
    orch.register_workflow(WorkflowSpec(
        name="w", owner="o", tasks=[TaskSpec(name="t", handler="ok")],
        retention=RetentionPolicy(keep_last=1)))
    orch.start()
    try:
        for _ in range(3):
            orch.trigger("o", "w")
            time.sleep(0.05)
        # 等待后台线程完成至少一轮清理（间隔 0.05s）
        deadline = time.time() + 5
        while time.time() < deadline:
            wf = storage.list_all_workflows()[0]
            if len(storage.list_terminal_runs(wf.id)) <= 1:
                break
            time.sleep(0.02)
        wf = storage.list_all_workflows()[0]
        remaining = len(storage.list_terminal_runs(wf.id))
        explain(f"后台清理后保留的已结束运行数={remaining}（上界 1）")
        assert remaining <= 1
        # 清理期间/之后触发仍然即时可用
        t0 = time.time()
        r = orch.trigger("o", "w")
        assert r.outcome.value == "CREATED"
        assert time.time() - t0 < 1.0, "触发不应被清理长时间卡住"
        audit = [a for a in storage.list_audit("o")
                 if a["kind"] == "RETENTION_REAPED"]
        assert audit, "后台清理也必须写审计"
        explain(f"后台清理审计条数={len(audit)}")
    finally:
        orch.stop()
