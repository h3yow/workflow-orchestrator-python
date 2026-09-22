"""HTTP 入口测试：注册、触发、查询、隔离、错误码可区分。"""
import time

from fastapi.testclient import TestClient

from app.api import create_app
from app.config import Settings
from app.executor import Executor, HandlerRegistry
from app.scheduler import Orchestrator
from app.storage import Storage

from .conftest import explain


def make_client(tmp_path, **overrides):
    settings = Settings(db_path=str(tmp_path / "api.db"),
                        tick_interval_seconds=0.01, **overrides)
    reg = HandlerRegistry()
    reg.register("ok", lambda p: "ok")
    orch = Orchestrator(Storage(settings.db_path),
                        Executor(settings.max_concurrent_tasks),
                        settings, reg)
    app = create_app(settings, orch)
    client = TestClient(app)
    client.__enter__()  # 触发 startup（含崩溃恢复）
    return client, orch


def test_api_happy_path_and_isolation(tmp_path):
    client, orch = make_client(tmp_path)
    try:
        h_alice = {"X-Owner-Id": "alice"}
        h_bob = {"X-Owner-Id": "bob"}
        wf = {"name": "etl", "tasks": [
            {"name": "extract", "handler": "ok"},
            {"name": "load", "handler": "ok", "depends_on": ["extract"]}]}
        r = client.post("/workflows", json=wf, headers=h_alice)
        assert r.status_code == 201, r.text
        explain(f"注册工作流: {r.json()['name']} owner=alice")

        r = client.post("/workflows/etl/runs", json={"idempotency_key": "k1"},
                        headers=h_alice)
        assert r.status_code == 201
        run_id = r.json()["id"]

        # 同键重复触发 -> 去重为同一运行
        r2 = client.post("/workflows/etl/runs", json={"idempotency_key": "k1"},
                         headers=h_alice)
        assert r2.json()["id"] == run_id
        explain(f"同键重复触发返回同一 run_id={run_id}")

        # 等待完成
        deadline = time.time() + 5
        while time.time() < deadline:
            view = client.get(f"/runs/{run_id}", headers=h_alice).json()
            if view["run"]["status"] in ("SUCCEEDED", "FAILED"):
                break
            time.sleep(0.02)
        explain(f"最终状态: {view['run']['status']}, "
                f"任务: {[(t['name'], t['status']) for t in view['tasks']]}")
        assert view["run"]["status"] == "SUCCEEDED"

        # 执行历史可查
        events = client.get(f"/runs/{run_id}/events", headers=h_alice).json()
        kinds = [e["kind"] for e in events]
        explain(f"执行历史事件: {kinds}")
        assert "RUN_CREATED" in kinds and "RUN_FINISHED" in kinds

        # 越权：bob 读 alice 的运行 -> 404（不泄露存在性）
        r = client.get(f"/runs/{run_id}", headers=h_bob)
        explain(f"bob 访问 alice 的运行 -> {r.status_code} {r.json()}")
        assert r.status_code == 404
        assert client.get("/runs", headers=h_bob).json() == []

        # 缺少身份头
        assert client.get("/runs").status_code == 401
    finally:
        orch.stop()


def test_api_validation_errors_distinguishable(tmp_path):
    client, orch = make_client(tmp_path)
    try:
        h = {"X-Owner-Id": "alice"}
        # 未知依赖
        r = client.post("/workflows", json={"name": "bad", "tasks": [
            {"name": "a", "handler": "ok", "depends_on": ["ghost"]}]},
            headers=h)
        explain(f"未知依赖 -> {r.status_code} {r.json()}")
        assert r.status_code == 422 and r.json()["error"] == "UNKNOWN_TASK"
        # 成环
        r = client.post("/workflows", json={"name": "cyc", "tasks": [
            {"name": "a", "handler": "ok", "depends_on": ["b"]},
            {"name": "b", "handler": "ok", "depends_on": ["a"]}]}, headers=h)
        explain(f"依赖成环 -> {r.status_code} {r.json()}")
        assert r.status_code == 422 and r.json()["error"] == "DEPENDENCY_CYCLE"
        # 触发不存在的工作流
        r = client.post("/workflows/nope/runs", json={}, headers=h)
        assert r.status_code == 404 and r.json()["error"] == "WORKFLOW_NOT_FOUND"
    finally:
        orch.stop()


def test_api_queue_full_429(tmp_path):
    reg_settings = dict(max_pending_runs=1)
    client, orch = make_client(tmp_path, **reg_settings)
    try:
        h = {"X-Owner-Id": "alice"}
        import threading
        block = threading.Event()
        orch.registry.register("block", lambda p: block.wait(timeout=5))
        client.post("/workflows", json={"name": "w", "tasks": [
            {"name": "t", "handler": "block"}]}, headers=h)
        client.post("/workflows/w/runs", json={"idempotency_key": "a"},
                    headers=h)
        r = client.post("/workflows/w/runs", json={"idempotency_key": "b"},
                        headers=h)
        explain(f"积压上限=1 时第二次触发 -> {r.status_code} {r.json()}")
        assert r.status_code == 429 and r.json()["error"] == "QUEUE_FULL"
        block.set()
    finally:
        orch.stop()
