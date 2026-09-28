"""HTTP 层新行为：触发收敛状态码/响应体、410 清理语义、保留策略更新、审计。"""
import threading
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
    release = threading.Event()
    reg.register("block", lambda p: release.wait(timeout=10))
    orch = Orchestrator(Storage(settings.db_path),
                        Executor(settings.max_concurrent_tasks),
                        settings, reg)
    app = create_app(settings, orch)
    client = TestClient(app)
    client.__enter__()
    return client, orch, release


def test_trigger_convergence_status_and_body(tmp_path):
    client, orch, release = make_client(tmp_path)
    h = {"X-Owner-Id": "alice"}
    try:
        client.post("/workflows", json={"name": "w", "tasks": [
            {"name": "t", "handler": "block"}],
            "active_run_policy": "reuse"}, headers=h)
        r1 = client.post("/workflows/w/runs", json={}, headers=h)
        r2 = client.post("/workflows/w/runs", json={}, headers=h)
        explain(f"首次触发={r1.status_code} outcome="
                f"{r1.json().get('trigger_outcome')}; 无标识再触发="
                f"{r2.status_code} outcome={r2.json().get('trigger_outcome')}")
        assert r1.status_code == 201
        assert r1.json()["trigger_outcome"] == "created"
        assert r2.status_code == 200
        body2 = r2.json()
        assert body2["trigger_outcome"] == "reused"
        assert body2["id"] == r1.json()["id"]
        assert body2["convergence_reason"]
        release.set()
    finally:
        orch.stop()


def test_trigger_reject_conflict_409(tmp_path):
    client, orch, release = make_client(tmp_path)
    h = {"X-Owner-Id": "alice"}
    try:
        client.post("/workflows", json={"name": "w", "tasks": [
            {"name": "t", "handler": "block"}],
            "active_run_policy": "reject"}, headers=h)
        r1 = client.post("/workflows/w/runs", json={}, headers=h)
        r2 = client.post("/workflows/w/runs", json={}, headers=h)
        explain(f"reject 策略: 首触发={r1.status_code}, 再触发={r2.status_code} "
                f"{r2.json()}")
        assert r1.status_code == 201
        assert r2.status_code == 409
        assert r2.json()["error"] == "ACTIVE_RUN_CONFLICT"
        assert r2.json()["active_run_id"] == r1.json()["id"]
        release.set()
    finally:
        orch.stop()


def test_purged_run_returns_410_and_cross_owner_404(tmp_path):
    client, orch, release = make_client(tmp_path)
    h_a = {"X-Owner-Id": "alice"}
    h_b = {"X-Owner-Id": "bob"}
    try:
        client.post("/workflows", json={"name": "w", "tasks": [
            {"name": "t", "handler": "ok"}], "retain_last_runs": 0},
            headers=h_a)
        r = client.post("/workflows/w/runs", json={}, headers=h_a)
        run_id = r.json()["id"]
        deadline = time.time() + 5
        while time.time() < deadline:
            if client.get(f"/runs/{run_id}", headers=h_a).json()["run"][
                    "status"] == "SUCCEEDED":
                break
            time.sleep(0.02)
        orch.enforce_retention_once()
        gone = client.get(f"/runs/{run_id}", headers=h_a)
        explain(f"owner 查询已清理运行 -> {gone.status_code} {gone.json()}")
        assert gone.status_code == 410
        assert gone.json()["error"] == "RUN_PURGED"
        # 他人查询同一条 -> 404，不得得知已清理
        cross = client.get(f"/runs/{run_id}", headers=h_b)
        explain(f"他人查询同一条 -> {cross.status_code} {cross.json()}")
        assert cross.status_code == 404
        assert cross.json()["error"] != "RUN_PURGED"
        # 不存在
        missing = client.get("/runs/nope", headers=h_a)
        assert missing.status_code == 404
        # 墓碑列表
        stones = client.get("/runs/purged", headers=h_a).json()
        assert [s["run_id"] for s in stones] == [run_id]
    finally:
        orch.stop()


def test_update_retention_policy_endpoint(tmp_path):
    client, orch, release = make_client(tmp_path)
    h = {"X-Owner-Id": "alice"}
    try:
        client.post("/workflows", json={"name": "w", "tasks": [
            {"name": "t", "handler": "ok"}]}, headers=h)
        r = client.put("/workflows/w/retention",
                       json={"retain_last_runs": 5, "retain_seconds": 3600},
                       headers=h)
        assert r.status_code == 200, r.text
        spec = r.json()["spec"]
        explain(f"更新保留策略: retain_last_runs={spec['retain_last_runs']}, "
                f"retain_seconds={spec['retain_seconds']}")
        assert spec["retain_last_runs"] == 5
        assert spec["retain_seconds"] == 3600
        bad = client.put("/workflows/w/retention",
                         json={"retain_last_runs": -2}, headers=h)
        explain(f"非法策略 -> {bad.status_code} {bad.json()}")
        assert bad.status_code == 422
        assert bad.json()["error"] == "INVALID_POLICY"
    finally:
        orch.stop()


def test_audit_endpoint_owner_isolated(tmp_path):
    client, orch, release = make_client(tmp_path)
    h_a = {"X-Owner-Id": "alice"}
    h_b = {"X-Owner-Id": "bob"}
    try:
        client.post("/workflows", json={"name": "w", "tasks": [
            {"name": "t", "handler": "block"}],
            "active_run_policy": "reject"}, headers=h_a)
        client.post("/workflows/w/runs", json={}, headers=h_a)
        client.post("/workflows/w/runs", json={}, headers=h_a)
        audit_a = client.get("/audit", headers=h_a).json()
        audit_b = client.get("/audit", headers=h_b).json()
        actions = {a["action"] for a in audit_a}
        explain(f"alice 审计动作集合={actions}; bob 审计条数={len(audit_b)}")
        assert "ACTIVE_CONFLICT_REJECTED" in actions
        assert audit_b == []
        release.set()
    finally:
        orch.stop()


def test_invalid_active_policy_register_422(tmp_path):
    client, orch, release = make_client(tmp_path)
    try:
        r = client.post("/workflows", json={"name": "w", "tasks": [
            {"name": "t", "handler": "ok"}],
            "active_run_policy": "weird"},
            headers={"X-Owner-Id": "alice"})
        explain(f"非法并发策略 -> {r.status_code} {r.json()}")
        assert r.status_code == 422
        assert r.json()["error"] == "INVALID_POLICY"
    finally:
        orch.stop()
