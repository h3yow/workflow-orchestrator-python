"""HTTP 层：并发收敛判定、保留策略配置/清理、410 查询语义、越权不可区分。"""
import time
import uuid

from fastapi.testclient import TestClient

from app.api import create_app
from app.config import Settings
from app.executor import Executor, HandlerRegistry
from app.scheduler import Orchestrator
from app.storage import Storage

from .conftest import explain


def make_client(tmp_path, **overrides):
    settings = Settings(db_path=str(tmp_path / "api.db"),
                        tick_interval_seconds=0.01,
                        schedule_enabled=False, **overrides)
    reg = HandlerRegistry()
    reg.register("ok", lambda p: "ok")
    import threading
    started, release = threading.Event(), threading.Event()
    reg.register("block", lambda p: (started.set(), release.wait(10))[1])
    orch = Orchestrator(Storage(settings.db_path),
                        Executor(settings.max_concurrent_tasks),
                        settings, reg)
    app = create_app(settings, orch)
    client = TestClient(app)
    client.__enter__()
    return client, orch, started, release


def test_trigger_outcome_in_response_and_reject_409(tmp_path):
    client, orch, started, release = make_client(tmp_path)
    try:
        h = {"X-Owner-Id": "alice"}
        client.post("/workflows", json={"name": "w", "tasks": [
            {"name": "t", "handler": "block"}],
            "active_run_policy": "reuse"}, headers=h)
        r1 = client.post("/workflows/w/runs", json={}, headers=h)
        assert started.wait(5)
        r2 = client.post("/workflows/w/runs", json={}, headers=h)
        explain(f"首次触发 outcome={r1.json()['outcome']}, "
                f"并发触发 outcome={r2.json()['outcome']}")
        assert r1.json()["outcome"] == "CREATED"
        assert r2.json()["outcome"] == "REUSED_ACTIVE"
        assert r1.json()["id"] == r2.json()["id"]
        assert r2.json()["detail"].startswith("active_run_id=")
        # 切到 reject 策略
        client.put("/workflows/w/policies",
                   json={"active_run_policy": "reject"}, headers=h)
        r3 = client.post("/workflows/w/runs", json={}, headers=h)
        explain(f"reject 策略下并发触发 -> {r3.status_code} {r3.json()}")
        assert r3.status_code == 409
        assert r3.json()["error"] == "ACTIVE_RUN_EXISTS"
        assert r1.json()["id"] in r3.json()["detail"]
        release.set()
    finally:
        orch.stop()


def test_retention_endpoint_and_410_semantics(tmp_path):
    client, orch, started, release = make_client(tmp_path)
    try:
        h = {"X-Owner-Id": "alice"}
        client.post("/workflows", json={"name": "w", "tasks": [
            {"name": "t", "handler": "ok"}],
            "retention": {"keep_last": 1}}, headers=h)
        r1 = client.post("/workflows/w/runs", json={}, headers=h).json()
        time.sleep(0.15)
        r2 = client.post("/workflows/w/runs", json={}, headers=h).json()
        time.sleep(0.15)
        resp = client.post("/workflows/w/retention:enforce", headers=h)
        explain(f"手动清理 -> {resp.status_code} {resp.json()}")
        assert resp.json()["reaped"] == 1
        # owner 自查：被清理 410，不存在 404
        gone = client.get(f"/runs/{r1['id']}", headers=h)
        missing = client.get(f"/runs/{uuid.uuid4().hex}", headers=h)
        alive = client.get(f"/runs/{r2['id']}", headers=h)
        explain(f"被清理={gone.status_code}({gone.json()['error']}), "
                f"不存在={missing.status_code}, 存活={alive.status_code}")
        assert gone.status_code == 410 and gone.json()["error"] == "RUN_REAPED"
        assert missing.status_code == 404
        assert alive.status_code == 200
        # 审计可查且包含清理范围
        audit = client.get("/audit?workflow=w", headers=h).json()
        kinds = [a["kind"] for a in audit]
        explain(f"审计种类={kinds}")
        assert "RETENTION_REAPED" in kinds
    finally:
        orch.stop()


def test_unauthorized_responses_indistinguishable(tmp_path):
    client, orch, started, release = make_client(tmp_path)
    try:
        ha, hb = {"X-Owner-Id": "alice"}, {"X-Owner-Id": "bob"}
        client.post("/workflows", json={"name": "w", "tasks": [
            {"name": "t", "handler": "ok"}],
            "retention": {"keep_last": 1}}, headers=ha)
        r1 = client.post("/workflows/w/runs", json={}, headers=ha).json()
        time.sleep(0.15)
        r2 = client.post("/workflows/w/runs", json={}, headers=ha).json()
        time.sleep(0.15)
        client.post("/workflows/w/retention:enforce", headers=ha)
        reaped = client.get(f"/runs/{r1['id']}", headers=hb)
        existing = client.get(f"/runs/{r2['id']}", headers=hb)
        missing = client.get(f"/runs/{uuid.uuid4().hex}", headers=hb)
        bodies = {x.text for x in (reaped, existing, missing)}
        explain(f"bob 三种查询: 状态={[x.status_code for x in (reaped, existing, missing)]}, "
                f"响应体集合={bodies}")
        assert [x.status_code for x in (reaped, existing, missing)] == [
            404, 404, 404]
        # 状态码与响应体都必须完全一致，无法借此判断哪个 id 真实存在
        assert bodies == {
            '{"error":"RUN_NOT_FOUND","detail":"run not found"}'}
        assert client.get("/audit", headers=hb).json() == []
    finally:
        orch.stop()


def test_invalid_retention_and_policy_rejected(tmp_path):
    client, orch, started, release = make_client(tmp_path)
    try:
        h = {"X-Owner-Id": "alice"}
        r = client.post("/workflows", json={"name": "w", "tasks": [
            {"name": "t", "handler": "ok"}],
            "retention": {"keep_last": 0}}, headers=h)
        explain(f"keep_last=0 -> {r.status_code} {r.json()}")
        # pydantic 层 ge=1 先挡住 -> 422 校验错误
        assert r.status_code == 422
        client.post("/workflows", json={"name": "w2", "tasks": [
            {"name": "t", "handler": "ok"}]}, headers=h)
        r = client.put("/workflows/w2/policies",
                       json={"active_run_policy": "wat"}, headers=h)
        explain(f"非法策略 -> {r.status_code} {r.json()}")
        assert r.status_code == 422 and r.json()["error"] == "INVALID_POLICY"
        r = client.put("/workflows/w2/policies",
                       json={"retention": {"keep_seconds": -1}}, headers=h)
        assert r.status_code == 422
    finally:
        orch.stop()
