"""HTTP 层：鉴权、可区分错误码、并发触发 API、越权隔离与定时计划管理。"""
from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from app.handlers import clear_handlers, register_handler

HA = {"X-API-Key": "dev-key-a"}
HB = {"X-API-Key": "dev-key-b"}


@pytest.fixture
def client(tmp_path):
    from app.main import create_app
    clear_handlers()
    register_handler("instant", lambda p, c: "ok")
    register_handler("medium", lambda p, c: time.sleep(0.15) or "ok")
    app = create_app(db_path=str(tmp_path / "api.db"),
                     start_scheduler=True, poll_interval=0.01)
    with TestClient(app) as c:
        yield c
    app.state.engine.shutdown()
    clear_handlers()


def _def(name="wf", **kw):
    body = {"name": name, "tasks": [{"key": "a", "handler": "instant"}]}
    body.update(kw)
    return body


def test_auth_required_and_keys_distinguish_tenants(client):
    print("[输入] 无 key / 错误 key / 合法 key；判定依据=前两者 401，合法 key 200")
    assert client.get("/api/v1/workflows").status_code == 401
    assert client.get("/api/v1/workflows",
                      headers={"X-API-Key": "nope"}).status_code == 401
    assert client.get("/api/v1/workflows", headers=HA).status_code == 200
    print("[判定] 401 与 200 正确区分")


def test_validation_error_codes_over_http(client):
    print("[输入] 通过 HTTP 提交成环/缺失依赖定义；"
          "判定依据=422 且 detail.code 分别可区分")
    r = client.post("/api/v1/workflows", headers=HA, json={
        "name": "cyc",
        "tasks": [
            {"key": "a", "handler": "h", "depends_on": ["b"]},
            {"key": "b", "handler": "h", "depends_on": ["a"]},
        ],
    })
    assert r.status_code == 422
    assert r.json()["detail"]["code"] == "CYCLIC_DEPENDENCY"
    r = client.post("/api/v1/workflows", headers=HA, json={
        "name": "miss",
        "tasks": [{"key": "a", "handler": "h", "depends_on": ["x"]}],
    })
    assert r.status_code == 422
    assert r.json()["detail"]["code"] == "MISSING_DEPENDENCY"
    print("[判定] 两类拒绝在 HTTP 层仍可区分")


def test_full_lifecycle_and_isolation(client):
    print("[输入] A 定义/触发/查询，B 越权访问；"
          "判定依据=A 全流程 200/终态 SUCCEEDED，B 全部 404")
    assert client.post("/api/v1/workflows", headers=HA, json=_def()).status_code == 200
    r = client.post("/api/v1/workflows/wf/trigger", headers=HA, json={})
    rid = r.json()["run_id"]
    for _ in range(100):
        state = client.get(f"/api/v1/runs/{rid}", headers=HA).json()["state"]
        if state in ("SUCCEEDED", "FAILED"):
            break
        time.sleep(0.02)
    assert state == "SUCCEEDED"
    tasks = client.get(f"/api/v1/runs/{rid}/tasks", headers=HA).json()["tasks"]
    assert tasks[0]["state"] == "SUCCEEDED"
    history = client.get(f"/api/v1/runs/{rid}/history", headers=HA).json()["history"]
    assert any(h["kind"] == "RUN_SUCCEEDED" for h in history)

    assert client.get(f"/api/v1/runs/{rid}", headers=HB).status_code == 404
    assert client.get(f"/api/v1/runs/{rid}/tasks", headers=HB).status_code == 404
    assert client.get(f"/api/v1/runs/{rid}/history", headers=HB).status_code == 404
    assert client.get("/api/v1/workflows", headers=HB).json()["workflows"] == []
    print(f"[判定] 历史 {len(history)} 条，跨租户访问全部 404")


def test_concurrent_trigger_http_reject(client):
    print("[输入] 经 HTTP 并发触发 16 次 REJECT 工作流；"
          "判定依据=仅 1 个 200，其余 409 ALREADY_ACTIVE，最终只有一个成功运行")
    import concurrent.futures
    client.post("/api/v1/workflows", headers=HA, json={
        "name": "slow",
        "tasks": [{"key": "a", "handler": "medium"}],
        "concurrency": "REJECT",
    })

    def fire():
        return client.post("/api/v1/workflows/slow/trigger", headers=HA, json={})

    with concurrent.futures.ThreadPoolExecutor(16) as pool:
        responses = list(pool.map(lambda _: fire(), range(16)))
    codes = sorted(r.status_code for r in responses)
    assert codes.count(200) == 1
    assert all(c in (200, 409) for c in codes)
    body_codes = {r.json()["detail"]["code"] for r in responses if r.status_code == 409}
    assert body_codes == {"ALREADY_ACTIVE"}
    print(f"[判定] 状态码分布 200x1 / 409x{codes.count(409)}，错误码唯一可区分")

    runs = client.get("/api/v1/runs?workflow_name=slow", headers=HA).json()["runs"]
    run_ids = [r["run_id"] for r in runs]
    assert len(run_ids) == 1
    for _ in range(200):
        state = client.get(f"/api/v1/runs/{run_ids[0]}",
                           headers=HA).json()["state"]
        if state in ("SUCCEEDED", "FAILED"):
            break
        time.sleep(0.01)
    assert state == "SUCCEEDED"


def test_queue_full_http_status(client):
    print("[输入] QUEUE 容量 1（1 运行），运行期间两次触发；"
          "判定依据=第一次入队 200，第二次 429 QUEUE_FULL")
    client.post("/api/v1/workflows", headers=HA, json={
        "name": "q", "tasks": [{"key": "a", "handler": "medium"}],
        "concurrency": "QUEUE", "max_queued_runs": 0,
    })
    assert client.post("/api/v1/workflows/q/trigger", headers=HA,
                       json={}).status_code == 200
    r1 = client.post("/api/v1/workflows/q/trigger", headers=HA, json={})
    assert r1.status_code == 429 and r1.json()["detail"]["code"] == "QUEUE_FULL"
    print("[判定] 积压拒绝映射为 HTTP 429，错误码 QUEUE_FULL")


def test_schedule_crud_http(client):
    print("[输入] 创建/列出/删除定时计划，重复创建需 409，删除未知计划 404")
    client.post("/api/v1/workflows", headers=HA, json=_def())
    r = client.post("/api/v1/schedules", headers=HA,
                    json={"name": "s1", "workflow_name": "wf",
                          "interval_seconds": 60})
    assert r.status_code == 200
    r = client.post("/api/v1/schedules", headers=HA,
                    json={"name": "s1", "workflow_name": "wf",
                          "interval_seconds": 60})
    assert r.status_code == 409 and r.json()["detail"]["code"] == "DUPLICATE_SCHEDULE"
    listed = client.get("/api/v1/schedules", headers=HA).json()["schedules"]
    assert len(listed) == 1 and listed[0]["schedule_name"] == "s1"
    # 租户 B 不可见
    assert client.get("/api/v1/schedules", headers=HB).json()["schedules"] == []
    assert client.delete("/api/v1/schedules/s1", headers=HA).status_code == 200
    assert client.delete("/api/v1/schedules/s1", headers=HA).status_code == 404
    print("[判定] 定时计划 CRUD、重复拒绝与租户隔离全部符合预期")
