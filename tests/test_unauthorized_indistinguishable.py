"""越权查询不可区分性测试：

- 越权方（bob）对“他人存活运行 / 他人已被清理运行 / 随机不存在 id”
  三种输入，在运行详情、执行历史、取消三个入口上拿到的响应必须**完全一致**
  （状态码、错误码、响应体字段与值逐字节相同，不回显 id、不夹带对方
  状态/清理时间/原因）；
- 本人查询自己被清理的运行仍返回 410 RUN_PURGED（带终态/清理时间/原因），
  与本人查“从未存在”的 404 继续可区分；
- 越权取消不得对他人运行产生任何副作用。
"""
import threading
import time
import uuid

from fastapi.testclient import TestClient

from app.api import create_app
from app.config import Settings
from app.executor import Executor, HandlerRegistry
from app.scheduler import Orchestrator
from app.storage import Storage

from .conftest import explain

HIDDEN_BODY = {"error": "RUN_NOT_FOUND", "detail": "run not found"}


def make_client(tmp_path):
    settings = Settings(db_path=str(tmp_path / "api.db"),
                        tick_interval_seconds=0.01)
    release = threading.Event()
    reg = HandlerRegistry()
    reg.register("ok", lambda p: "ok")
    reg.register("block", lambda p: release.wait(timeout=10))
    orch = Orchestrator(Storage(settings.db_path),
                        Executor(settings.max_concurrent_tasks),
                        settings, reg)
    client = TestClient(create_app(settings, orch))
    client.__enter__()  # 触发 startup（调度循环开始）
    return client, orch, release


def _wait_terminal(client, headers, run_id, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        st = client.get(f"/runs/{run_id}", headers=headers).json()["run"][
            "status"]
        if st in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return st
        time.sleep(0.02)
    raise AssertionError(f"run {run_id} did not finish in time")


def test_cross_owner_three_ids_indistinguishable_all_entries(tmp_path):
    """三种 id 在三个按运行入口上对越权方完全一致。"""
    client, orch, release = make_client(tmp_path)
    h_a = {"X-Owner-Id": "alice"}
    h_b = {"X-Owner-Id": "bob"}
    try:
        # 1) 他人名下“活着”的运行（block 处理器使其停留在 RUNNING）
        client.post("/workflows", json={"name": "live", "tasks": [
            {"name": "t", "handler": "block"}]}, headers=h_a)
        alive_id = client.post("/workflows/live/runs", json={},
                                headers=h_a).json()["id"]
        deadline = time.time() + 5
        while time.time() < deadline:
            st = client.get(f"/runs/{alive_id}", headers=h_a).json()["run"][
                "status"]
            if st == "RUNNING":
                break
            time.sleep(0.02)
        assert st == "RUNNING", st

        # 2) 他人名下“已被保留策略清理”的运行
        client.post("/workflows", json={"name": "purged", "tasks": [
            {"name": "t", "handler": "ok"}], "retain_last_runs": 0},
            headers=h_a)
        purged_id = client.post("/workflows/purged/runs", json={},
                                 headers=h_a).json()["id"]
        assert _wait_terminal(client, h_a, purged_id) == "SUCCEEDED"
        orch.enforce_retention_once()
        purged_self = client.get(f"/runs/{purged_id}", headers=h_a)
        assert purged_self.status_code == 410, "前置：本人应见 410 墓碑"

        # 3) 随机不存在的 id
        missing_id = uuid.uuid4().hex

        targets = [(alive_id, "他人-存活"),
                   (purged_id, "他人-已清理"),
                   (missing_id, "随机不存在")]
        entries = [
            ("GET", lambda rid: client.get(f"/runs/{rid}", headers=h_b)),
            ("GET/events", lambda rid: client.get(
                f"/runs/{rid}/events", headers=h_b)),
            ("POST/cancel", lambda rid: client.post(
                f"/runs/{rid}/cancel", headers=h_b)),
        ]
        explain(f"输入: bob 越权查询三种 id={[(t, i[:8]) for i, t in targets]}"
                f" x 三个入口; 期望 9 个响应 (404, {HIDDEN_BODY}) 完全一致")

        seen = []
        for entry_name, call in entries:
            for rid, label in targets:
                r = call(rid)
                seen.append((entry_name, label, r.status_code, r.text,
                             r.json()))

        # 状态码与响应体（原始文本，逐字节）三者两两一致
        signatures = {(status, text) for _, _, status, text, _ in seen}
        explain(f"判定依据: 去重后的响应签名数={len(signatures)} -> {signatures}")
        assert len(signatures) == 1, f"越权响应不一致: {signatures}"

        status, text = next(iter(signatures))
        assert status == 404
        # 响应体字段与值固定，不回显 id，不夹带任何对方运行信息
        for _, _, _, _, body in seen:
            assert body == HIDDEN_BODY
            assert set(body.keys()) == {"error", "detail"}
            assert alive_id not in text and purged_id not in text
        assert text == '{"error":"RUN_NOT_FOUND","detail":"run not found"}'
        explain("判定依据: 全部为恒定 404 RUN_NOT_FOUND，且无 purged_at/"
                "terminal_status/reason/run_id 等字段")

        # 越权取消不得对他人存活运行产生副作用：alice 仍能正常查到并取消
        view = client.get(f"/runs/{alive_id}", headers=h_a)
        assert view.status_code == 200
        assert view.json()["run"]["status"] == "RUNNING"
        cancel_self = client.post(f"/runs/{alive_id}/cancel", headers=h_a)
        assert cancel_self.status_code == 200
        explain("判定依据: bob 的越权取消后，alice 本人查询仍 200 且可自行取消")
        release.set()
    finally:
        release.set()
        orch.stop()


def test_owner_purged_still_distinguishable_from_missing(tmp_path):
    """本人查自己被清理的运行（410）与查不存在（404）继续可区分。"""
    client, orch, release = make_client(tmp_path)
    h = {"X-Owner-Id": "alice"}
    try:
        client.post("/workflows", json={"name": "w", "tasks": [
            {"name": "t", "handler": "ok"}], "retain_last_runs": 0},
            headers=h)
        rid = client.post("/workflows/w/runs", json={}, headers=h).json()["id"]
        assert _wait_terminal(client, h, rid) == "SUCCEEDED"
        deleted = orch.enforce_retention_once()
        missing_id = uuid.uuid4().hex
        explain(f"输入: 本人查已清理 run={rid[:8]}（清理 {deleted} 条）与"
                f"不存在 id={missing_id[:8]}; 期望 410 RUN_PURGED vs 404")

        purged = client.get(f"/runs/{rid}", headers=h)
        missing = client.get(f"/runs/{missing_id}", headers=h)
        explain(f"判定依据(已清理): {purged.status_code} {purged.json()}")
        explain(f"判定依据(不存在): {missing.status_code} {missing.json()}")

        assert purged.status_code == 410
        pb = purged.json()
        assert pb["error"] == "RUN_PURGED"
        assert pb["terminal_status"] == "SUCCEEDED"
        assert isinstance(pb["purged_at"], float) and pb["purged_at"] > 0
        assert pb["reason"]

        assert missing.status_code == 404
        assert missing.json() == HIDDEN_BODY
        assert (purged.status_code, purged.text) != (missing.status_code,
                                                      missing.text)

        # 执行历史入口同样保持“已清理可区分”
        purged_events = client.get(f"/runs/{rid}/events", headers=h)
        missing_events = client.get(f"/runs/{missing_id}/events", headers=h)
        assert purged_events.status_code == 410
        assert purged_events.json()["error"] == "RUN_PURGED"
        assert missing_events.status_code == 404
        assert missing_events.json() == HIDDEN_BODY

        # 合法路径不受影响：本人存活运行仍 200
        client.post("/workflows", json={"name": "live", "tasks": [
            {"name": "t", "handler": "ok"}]}, headers=h)
        live_id = client.post("/workflows/live/runs", json={},
                               headers=h).json()["id"]
        assert _wait_terminal(client, h, live_id) == "SUCCEEDED"
        ok = client.get(f"/runs/{live_id}", headers=h)
        assert ok.status_code == 200 and ok.json()["run"]["id"] == live_id
        explain("判定依据: 本人正常运行查询仍 200，合法路径未受影响")
    finally:
        release.set()
        orch.stop()
