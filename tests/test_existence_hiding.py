"""越权查询的存在性隐藏（信息不可区分性）：

对"按 run_id 进入"的所有入口（运行详情 / 执行历史 / 取消），
越权方在以下三种目标上拿到的响应必须逐字节一致：
  1. 别人名下仍然存在的运行；
  2. 别人名下已被保留策略清理的运行；
  3. 随机编造、从未存在的 run_id。
一致维度：HTTP 状态码、错误码、响应体原始字节（字段与值）。

同时保证合法语义不回退：
  - 自己查询自己被清理的运行 -> 410 RUN_PURGED，与"不存在"(404) 可区分；
  - 自己查询自己正常运行的路径不受影响。
"""
import time

from .conftest import explain
from .test_new_features_api import make_client

H_ALICE = {"X-Owner-Id": "alice"}
H_BOB = {"X-Owner-Id": "bob"}

# 所有按 run_id 进入的入口
ENDPOINTS = (
    ("GET", "/runs/{rid}"),
    ("GET", "/runs/{rid}/events"),
    ("POST", "/runs/{rid}/cancel"),
)


def _wait_terminal(client, run_id, headers):
    deadline = time.time() + 5
    while time.time() < deadline:
        body = client.get(f"/runs/{run_id}", headers=headers).json()
        if body["run"]["status"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return body["run"]["status"]
        time.sleep(0.02)
    raise AssertionError(f"run {run_id} 未在超时内进入终态")


def _trigger(client, name):
    r = client.post(f"/workflows/{name}/runs", json={}, headers=H_ALICE)
    assert r.status_code == 201
    return r.json()["id"]


def _probe(client, method, path, headers):
    return client.request(method, path, headers=headers)


def test_cross_owner_responses_are_byte_identical(tmp_path):
    """三种 id（他人存活 / 他人已清理 / 从未存在）越权响应完全一致。"""
    client, orch, _ = make_client(tmp_path)
    try:
        # alice 的两条运行：一条保留（无清理策略），一条会被清理
        client.post("/workflows", json={"name": "w_keep", "tasks": [
            {"name": "t", "handler": "ok"}]}, headers=H_ALICE)
        client.post("/workflows", json={"name": "w_purge", "tasks": [
            {"name": "t", "handler": "ok"}], "retain_last_runs": 0},
            headers=H_ALICE)
        alive_id = _trigger(client, "w_keep")
        purged_id = _trigger(client, "w_purge")
        _wait_terminal(client, alive_id, H_ALICE)
        _wait_terminal(client, purged_id, H_ALICE)
        deleted = orch.enforce_retention_once()
        assert deleted >= 1 and orch.storage.get_run(purged_id) is None
        assert orch.storage.get_tombstone(purged_id) is not None
        assert orch.storage.get_run(alive_id) is not None

        targets = {
            "他人-存活运行": alive_id,
            "他人-已清理运行": purged_id,
            "从未存在": "nonexistent-" + "0" * 24,
        }
        explain(f"输入: bob 分别探测 {list(targets)}；"
                "期望三种 id 在每个入口的状态码/错误码/响应体完全一致")
        for method, tmpl in ENDPOINTS:
            responses = {label: _probe(client, method, tmpl.format(rid=rid),
                                       H_BOB)
                         for label, rid in targets.items()}
            base = responses["从未存在"]
            for label, resp in responses.items():
                explain(f"  {method} {tmpl} [{label}] -> "
                        f"{resp.status_code} {resp.text}")
                # 状态码一致
                assert resp.status_code == base.status_code == 404
                # 错误码一致
                assert resp.json()["error"] == base.json()["error"]
                # 响应体逐字节一致（字段与值无任何差别）
                assert resp.content == base.content, (
                    f"{method} {tmpl}: [{label}] 与 [从未存在] 响应体不同: "
                    f"{resp.text} != {base.text}")
            # 不夹带任何关于目标运行的信息：无 run_id/状态/清理时间等字段
            assert set(base.json()) == {"error", "detail"}
            for label, rid in targets.items():
                assert rid not in base.text
            for leaked in ("purged_at", "terminal_status", "reason",
                           "owner", "status"):
                assert leaked not in base.json()
    finally:
        orch.stop()


def test_own_purged_run_still_distinguishable(tmp_path):
    """自己查自己被清理的运行仍是 410 RUN_PURGED，与'不存在'分得开。"""
    client, orch, _ = make_client(tmp_path)
    try:
        client.post("/workflows", json={"name": "w", "tasks": [
            {"name": "t", "handler": "ok"}], "retain_last_runs": 0},
            headers=H_ALICE)
        rid = _trigger(client, "w")
        _wait_terminal(client, rid, H_ALICE)
        orch.enforce_retention_once()

        purged = client.get(f"/runs/{rid}", headers=H_ALICE)
        missing = client.get("/runs/" + "nonexistent-" + "1" * 24,
                             headers=H_ALICE)
        explain(f"输入: alice 查自己被清理运行 -> {purged.status_code} "
                f"{purged.json()}; 查不存在 id -> {missing.status_code} "
                f"{missing.json()}")
        # 被清理：410 + RUN_PURGED，且携带清理时间/终态/原因（自己的数据可披露）
        assert purged.status_code == 410
        assert purged.json()["error"] == "RUN_PURGED"
        assert purged.json()["purged_at"]
        assert purged.json()["terminal_status"] == "SUCCEEDED"
        # 从未存在：404，两者状态码与错误码都不同 -> 可区分
        assert missing.status_code == 404
        assert missing.json()["error"] == "RUN_NOT_FOUND"
        assert purged.status_code != missing.status_code
        assert purged.json()["error"] != missing.json()["error"]
        # 事件入口同样区分
        purged_ev = client.get(f"/runs/{rid}/events", headers=H_ALICE)
        assert purged_ev.status_code == 410
        # 自己正常存在的运行不受影响
        client.post("/workflows", json={"name": "w2", "tasks": [
            {"name": "t", "handler": "ok"}]}, headers=H_ALICE)
        ok_id = _trigger(client, "w2")
        ok = client.get(f"/runs/{ok_id}", headers=H_ALICE)
        explain(f"判定依据: 自己正常运行 -> {ok.status_code}")
        assert ok.status_code == 200
        assert ok.json()["run"]["id"] == ok_id
    finally:
        orch.stop()
