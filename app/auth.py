"""极简租户鉴权：基于本地配置的 API Key，隔离不同调用方数据。

API Key 通过环境变量 WORKFLOW_API_KEYS 配置，格式 "key1=tenantA,key2=tenantB"。
未配置时使用开发默认值（仅限本地验证）：dev-key-a -> tenant-a, dev-key-b -> tenant-b。
"""
from __future__ import annotations

import os
import threading

_DEFAULT = "dev-key-a=tenant-a,dev-key-b=tenant-b"
_LOCK = threading.Lock()
_MAP: dict[str, str] | None = None


def _load_map() -> dict[str, str]:
    global _MAP
    with _LOCK:
        if _MAP is None:
            raw = os.environ.get("WORKFLOW_API_KEYS", _DEFAULT)
            mapping: dict[str, str] = {}
            for pair in raw.split(","):
                pair = pair.strip()
                if not pair:
                    continue
                if "=" not in pair:
                    continue
                key, tenant = pair.split("=", 1)
                mapping[key.strip()] = tenant.strip()
            _MAP = mapping
        return _MAP


def reload_keys() -> None:
    """测试辅助：重新读取环境变量。"""
    global _MAP
    with _LOCK:
        _MAP = None


def resolve_tenant(api_key: str | None) -> str | None:
    """把 API Key 解析为租户 ID；无法识别返回 None（由调用方判 401）。"""
    if not api_key:
        return None
    return _load_map().get(api_key)
