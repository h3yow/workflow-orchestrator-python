"""内置处理器：便于本地验证与演示，全部幂等、无外部依赖。"""
from __future__ import annotations

import time
from typing import Any

from .executor import HandlerRegistry


def _echo(payload: dict[str, Any]) -> dict:
    return {"echo": payload}


def _sleep(payload: dict[str, Any]) -> str:
    time.sleep(float(payload.get("seconds", 0.1)))
    return "slept"


def _fail(payload: dict[str, Any]) -> None:
    raise RuntimeError("intentional failure (builtin fail handler)")


def register_builtin_handlers(reg: HandlerRegistry) -> None:
    reg.register("echo", _echo)
    reg.register("sleep", _sleep)
    reg.register("fail", _fail)
