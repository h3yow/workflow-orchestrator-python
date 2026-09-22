"""任务处理器注册表：handler 名称 -> 可调用实现。

处理器签名：handler(params: dict, context: dict) -> 任意可 JSON 序列化值。

context 包含：
  run_id, task_key, tenant_id, attempt_no（从 1 开始）,
  idempotency_key —— 稳定的幂等键：同一任务的同一次尝试在重试/重启后保持不变，
  previous_attempt —— 之前是否已尝试过（>1 表示重试）。

副作用幂等约定：重试不会在调度层造成“同一 attempt 重复执行”，但崩溃恢复可能
重放同一 attempt；有外部副作用的处理器应使用 context["idempotency_key"] 去重。
"""
from __future__ import annotations

from collections.abc import Callable
from typing import Any

TaskHandler = Callable[[dict[str, Any], dict[str, Any]], Any]

_REGISTRY: dict[str, TaskHandler] = {}


def register_handler(name: str, fn: TaskHandler) -> None:
    if not name:
        raise ValueError("handler 名称不能为空")
    _REGISTRY[name] = fn


def get_handler(name: str) -> TaskHandler | None:
    return _REGISTRY.get(name)


def clear_handlers() -> None:
    """仅供测试使用。"""
    _REGISTRY.clear()
