"""内置示例任务处理器，便于本地手工验证（curl）。"""
from __future__ import annotations

import time
from typing import Any

from app.handlers import register_handler


def echo(params: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    """原样回显参数，并附带执行上下文摘要。"""
    return {
        "params": params,
        "run_id": context["run_id"],
        "task_key": context["task_key"],
        "attempt_no": context["attempt_no"],
        "idempotency_key": context["idempotency_key"],
    }


def sleep_then_echo(params: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    """睡眠 params.seconds（默认 0.1s）后回显，便于演示并发/超时。"""
    seconds = float(params.get("seconds", 0.1))
    time.sleep(seconds)
    return echo(params, context)


def fail_times(params: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    """前 fail_until 次尝试失败，之后成功；用于手工演示重试。

    注意处理器是无状态函数，跨进程的“第几次”需由调用方借助
    context["idempotency_key"] 自行持久化；此处仅按 attempt_no 演示：
    params.fail_attempts（默认 1）表示第几次尝试之前都失败。
    """
    fail_attempts = int(params.get("fail_attempts", 1))
    if context["attempt_no"] <= fail_attempts:
        raise RuntimeError(f"内置演示失败：第 {context['attempt_no']} 次尝试")
    return echo(params, context)


def register_builtins() -> None:
    register_handler("builtin.echo", echo)
    register_handler("builtin.sleep", sleep_then_echo)
    register_handler("builtin.fail_times", fail_times)
