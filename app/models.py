"""领域模型与枚举（状态机定义）。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class TaskState(str, enum.Enum):
    PENDING = "PENDING"
    READY = "READY"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    RETRY_WAIT = "RETRY_WAIT"
    TIMED_OUT = "TIMED_OUT"
    SKIPPED = "SKIPPED"


class RunState(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class ConcurrencyPolicy(str, enum.Enum):
    REJECT = "REJECT"          # 已有活动 run 时拒绝新触发
    SKIP = "SKIP"              # 去重：返回已有 run
    QUEUE = "QUEUE"            # 排队等待（受队列上限约束）


@dataclass(frozen=True)
class RetryPolicy:
    max_retries: int = 0
    backoff_seconds: float = 0.0
    backoff_multiplier: float = 1.0

    def delay_for(self, failed_attempt: int) -> float:
        """第 failed_attempt 次执行失败后，下一次重试前需等待的秒数。

        指数退避：backoff_seconds * multiplier**(failed_attempt-1)。
        failed_attempt 从 1 开始（首次执行失败后等 backoff_seconds）。
        """
        if failed_attempt < 1:
            return 0.0
        return self.backoff_seconds * (self.backoff_multiplier ** (failed_attempt - 1))


@dataclass(frozen=True)
class TaskDef:
    key: str
    handler: str
    depends_on: tuple[str, ...] = ()
    params: dict[str, Any] = field(default_factory=dict)
    timeout_seconds: float | None = None
    retry: RetryPolicy | None = None


@dataclass(frozen=True)
class WorkflowDef:
    name: str
    version: int
    tasks: tuple[TaskDef, ...]
    concurrency: ConcurrencyPolicy = ConcurrencyPolicy.REJECT
    max_queued_runs: int = 0


@dataclass
class WorkflowSpec:
    tenant_id: str
    definition: WorkflowDef


@dataclass
class RunInfo:
    id: str
    tenant_id: str
    workflow_name: str
    version: int
    state: RunState
    trigger: str
    queued_at: str
    started_at: str | None
    finished_at: str | None
    error_code: str | None
    error_message: str | None


@dataclass
class TaskAttempt:
    attempt_no: int
    state: str
    started_at: str
    finished_at: str | None
    error_code: str | None
    error_message: str | None
    timed_out: bool


@dataclass
class TaskInfo:
    id: str
    run_id: str
    task_key: str
    state: TaskState
    attempt_count: int
    scheduled_at: str | None
    started_at: str | None
    finished_at: str | None
    error_code: str | None
    error_message: str | None
    attempts: list[TaskAttempt] = field(default_factory=list)


@dataclass
class DecisionLog:
    id: int
    ts: str
    tenant_id: str
    run_id: str | None
    task_id: str | None
    kind: str
    detail: str
