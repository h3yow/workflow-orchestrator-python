"""HTTP 请求/响应 schema。"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class RetrySpec(BaseModel):
    max_retries: int = Field(default=0, ge=0)
    backoff_seconds: float = Field(default=0.0, ge=0)
    backoff_multiplier: float = Field(default=1.0, ge=1.0)


class TaskSpec(BaseModel):
    key: str
    handler: str
    depends_on: list[str] = Field(default_factory=list)
    params: dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: float | None = Field(default=None, gt=0)
    retry: RetrySpec | None = None


class WorkflowSpecIn(BaseModel):
    name: str = Field(min_length=1)
    version: int = 1
    tasks: list[TaskSpec] = Field(min_length=1)
    concurrency: str = "REJECT"  # REJECT / SKIP / QUEUE
    max_queued_runs: int = Field(default=0, ge=0)


class TriggerIn(BaseModel):
    trigger: str = "manual"


class ScheduleIn(BaseModel):
    name: str = Field(min_length=1)
    workflow_name: str = Field(min_length=1)
    interval_seconds: float = Field(gt=0)
