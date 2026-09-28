"""FastAPI HTTP 入口。

隔离边界：调用方必须携带 X-Owner-Id 头；所有读写按 owner 过滤，
访问他人资源一律返回 404（不泄露存在性），非法请求返回 409/422 并
携带可区分的原因码。
"""
from __future__ import annotations

import dataclasses
import enum

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from .config import Settings
from .executor import Executor, HandlerRegistry
from .models import (
    ActiveRunPolicy, Rejected, RejectReason, RetentionPolicy,
    WorkflowSpec, TaskSpec,
)
from .scheduler import Orchestrator
from .storage import Storage


class TaskSpecIn(BaseModel):
    name: str
    handler: str
    depends_on: list[str] = Field(default_factory=list)
    max_attempts: int = 1
    backoff_seconds: float = 0.0
    timeout_seconds: float | None = None


class RetentionIn(BaseModel):
    keep_last: int | None = Field(default=None, ge=1)
    keep_seconds: float | None = Field(default=None, gt=0)


class WorkflowIn(BaseModel):
    name: str
    tasks: list[TaskSpecIn]
    schedule_interval_seconds: float | None = None
    active_run_policy: str = ActiveRunPolicy.ALLOW.value
    retention: RetentionIn = Field(default_factory=RetentionIn)


class TriggerIn(BaseModel):
    idempotency_key: str | None = None


class PolicyIn(BaseModel):
    active_run_policy: str | None = None
    retention: RetentionIn | None = None


def _jsonable(obj):
    from .models import TriggerResult
    if isinstance(obj, TriggerResult):
        body = _jsonable(obj.run)
        body["outcome"] = obj.outcome.value
        body["detail"] = obj.detail
        return body
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return {k: _jsonable(v) for k, v in dataclasses.asdict(obj).items()}
    if isinstance(obj, enum.Enum):
        return obj.value
    if isinstance(obj, list):
        return [_jsonable(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _jsonable(v) for k, v in obj.items()}
    return obj


_STATUS = {
    RejectReason.WORKFLOW_NOT_FOUND: 404,
    RejectReason.RUN_NOT_FOUND: 404,
    RejectReason.FORBIDDEN: 404,  # 不泄露他人资源的存在性
    RejectReason.RUN_REAPED: 410,  # 曾存在但被保留策略清理（仅本 owner 可见）
    RejectReason.UNKNOWN_TASK: 422,
    RejectReason.DEPENDENCY_CYCLE: 422,
    RejectReason.DUPLICATE_WORKFLOW: 409,
    RejectReason.DUPLICATE_RUN: 409,
    RejectReason.ACTIVE_RUN_EXISTS: 409,
    RejectReason.INVALID_POLICY: 422,
    RejectReason.INVALID_RETENTION: 422,
    RejectReason.QUEUE_FULL: 429,
    RejectReason.RUN_NOT_ACTIVE: 409,
}


def create_app(settings: Settings | None = None,
               orchestrator: Orchestrator | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    if orchestrator is None:
        storage = Storage(settings.db_path)
        registry = HandlerRegistry()
        from .handlers import register_builtin_handlers
        register_builtin_handlers(registry)
        orchestrator = Orchestrator(
            storage, Executor(settings.max_concurrent_tasks),
            settings, registry)
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def lifespan(_):
        orchestrator.start()
        yield
        orchestrator.stop()

    app = FastAPI(title="workflow-orchestrator", lifespan=lifespan)
    app.state.orchestrator = orchestrator

    def owner_of(x_owner_id: str | None) -> str:
        if not x_owner_id:
            raise HTTPException(401, "missing X-Owner-Id header")
        return x_owner_id

    @app.exception_handler(Rejected)
    def _rejected(_, exc: Rejected):
        from fastapi.responses import JSONResponse
        status = _STATUS.get(exc.reason, 400)
        error = exc.reason.value
        detail = exc.detail
        if exc.reason in (RejectReason.RUN_NOT_FOUND, RejectReason.FORBIDDEN):
            # 运行级 404（不存在/越权访问他人资源/越权探测他人墓碑）给出完全
            # 一致的固定响应体，不回显 run_id 等任何输入，杜绝存在性侧信道。
            # 注意：RUN_REAPED 是 410，只会返回给资源真正的 owner。
            error = RejectReason.RUN_NOT_FOUND.value
            detail = "run not found"
        return JSONResponse(
            status_code=status, content={"error": error, "detail": detail})

    @app.post("/workflows", status_code=201)
    def register(body: WorkflowIn, x_owner_id: str | None = Header(None)):
        owner = owner_of(x_owner_id)
        spec = WorkflowSpec(
            name=body.name, owner=owner,
            tasks=[TaskSpec(**t.model_dump()) for t in body.tasks],
            schedule_interval_seconds=body.schedule_interval_seconds,
            active_run_policy=body.active_run_policy,
            retention=RetentionPolicy(**body.retention.model_dump()))
        wf = orchestrator.register_workflow(spec)
        return _jsonable(wf)

    @app.get("/workflows")
    def list_workflows(x_owner_id: str | None = Header(None)):
        owner = owner_of(x_owner_id)
        return [_jsonable(w) for w in
                orchestrator.storage.list_workflows(owner)]

    @app.put("/workflows/{name}/policies")
    def update_policies(name: str, body: PolicyIn,
                        x_owner_id: str | None = Header(None)):
        owner = owner_of(x_owner_id)
        retention = (RetentionPolicy(**body.retention.model_dump())
                     if body.retention is not None else None)
        wf = orchestrator.update_policies(
            owner, name, active_run_policy=body.active_run_policy,
            retention=retention)
        return _jsonable(wf)

    @app.post("/workflows/{name}/retention:enforce")
    def enforce_retention(name: str,
                          x_owner_id: str | None = Header(None)):
        """立即按该工作流的保留策略执行一次清理（后台周期清理之外的手动入口）。"""
        owner = owner_of(x_owner_id)
        wf = orchestrator.storage.get_workflow_by_name(owner, name)
        if not wf:
            raise Rejected(RejectReason.WORKFLOW_NOT_FOUND, name)
        reaped = orchestrator.enforce_retention(wf)
        return {"workflow": name, "reaped": reaped}

    @app.get("/audit")
    def list_audit(workflow: str | None = None,
                   x_owner_id: str | None = Header(None)):
        owner = owner_of(x_owner_id)
        return orchestrator.list_audit(owner, workflow)

    @app.post("/workflows/{name}/runs", status_code=201)
    def trigger(name: str, body: TriggerIn | None = None,
                x_owner_id: str | None = Header(None)):
        owner = owner_of(x_owner_id)
        key = body.idempotency_key if body else None
        return _jsonable(orchestrator.trigger(owner, name, key))

    @app.get("/runs")
    def list_runs(x_owner_id: str | None = Header(None)):
        owner = owner_of(x_owner_id)
        return [_jsonable(r) for r in orchestrator.storage.list_runs(owner)]

    @app.get("/runs/{run_id}")
    def get_run(run_id: str, x_owner_id: str | None = Header(None)):
        owner = owner_of(x_owner_id)
        return _jsonable(orchestrator.get_run_view(owner, run_id))

    @app.get("/runs/{run_id}/events")
    def get_events(run_id: str, x_owner_id: str | None = Header(None)):
        owner = owner_of(x_owner_id)
        return orchestrator.get_run_events(owner, run_id)

    @app.post("/runs/{run_id}/cancel")
    def cancel(run_id: str, x_owner_id: str | None = Header(None)):
        owner = owner_of(x_owner_id)
        return _jsonable(orchestrator.cancel(owner, run_id))

    return app
