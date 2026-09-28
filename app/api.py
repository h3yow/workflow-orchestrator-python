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
from .models import Rejected, RejectReason, WorkflowSpec, TaskSpec
from .scheduler import Orchestrator
from .storage import Storage


class TaskSpecIn(BaseModel):
    name: str
    handler: str
    depends_on: list[str] = Field(default_factory=list)
    max_attempts: int = 1
    backoff_seconds: float = 0.0
    timeout_seconds: float | None = None


class WorkflowIn(BaseModel):
    name: str
    tasks: list[TaskSpecIn]
    schedule_interval_seconds: float | None = None
    # 并发收敛：allow（默认，可并行）| reuse（复用活跃运行）| reject（拒绝）
    active_run_policy: str = "allow"
    # 历史保留：仅对已结束运行生效；None 表示该维度不限制
    retain_last_runs: int | None = None
    retain_seconds: float | None = None
    tombstone_limit: int = 100


class TriggerIn(BaseModel):
    idempotency_key: str | None = None


class RetentionIn(BaseModel):
    retain_last_runs: int | None = None
    retain_seconds: float | None = None
    tombstone_limit: int | None = None


def _jsonable(obj):
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
    RejectReason.UNKNOWN_TASK: 422,
    RejectReason.DEPENDENCY_CYCLE: 422,
    RejectReason.INVALID_POLICY: 422,
    RejectReason.DUPLICATE_WORKFLOW: 409,
    RejectReason.DUPLICATE_RUN: 409,
    RejectReason.ACTIVE_RUN_CONFLICT: 409,
    RejectReason.QUEUE_FULL: 429,
    RejectReason.RUN_NOT_ACTIVE: 409,
    RejectReason.RUN_PURGED: 410,  # 资源曾存在但已被保留策略清理
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
        body = {"error": exc.reason.value, "detail": exc.detail}
        if exc.extra:
            body.update(exc.extra)  # 机器可读补充（如 active_run_id）
        return JSONResponse(
            status_code=_STATUS.get(exc.reason, 400), content=body)

    @app.post("/workflows", status_code=201)
    def register(body: WorkflowIn, x_owner_id: str | None = Header(None)):
        owner = owner_of(x_owner_id)
        spec = WorkflowSpec(
            name=body.name, owner=owner,
            tasks=[TaskSpec(**t.model_dump()) for t in body.tasks],
            schedule_interval_seconds=body.schedule_interval_seconds,
            active_run_policy=body.active_run_policy,
            retain_last_runs=body.retain_last_runs,
            retain_seconds=body.retain_seconds,
            tombstone_limit=body.tombstone_limit)
        wf = orchestrator.register_workflow(spec)
        return _jsonable(wf)

    @app.get("/workflows")
    def list_workflows(x_owner_id: str | None = Header(None)):
        owner = owner_of(x_owner_id)
        return [_jsonable(w) for w in
                orchestrator.storage.list_workflows(owner)]

    @app.put("/workflows/{name}/retention")
    def update_retention(name: str, body: RetentionIn,
                          x_owner_id: str | None = Header(None)):
        owner = owner_of(x_owner_id)
        wf = orchestrator.update_retention_policy(
            owner, name, body.retain_last_runs, body.retain_seconds,
            body.tombstone_limit)
        return _jsonable(wf)

    @app.post("/workflows/{name}/runs")
    def trigger(name: str, body: TriggerIn | None = None,
                x_owner_id: str | None = Header(None)):
        owner = owner_of(x_owner_id)
        key = body.idempotency_key if body else None
        outcome = orchestrator.trigger_detailed(owner, name, key)
        # 复用已有的运行返回 200；新建返回 201，便于调用方区分
        from fastapi.responses import JSONResponse
        payload = _jsonable(outcome.run)
        payload["trigger_outcome"] = outcome.outcome
        payload["convergence_reason"] = outcome.reason
        payload["idempotency_hit"] = outcome.idempotency_hit
        status = 201 if outcome.outcome == "created" else 200
        return JSONResponse(status_code=status, content=payload)

    @app.get("/runs")
    def list_runs(x_owner_id: str | None = Header(None)):
        owner = owner_of(x_owner_id)
        return [_jsonable(r) for r in orchestrator.storage.list_runs(owner)]

    @app.get("/runs/purged")
    def list_purged(x_owner_id: str | None = Header(None)):
        """本调用方被保留策略清理的运行墓碑（区分于从未存在）。"""
        owner = owner_of(x_owner_id)
        return orchestrator.list_tombstones(owner)

    @app.get("/audit")
    def list_audit(limit: int = 200, x_owner_id: str | None = Header(None)):
        """本调用方的收敛/拒绝/清理可解释记录（有界，按时间倒序）。"""
        owner = owner_of(x_owner_id)
        return orchestrator.list_audit(owner, min(max(limit, 1), 1000))

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
