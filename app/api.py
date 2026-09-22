"""FastAPI 路由：定义/列表、触发、状态与历史查询、定时计划管理。

所有端点都经过 _require_tenant 解析调用方身份（X-API-Key），并把 tenant_id
传给引擎——租户之间的工作流、运行、任务与决策历史完全隔离；
跨租户访问他人 run_id 返回 404（不泄漏存在性），无效 key 返回 401。
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException

from app.auth import resolve_tenant
from app.errors import ErrorCode, WorkflowError
from app.models import ConcurrencyPolicy
from app.schemas import ScheduleIn, TriggerIn, WorkflowSpecIn

logger = logging.getLogger("workflow.api")
router = APIRouter()

# 由 create_app 注入
_engine = None


def init_engine(engine) -> None:
    global _engine
    _engine = engine


def _engine_dep():
    assert _engine is not None, "engine 未初始化"
    return _engine


def _require_tenant(x_api_key: str | None = Header(default=None)) -> str:
    tenant = resolve_tenant(x_api_key)
    if tenant is None:
        logger.info("拒绝请求：缺失或无效的 X-API-Key")
        raise HTTPException(status_code=401,
                            detail={"code": ErrorCode.UNAUTHORIZED,
                                    "message": "缺失或无效的 X-API-Key"})
    return tenant

# -------------------------------------------------------------- 错误码映射
_STATUS = {
    ErrorCode.CYCLIC_DEPENDENCY: 422,
    ErrorCode.MISSING_DEPENDENCY: 422,
    ErrorCode.DUPLICATE_TASK: 422,
    ErrorCode.INVALID_DEFINITION: 422,
    ErrorCode.INVALID_TRANSITION: 409,
    ErrorCode.ALREADY_ACTIVE: 409,
    ErrorCode.QUEUE_FULL: 429,
    ErrorCode.WORKFLOW_NOT_FOUND: 404,
    ErrorCode.RUN_NOT_FOUND: 404,
    ErrorCode.TASK_NOT_FOUND: 404,
    ErrorCode.SCHEDULE_NOT_FOUND: 404,
    ErrorCode.DUPLICATE_SCHEDULE: 409,
    ErrorCode.FORBIDDEN: 403,
}


def _http_error(func):
    def wrap(*a: Any, **kw: Any):
        try:
            return func(*a, **kw)
        except WorkflowError as exc:
            status = _STATUS.get(exc.code, 400)
            logger.info("请求被拒绝 code=%s message=%s", exc.code, exc.message)
            raise HTTPException(status_code=status,
                                detail={"code": exc.code, "message": exc.message})
    return wrap


# ----------------------------------------------------------------- 工作流
@router.post("/workflows")
def register_workflow(spec: WorkflowSpecIn,
                      tenant: str = Depends(_require_tenant),
                      engine=Depends(_engine_dep)):
    definition = _to_definition(spec)
    _http_error(engine.register_workflow)(tenant, definition)
    return {"name": spec.name, "version": spec.version, "tenant_id": tenant}


@router.get("/workflows")
def list_workflows(tenant: str = Depends(_require_tenant),
                   engine=Depends(_engine_dep)):
    items = engine.list_workflows(tenant)
    return {"workflows": [_def_to_dict(d) for d in items]}


@router.get("/workflows/{name}/definition")
def get_workflow(name: str, tenant: str = Depends(_require_tenant),
                 engine=Depends(_engine_dep)):
    return _def_to_dict(_http_error(engine.get_workflow)(tenant, name))


# ------------------------------------------------------------------- 运行
@router.post("/workflows/{name}/trigger")
def trigger_workflow(name: str, body: TriggerIn,
                     tenant: str = Depends(_require_tenant),
                     engine=Depends(_engine_dep)):
    run_id, deduped = _http_error(engine.trigger_run)(tenant, name, body.trigger)
    return {"run_id": run_id, "deduped": deduped}


@router.get("/runs")
def list_runs(workflow_name: str | None = None,
              tenant: str = Depends(_require_tenant),
              engine=Depends(_engine_dep)):
    runs = _http_error(engine.list_runs)(tenant, workflow_name)
    return {"runs": [_run_to_dict(r) for r in runs]}


@router.get("/runs/{run_id}")
def get_run(run_id: str, tenant: str = Depends(_require_tenant),
            engine=Depends(_engine_dep)):
    return _run_to_dict(_http_error(engine.get_run)(tenant, run_id))


@router.get("/runs/{run_id}/tasks")
def get_run_tasks(run_id: str, tenant: str = Depends(_require_tenant),
                  engine=Depends(_engine_dep)):
    tasks = _http_error(engine.list_tasks)(tenant, run_id)
    return {"tasks": [_task_to_dict(t) for t in tasks]}


@router.get("/runs/{run_id}/history")
def get_run_history(run_id: str, tenant: str = Depends(_require_tenant),
                    engine=Depends(_engine_dep)):
    logs = _http_error(engine.list_history)(tenant, run_id)
    return {"history": [log.__dict__ for log in logs]}


# ----------------------------------------------------------------- 定时计划
@router.post("/schedules")
def create_schedule(body: ScheduleIn,
                    tenant: str = Depends(_require_tenant),
                    engine=Depends(_engine_dep)):
    sid = _http_error(engine.create_schedule)(
        tenant, body.name, body.workflow_name, body.interval_seconds)
    return {"id": sid, "name": body.name, "tenant_id": tenant}


@router.get("/schedules")
def list_schedules(tenant: str = Depends(_require_tenant),
                   engine=Depends(_engine_dep)):
    rows = engine.list_schedules(tenant)
    return {"schedules": [dict(row) for row in rows]}


@router.delete("/schedules/{name}")
def delete_schedule(name: str, tenant: str = Depends(_require_tenant),
                    engine=Depends(_engine_dep)):
    _http_error(engine.delete_schedule)(tenant, name)
    return {"deleted": name}


# --------------------------------------------------------------- 序列化辅助
def _to_definition(spec: WorkflowSpecIn):
    from app.models import RetryPolicy, TaskDef, WorkflowDef

    try:
        policy = ConcurrencyPolicy(spec.concurrency)
    except ValueError:
        raise WorkflowError(
            ErrorCode.INVALID_DEFINITION,
            f"未知并发策略: {spec.concurrency}（可选 REJECT/SKIP/QUEUE）",
        )
    tasks = tuple(
        TaskDef(
            key=t.key, handler=t.handler, depends_on=tuple(t.depends_on),
            params=t.params, timeout_seconds=t.timeout_seconds,
            retry=RetryPolicy(
                max_retries=t.retry.max_retries,
                backoff_seconds=t.retry.backoff_seconds,
                backoff_multiplier=t.retry.backoff_multiplier,
            ) if t.retry else None,
        )
        for t in spec.tasks
    )
    return WorkflowDef(
        name=spec.name, version=spec.version, tasks=tasks,
        concurrency=policy, max_queued_runs=spec.max_queued_runs,
    )


def _def_to_dict(d) -> dict[str, Any]:
    return {
        "name": d.name, "version": d.version,
        "concurrency": d.concurrency.value,
        "max_queued_runs": d.max_queued_runs,
        "tasks": [
            {
                "key": t.key, "handler": t.handler,
                "depends_on": list(t.depends_on), "params": t.params,
                "timeout_seconds": t.timeout_seconds,
                "retry": (
                    {"max_retries": t.retry.max_retries,
                     "backoff_seconds": t.retry.backoff_seconds,
                     "backoff_multiplier": t.retry.backoff_multiplier}
                    if t.retry else None
                ),
            }
            for t in d.tasks
        ],
    }


def _run_to_dict(r) -> dict[str, Any]:
    return {
        "run_id": r.id, "tenant_id": r.tenant_id,
        "workflow_name": r.workflow_name, "version": r.version,
        "state": r.state.value, "trigger": r.trigger,
        "queued_at": r.queued_at, "started_at": r.started_at,
        "finished_at": r.finished_at,
        "error": (None if r.error_code is None
                  else {"code": r.error_code, "message": r.error_message}),
    }


def _task_to_dict(t) -> dict[str, Any]:
    return {
        "task_id": t.id, "task_key": t.task_key, "state": t.state.value,
        "attempt_count": t.attempt_count,
        "scheduled_at": t.scheduled_at, "started_at": t.started_at,
        "finished_at": t.finished_at,
        "error": (None if t.error_code is None
                  else {"code": t.error_code, "message": t.error_message}),
        "attempts": [a.__dict__ for a in t.attempts],
    }
