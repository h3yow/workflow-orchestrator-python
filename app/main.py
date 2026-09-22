"""FastAPI 应用工厂与 uvicorn 入口。"""
from __future__ import annotations

import logging
import os

from fastapi import FastAPI

from app.api import init_engine, router
from app.builtins import register_builtins
from app.engine import Engine


def create_app(db_path: str | None = None,
               max_workers: int | None = None,
               poll_interval: float | None = None,
               start_scheduler: bool = True) -> FastAPI:
    logging.basicConfig(
        level=os.environ.get("WORKFLOW_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    register_builtins()
    application = FastAPI(title="workflow-orchestrator", version="0.1.0")
    engine = Engine(
        db_path=db_path or os.environ.get("WORKFLOW_DB", "data/workflow.db"),
        max_workers=max_workers or int(os.environ.get("WORKFLOW_MAX_WORKERS", "4")),
        poll_interval=(poll_interval if poll_interval is not None
                       else float(os.environ.get("WORKFLOW_POLL_INTERVAL", "0.05"))),
        retain_terminal_runs=int(os.environ.get("WORKFLOW_RETAIN_RUNS", "100")),
        start_scheduler=start_scheduler,
    )
    init_engine(engine)
    application.include_router(router, prefix="/api/v1")
    application.state.engine = engine

    @application.get("/health")
    def health():
        return {"status": "ok"}

    @application.on_event("shutdown")
    def _shutdown():
        engine.shutdown()

    return application


app = create_app()
