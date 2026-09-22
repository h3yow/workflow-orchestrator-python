import logging
import sys

import pytest

from app.config import Settings
from app.executor import Executor, HandlerRegistry
from app.scheduler import Orchestrator
from app.storage import Storage


@pytest.fixture(autouse=True)
def _log_to_stdout(caplog):
    """单测日志输出到 stdout，便于查看调度决策依据。"""
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    logging.getLogger("orchestrator").addHandler(handler)
    with caplog.at_level(logging.INFO, logger="orchestrator"):
        yield


def explain(msg: str) -> None:
    """在单测日志中打印输入与判定依据。"""
    print(f"\n[TEST-EXPLAIN] {msg}")


@pytest.fixture
def make_orch(tmp_path):
    """构建使用临时数据库的 Orchestrator，测试后清理。"""
    created = []

    def factory(db_name="test.db", registry=None, **overrides):
        settings = Settings(db_path=str(tmp_path / db_name),
                            tick_interval_seconds=0.01, **overrides)
        storage = Storage(settings.db_path)
        reg = registry or HandlerRegistry()
        orch = Orchestrator(storage, Executor(settings.max_concurrent_tasks),
                            settings, reg)
        created.append(orch)
        return orch

    yield factory
    for o in created:
        o.stop()
