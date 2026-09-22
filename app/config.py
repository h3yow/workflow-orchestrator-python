"""运行配置。"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class Settings:
    db_path: str = "orchestrator.db"
    max_concurrent_tasks: int = 4        # 全局任务并发上限
    max_pending_runs: int = 100          # 运行积压上限（PENDING/RUNNING）
    duplicate_run_policy: str = "dedup"  # 同幂等键重复触发: "dedup" 返回已有 | "reject" 拒绝
    tick_interval_seconds: float = 0.05  # 调度循环间隔
    schedule_enabled: bool = True        # 是否启用定时触发

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            db_path=os.environ.get("ORCH_DB_PATH", "orchestrator.db"),
            max_concurrent_tasks=int(os.environ.get("ORCH_MAX_CONCURRENCY", "4")),
            max_pending_runs=int(os.environ.get("ORCH_MAX_PENDING_RUNS", "100")),
            duplicate_run_policy=os.environ.get("ORCH_DUP_POLICY", "dedup"),
        )
