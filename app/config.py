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
    retention_enabled: bool = True       # 是否启用后台历史保留清理
    cleanup_interval_seconds: float = 30.0   # 后台清理尝试间隔
    cleanup_batch_runs: int = 100        # 单次清理最多处理的运行数（控制锁持有时间）
    tombstone_ttl_seconds: float = 7 * 24 * 3600  # 清理墓碑保留期：期内可区分"被清理"
    audit_ttl_seconds: float = 7 * 24 * 3600      # 收敛/清理审计记录保留期
    audit_max_rows: int = 5000           # 审计记录硬上界（防止表本身无限膨胀）

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            db_path=os.environ.get("ORCH_DB_PATH", "orchestrator.db"),
            max_concurrent_tasks=int(os.environ.get("ORCH_MAX_CONCURRENCY", "4")),
            max_pending_runs=int(os.environ.get("ORCH_MAX_PENDING_RUNS", "100")),
            duplicate_run_policy=os.environ.get("ORCH_DUP_POLICY", "dedup"),
            tick_interval_seconds=float(
                os.environ.get("ORCH_TICK_INTERVAL", "0.05")),
            cleanup_interval_seconds=float(
                os.environ.get("ORCH_CLEANUP_INTERVAL", "30")),
            tombstone_ttl_seconds=float(
                os.environ.get("ORCH_TOMBSTONE_TTL", str(7 * 24 * 3600))),
            audit_ttl_seconds=float(
                os.environ.get("ORCH_AUDIT_TTL", str(7 * 24 * 3600))),
            audit_max_rows=int(os.environ.get("ORCH_AUDIT_MAX_ROWS", "5000")),
        )
