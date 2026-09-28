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
    # 工作流未单独配置时的默认并发收敛策略
    default_active_run_policy: str = "allow"
    # 工作流未单独配置时的默认历史保留策略（None 表示不限制）
    default_retain_last_runs: int | None = None
    default_retain_seconds: float | None = None
    default_tombstone_limit: int = 100   # 每工作流默认墓碑保留条数
    retention_interval_seconds: float = 1.0  # 后台保留清理检查周期
    retention_batch_size: int = 200      # 单次清理最多删除的运行条数（分批，避免长时间卡住触发/查询）
    audit_max_entries: int = 5000       # 审计日志总条数上限（有界）
    audit_max_age_seconds: float = 7 * 24 * 3600  # 审计日志保留时长
    purge_interval_seconds: float = 60.0  # 审计日志自身清理周期

    @classmethod
    def from_env(cls) -> "Settings":
        def _int_env(name: str) -> int | None:
            raw = os.environ.get(name)
            return int(raw) if raw not in (None, "") else None

        def _float_env(name: str) -> float | None:
            raw = os.environ.get(name)
            return float(raw) if raw not in (None, "") else None

        return cls(
            db_path=os.environ.get("ORCH_DB_PATH", "orchestrator.db"),
            max_concurrent_tasks=int(os.environ.get("ORCH_MAX_CONCURRENCY", "4")),
            max_pending_runs=int(os.environ.get("ORCH_MAX_PENDING_RUNS", "100")),
            duplicate_run_policy=os.environ.get("ORCH_DUP_POLICY", "dedup"),
            default_active_run_policy=os.environ.get(
                "ORCH_DEFAULT_ACTIVE_POLICY", "allow"),
            default_retain_last_runs=_int_env("ORCH_DEFAULT_RETAIN_LAST_RUNS"),
            default_retain_seconds=_float_env("ORCH_DEFAULT_RETAIN_SECONDS"),
        )
