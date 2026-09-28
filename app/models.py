"""核心数据模型与状态机定义。"""
from __future__ import annotations

import enum
import time
import uuid
from dataclasses import dataclass, field


def new_id() -> str:
    return uuid.uuid4().hex


def now() -> float:
    return time.time()


class RunStatus(str, enum.Enum):
    PENDING = "PENDING"        # 已入队，等待调度
    RUNNING = "RUNNING"        # 至少一个任务在执行/可执行
    SUCCEEDED = "SUCCEEDED"    # 全部任务成功（终态）
    FAILED = "FAILED"          # 存在任务失败且不可重试（终态）
    CANCELLED = "CANCELLED"    # 被取消（终态）


class TaskStatus(str, enum.Enum):
    PENDING = "PENDING"        # 等待前置任务
    READY = "READY"            # 前置已全部成功，等待执行槽位
    RUNNING = "RUNNING"        # 正在执行
    SUCCEEDED = "SUCCEEDED"    # 终态
    FAILED = "FAILED"          # 终态：重试耗尽
    TIMED_OUT = "TIMED_OUT"    # 终态：执行超时
    SKIPPED = "SKIPPED"        # 终态：前置失败，未被执行
    CANCELLED = "CANCELLED"    # 终态


TERMINAL_RUN_STATES = {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED}
TERMINAL_TASK_STATES = {
    TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.TIMED_OUT,
    TaskStatus.SKIPPED, TaskStatus.CANCELLED,
}

# 合法状态转移表
RUN_TRANSITIONS: dict[RunStatus, set[RunStatus]] = {
    RunStatus.PENDING: {RunStatus.RUNNING, RunStatus.CANCELLED},
    RunStatus.RUNNING: {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED},
    RunStatus.SUCCEEDED: set(),
    RunStatus.FAILED: set(),
    RunStatus.CANCELLED: set(),
}

TASK_TRANSITIONS: dict[TaskStatus, set[TaskStatus]] = {
    TaskStatus.PENDING: {TaskStatus.READY, TaskStatus.SKIPPED, TaskStatus.CANCELLED},
    TaskStatus.READY: {TaskStatus.RUNNING, TaskStatus.CANCELLED, TaskStatus.SKIPPED},
    TaskStatus.RUNNING: {
        TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.TIMED_OUT,
        TaskStatus.READY,  # 重试/重启恢复时回到 READY
        TaskStatus.CANCELLED,
    },
    TaskStatus.SUCCEEDED: set(),
    TaskStatus.FAILED: set(),
    TaskStatus.TIMED_OUT: set(),
    TaskStatus.SKIPPED: set(),
    TaskStatus.CANCELLED: set(),
}


class InvalidTransition(Exception):
    """非法状态转移。"""

    def __init__(self, entity: str, current: enum.Enum, target: enum.Enum):
        self.entity = entity
        self.current = current
        self.target = target
        super().__init__(
            f"illegal {entity} transition: {current.value} -> {target.value}"
        )


def check_transition(table, current, target, entity: str):
    if target not in table.get(current, set()):
        raise InvalidTransition(entity, current, target)


class RejectReason(str, enum.Enum):
    UNKNOWN_TASK = "UNKNOWN_TASK"                # 依赖引用了不存在的任务
    DEPENDENCY_CYCLE = "DEPENDENCY_CYCLE"        # 依赖成环
    DUPLICATE_WORKFLOW = "DUPLICATE_WORKFLOW"    # 同名工作流已存在
    DUPLICATE_RUN = "DUPLICATE_RUN"              # 幂等键冲突且策略为拒绝
    QUEUE_FULL = "QUEUE_FULL"                    # 运行积压超过上限
    WORKFLOW_NOT_FOUND = "WORKFLOW_NOT_FOUND"
    RUN_NOT_FOUND = "RUN_NOT_FOUND"
    FORBIDDEN = "FORBIDDEN"                      # 越权访问
    RUN_NOT_ACTIVE = "RUN_NOT_ACTIVE"            # 运行已结束，无法取消等操作
    ACTIVE_RUN_CONFLICT = "ACTIVE_RUN_CONFLICT"  # 已有活跃运行且策略为 reject
    RUN_PURGED = "RUN_PURGED"                    # 运行曾存在，但已被保留策略清理
    INVALID_POLICY = "INVALID_POLICY"            # 并发/保留策略参数非法


# 并发收敛策略：同一工作流同一时刻的活跃运行数
ACTIVE_POLICY_ALLOW = "allow"    # 不收敛（历史默认语义，受积压上限约束）
ACTIVE_POLICY_REUSE = "reuse"    # 复用已有活跃运行
ACTIVE_POLICY_REJECT = "reject"  # 拒绝后来的触发
ACTIVE_POLICIES = {ACTIVE_POLICY_ALLOW, ACTIVE_POLICY_REUSE,
                   ACTIVE_POLICY_REJECT}


class Rejected(Exception):
    """请求被拒绝，携带可区分分的原因。"""

    def __init__(self, reason: RejectReason, detail: str = "",
                 extra: dict | None = None):
        self.reason = reason
        self.detail = detail
        self.extra = extra or {}
        super().__init__(f"{reason.value}: {detail}")


@dataclass
class TaskSpec:
    name: str
    handler: str                      # 执行器注册表中的处理器名
    depends_on: list[str] = field(default_factory=list)
    max_attempts: int = 1             # 总尝试次数（含首次），>=1
    backoff_seconds: float = 0.0      # 退避基数，第 n 次重试等待 backoff * 2^(n-1)
    timeout_seconds: float | None = None  # 单次执行超时


@dataclass
class WorkflowSpec:
    name: str
    owner: str
    tasks: list[TaskSpec]
    schedule_interval_seconds: float | None = None  # 定时触发间隔，None 表示仅手动
    # 并发收敛策略：同一时刻是否允许多条活跃运行
    # "allow"(不收敛，历史默认) | "reuse"(复用活跃运行) | "reject"(拒绝)
    active_run_policy: str = "allow"
    # 历史保留策略（仅针对已结束运行）；None 表示该维度不限制
    retain_last_runs: int | None = None   # 仅保留最近 N 条已结束运行
    retain_seconds: float | None = None  # 仅保留最近 N 秒内结束的运行
    # 因保留策略被清理的运行墓碑上限（自身有界，防止清理记录无限增长）
    tombstone_limit: int = 100


@dataclass
class Workflow:
    id: str
    name: str
    owner: str
    spec: WorkflowSpec
    created_at: float


@dataclass
class Run:
    id: str
    workflow_id: str
    owner: str
    status: RunStatus
    trigger: str               # "manual" | "schedule"
    idempotency_key: str | None
    created_at: float
    updated_at: float


@dataclass
class TaskRun:
    id: str
    run_id: str
    name: str
    status: TaskStatus
    attempt: int               # 已执行的尝试次数
    max_attempts: int
    next_retry_at: float | None  # 退避到期时间
    error: str | None
    started_at: float | None
    finished_at: float | None


# 触发收敛结果
TRIGGER_CREATED = "created"    # 新运行已创建
TRIGGER_REUSED = "reused"    # 复用了已有的活跃运行（无键或幂等键命中）


@dataclass
class TriggerOutcome:
    """触发请求的可解释结果：新建 / 复用，复用携带被复用的运行与原因。"""
    run: Run
    outcome: str            # TRIGGER_CREATED | TRIGGER_REUSED
    reason: str             # 人类可读的收敛/去重依据
    idempotency_hit: bool = False  # 是否因幂等键命中而复用
