"""可区分的错误码与异常类型。"""
from __future__ import annotations


class ErrorCode:
    CYCLIC_DEPENDENCY = "CYCLIC_DEPENDENCY"
    MISSING_DEPENDENCY = "MISSING_DEPENDENCY"
    DUPLICATE_TASK = "DUPLICATE_TASK"
    WORKFLOW_NOT_FOUND = "WORKFLOW_NOT_FOUND"
    RUN_NOT_FOUND = "RUN_NOT_FOUND"
    TASK_NOT_FOUND = "TASK_NOT_FOUND"
    ALREADY_ACTIVE = "ALREADY_ACTIVE"
    QUEUE_FULL = "QUEUE_FULL"
    INVALID_TRANSITION = "INVALID_TRANSITION"
    INVALID_DEFINITION = "INVALID_DEFINITION"
    UNAUTHORIZED = "UNAUTHORIZED"
    FORBIDDEN = "FORBIDDEN"
    SCHEDULE_NOT_FOUND = "SCHEDULE_NOT_FOUND"
    DUPLICATE_SCHEDULE = "DUPLICATE_SCHEDULE"


class WorkflowError(Exception):
    """带可区分错误码的业务异常。"""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message
