"""工作流定义校验：重复任务、缺失依赖、依赖成环。"""
from __future__ import annotations

from app.errors import ErrorCode, WorkflowError
from app.models import WorkflowDef


def validate_definition(definition: WorkflowDef) -> None:
    """校验工作流定义，非法时抛 WorkflowError（可区分错误码）。

    - DUPLICATE_TASK: 任务 key 重复
    - INVALID_DEFINITION: 名称为空 / 重试或超时参数非法
    - MISSING_DEPENDENCY: depends_on 引用了不存在的任务
    - CYCLIC_DEPENDENCY: 依赖关系成环（Kahn 拓扑排序后仍有剩余节点）
    """
    if not definition.name or not definition.name.strip():
        raise WorkflowError(ErrorCode.INVALID_DEFINITION, "workflow name 不能为空")
    if not definition.tasks:
        raise WorkflowError(ErrorCode.INVALID_DEFINITION, "工作流至少需要一个任务")

    seen: set[str] = set()
    for task in definition.tasks:
        if not task.key or not task.key.strip():
            raise WorkflowError(ErrorCode.INVALID_DEFINITION, "任务 key 不能为空")
        if task.key in seen:
            raise WorkflowError(
                ErrorCode.DUPLICATE_TASK,
                f"任务 key 重复: {task.key}",
            )
        seen.add(task.key)
        retry = task.retry
        if retry is not None:
            if retry.max_retries < 0:
                raise WorkflowError(
                    ErrorCode.INVALID_DEFINITION,
                    f"任务 {task.key} 的 max_retries 不能为负",
                )
            if retry.backoff_seconds < 0 or retry.backoff_multiplier < 1:
                raise WorkflowError(
                    ErrorCode.INVALID_DEFINITION,
                    f"任务 {task.key} 的退避参数非法",
                )
        if task.timeout_seconds is not None and task.timeout_seconds <= 0:
            raise WorkflowError(
                ErrorCode.INVALID_DEFINITION,
                f"任务 {task.key} 的 timeout_seconds 必须为正数",
            )

    # 引用了不存在的依赖（自引用也先在这里被识别为 MISSING? 不——
    # 自引用任务本身存在，但会构成环；区分处理）
    for task in definition.tasks:
        for dep in task.depends_on:
            if dep not in seen:
                raise WorkflowError(
                    ErrorCode.MISSING_DEPENDENCY,
                    f"任务 {task.key} 依赖了不存在的任务: {dep}",
                )

    # 成环检测：Kahn 拓扑排序
    indegree: dict[str, int] = {t.key: 0 for t in definition.tasks}
    adjacency: dict[str, list[str]] = {t.key: [] for t in definition.tasks}
    for task in definition.tasks:
        unique_deps = set(task.depends_on)
        indegree[task.key] = len(unique_deps)
        for dep in unique_deps:
            adjacency[dep].append(task.key)

    queue = [key for key, deg in indegree.items() if deg == 0]
    visited = 0
    while queue:
        node = queue.pop()
        visited += 1
        for nxt in adjacency[node]:
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                queue.append(nxt)

    if visited != len(definition.tasks):
        cyclic = sorted(k for k, d in indegree.items() if d > 0)
        raise WorkflowError(
            ErrorCode.CYCLIC_DEPENDENCY,
            f"依赖关系成环，涉及任务: {', '.join(cyclic)}",
        )
