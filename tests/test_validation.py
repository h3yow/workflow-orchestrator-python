"""依赖成环、缺失依赖、重复任务必须被拒绝且原因可区分。"""
from __future__ import annotations

import pytest

from app.errors import WorkflowError
from app.models import TaskDef, WorkflowDef
from app.validation import validate_definition


def _wf(tasks, name="w"):
    return WorkflowDef(name=name, version=1, tasks=tuple(tasks))


def test_accepts_dag():
    definition = _wf([
        TaskDef(key="a", handler="h"),
        TaskDef(key="b", handler="h", depends_on=("a",)),
        TaskDef(key="c", handler="h", depends_on=("a", "b")),
    ])
    print("[输入] 合法 DAG a->b->c,a->c；判定依据=拓扑排序通过")
    validate_definition(definition)  # 不抛异常即通过


def test_rejects_cyclic_dependency():
    definition = _wf([
        TaskDef(key="a", handler="h", depends_on=("b",)),
        TaskDef(key="b", handler="h", depends_on=("a",)),
    ])
    print("[输入] a<->b 互相依赖；判定依据=Kahn 排序后仍有节点，必须 CYCLIC_DEPENDENCY")
    with pytest.raises(WorkflowError) as exc:
        validate_definition(definition)
    assert exc.value.code == "CYCLIC_DEPENDENCY"
    assert "a" in exc.value.message and "b" in exc.value.message


def test_rejects_self_dependency():
    definition = _wf([TaskDef(key="a", handler="h", depends_on=("a",))])
    print("[输入] a 依赖自身；判定依据=自环也是环，必须 CYCLIC_DEPENDENCY")
    with pytest.raises(WorkflowError) as exc:
        validate_definition(definition)
    assert exc.value.code == "CYCLIC_DEPENDENCY"


def test_rejects_missing_dependency():
    definition = _wf([TaskDef(key="a", handler="h", depends_on=("ghost",))])
    print("[输入] a 依赖不存在的 ghost；判定依据=引用集合外任务，MISSING_DEPENDENCY")
    with pytest.raises(WorkflowError) as exc:
        validate_definition(definition)
    assert exc.value.code == "MISSING_DEPENDENCY"
    assert "ghost" in exc.value.message


def test_rejects_duplicate_task():
    definition = _wf([
        TaskDef(key="a", handler="h"),
        TaskDef(key="a", handler="h2"),
    ])
    print("[输入] 两个同名任务 a；判定依据=key 重复，DUPLICATE_TASK")
    with pytest.raises(WorkflowError) as exc:
        validate_definition(definition)
    assert exc.value.code == "DUPLICATE_TASK"


def test_error_codes_are_distinguishable():
    print("[输入] 环 vs 缺失依赖；判定依据=两者错误码必须不同")
    cyclic = _wf([
        TaskDef(key="a", handler="h", depends_on=("b",)),
        TaskDef(key="b", handler="h", depends_on=("a",)),
    ])
    missing = _wf([TaskDef(key="a", handler="h", depends_on=("x",))])
    codes = []
    for d in (cyclic, missing):
        with pytest.raises(WorkflowError) as exc:
            validate_definition(d)
        codes.append(exc.value.code)
    assert codes == ["CYCLIC_DEPENDENCY", "MISSING_DEPENDENCY"]
