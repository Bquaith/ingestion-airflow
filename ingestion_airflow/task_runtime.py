from __future__ import annotations

from typing import Any, Iterable


_MISSING_XCOM = object()


def get_missing_return_value_tasks(task_instance: Any, task_ids: Iterable[str]) -> list[str]:
    """Return task ids that do not have a return-value XCom for the current DagRun."""
    missing_task_ids: list[str] = []
    for task_id in task_ids:
        value = task_instance.xcom_pull(
            task_ids=task_id,
            key="return_value",
            default=_MISSING_XCOM,
        )
        if value is _MISSING_XCOM:
            missing_task_ids.append(task_id)
    return missing_task_ids
