from __future__ import annotations

from typing import Dict, List, Optional

from app.services.storage import SceneRepository


def execution_target_count(task: Dict[str, object]) -> int:
    return len(task.get("browsers") or []) * len(task.get("viewports") or [])


def batch_task_details(
    repo: SceneRepository,
    batch: Dict[str, object],
) -> List[Dict[str, object]]:
    details: List[Dict[str, object]] = []
    for task_id in batch.get("task_ids") or []:
        task = repo.get_task(str(task_id))
        if not task:
            continue
        details.append(
            {
                "id": str(task["id"]),
                "name": str(task.get("name") or task["id"]),
                "execution_count": execution_target_count(task),
            }
        )
    return details


def validate_task_subset(
    repo: SceneRepository,
    batch: Dict[str, object],
    requested_task_ids: Optional[List[str]],
) -> Optional[List[str]]:
    """Validate and order a run subset according to the batch's task order."""

    if requested_task_ids is None:
        return None
    requested = {str(task_id) for task_id in requested_task_ids if str(task_id)}
    if not requested:
        raise ValueError("Select at least one task for this run.")
    batch_ids = [str(task_id) for task_id in batch.get("task_ids") or []]
    unknown = sorted(requested - set(batch_ids))
    if unknown:
        raise ValueError("Selected tasks are not part of this batch: " + ", ".join(unknown))
    ordered = [task_id for task_id in batch_ids if task_id in requested]
    missing = [task_id for task_id in ordered if not repo.get_task(task_id)]
    if missing:
        raise ValueError("Selected tasks no longer exist: " + ", ".join(missing))
    return ordered
