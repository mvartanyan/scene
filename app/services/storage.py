from __future__ import annotations

import json
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from fastapi import Depends

from app.constants import DEFAULT_BROWSERS, DEFAULT_VIEWPORTS
from app.services.artifacts import get_artifact_store

STATE_VERSION = 1


def _utcnow() -> str:
    """Return timezone-aware ISO timestamp."""
    return datetime.now(tz=timezone.utc).isoformat()


def _default_state() -> Dict[str, Any]:
    return {
        "version": STATE_VERSION,
        "projects": {},
        "pages": {},
        "tasks": {},
        "batches": {},
        "runs": {},
        "executions": {},
        "baselines": {},
        "config": {
            "browsers": DEFAULT_BROWSERS.copy(),
            "viewports": DEFAULT_VIEWPORTS.copy(),
            "display_timezone": "utc",
            "run_timeout_seconds": 600,
            "max_concurrent_executions": 4,
            "scene_host_url": "http://host.docker.internal:8000",
            "capture_post_wait_ms": 7000,
        },
    }


def _sanitize_script(value: Optional[Any]) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped or stripped.lower() == "none":
            return None
        return stripped
    return None


def _sanitize_actions(value: Optional[Any]) -> List[Dict[str, Any]]:
    if not value:
        return []
    if isinstance(value, list):
        cleaned: List[Dict[str, Any]] = []
        for item in value:
            if isinstance(item, dict) and item.get("type"):
                cleaned.append(dict(item))
        return cleaned
    return []


class LocalDynamoStorage:
    """Very small DynamoDB-like persistence layer backed by a JSON file.

    The goal is to mimic the partitioned collections we expect from Dynamo without
    introducing AWS dependencies. Each top-level collection stores items keyed
    by their primary identifier. All writes are synchronised via an internal lock.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.Lock()
        self._state = self._load()

    def _load(self) -> Dict[str, Any]:
        if not self._path.exists():
            return _default_state()
        with self._path.open("r", encoding="utf-8") as handle:
            state = json.load(handle)
            state.setdefault("baselines", {})
            state.setdefault("executions", {})
            if "config" not in state:
                state["config"] = {
                    "browsers": DEFAULT_BROWSERS.copy(),
                    "viewports": DEFAULT_VIEWPORTS.copy(),
                    "display_timezone": "utc",
                    "run_timeout_seconds": 600,
                    "max_concurrent_executions": 4,
                    "scene_host_url": "http://host.docker.internal:8000",
                    "capture_post_wait_ms": 7000,
                }
            else:
                state_config = state["config"]
                state_config.setdefault("browsers", DEFAULT_BROWSERS.copy())
                state_config.setdefault("viewports", DEFAULT_VIEWPORTS.copy())
                state_config.setdefault("display_timezone", "utc")
                state_config.setdefault("run_timeout_seconds", 600)
                state_config.setdefault("max_concurrent_executions", 4)
                state_config.setdefault("scene_host_url", "http://host.docker.internal:8000")
                state_config.setdefault("capture_post_wait_ms", 7000)
            for batch in state.get("batches", {}).values():
                batch.setdefault("run_diff_threshold", None)
                batch.setdefault("execution_diff_threshold", None)
            for run in state.get("runs", {}).values():
                summary = run.setdefault("summary", {})
                summary.setdefault("executions_total", 0)
                summary.setdefault("executions_finished", 0)
                summary.setdefault("executions_failed", 0)
                summary.setdefault("executions_cancelled", 0)
                summary.setdefault("diff_average", 0.0)
                summary.setdefault("diff_maximum", 0.0)
                summary.setdefault("diff_samples", 0)
            return state

    def _persist(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("w", encoding="utf-8") as handle:
            json.dump(self._state, handle, indent=2, sort_keys=True)

    def _collection(self, name: str) -> Dict[str, Any]:
        return self._state.setdefault(name, {})

    def get_config(self) -> Dict[str, Any]:
        return self._state.setdefault(
            "config",
            {
                "browsers": DEFAULT_BROWSERS.copy(),
                "viewports": DEFAULT_VIEWPORTS.copy(),
                "display_timezone": "utc",
                "run_timeout_seconds": 600,
                "max_concurrent_executions": 4,
                "scene_host_url": "http://host.docker.internal:8000",
                "capture_post_wait_ms": 7000,
            },
        )

    def update_config(
        self,
        *,
        browsers: Optional[List[str]] = None,
        viewports: Optional[List[str]] = None,
        display_timezone: Optional[str] = None,
        run_timeout_seconds: Optional[int] = None,
        max_concurrent_executions: Optional[int] = None,
        scene_host_url: Optional[str] = None,
        capture_post_wait_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        with self._lock:
            config = self.get_config()
            if browsers is not None:
                config["browsers"] = browsers
            if viewports is not None:
                config["viewports"] = viewports
            if display_timezone is not None:
                config["display_timezone"] = display_timezone
            if run_timeout_seconds is not None:
                config["run_timeout_seconds"] = run_timeout_seconds
            if max_concurrent_executions is not None:
                config["max_concurrent_executions"] = max_concurrent_executions
            if scene_host_url is not None:
                config["scene_host_url"] = scene_host_url
            if capture_post_wait_ms is not None:
                config["capture_post_wait_ms"] = capture_post_wait_ms
            self._persist()
            return config

    def upsert(self, collection: str, item_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            self._collection(collection)[item_id] = payload
            self._persist()
            return payload

    def get(self, collection: str, item_id: str) -> Optional[Dict[str, Any]]:
        return self._collection(collection).get(item_id)

    def delete(self, collection: str, item_id: str) -> None:
        with self._lock:
            if item_id in self._collection(collection):
                del self._collection(collection)[item_id]
                self._persist()

    def list(self, collection: str) -> List[Dict[str, Any]]:
        return list(self._collection(collection).values())

    def filter(self, collection: str, *, key: str, value: Any) -> List[Dict[str, Any]]:
        return [item for item in self.list(collection) if item.get(key) == value]

    def bulk_delete(self, collection: str, item_ids: Iterable[str]) -> None:
        with self._lock:
            coll = self._collection(collection)
            removed = False
            for item_id in item_ids:
                if item_id in coll:
                    del coll[item_id]
                    removed = True
            if removed:
                self._persist()


class SceneRepository:
    """Repository offering domain-focused helpers on top of LocalDynamoStorage."""

    def __init__(self, storage: LocalDynamoStorage) -> None:
        self._storage = storage

    # -- Config -------------------------------------------------------------------
    def get_config(self) -> Dict[str, Any]:
        config = self._storage.get_config()
        return {
            "browsers": list(config.get("browsers", DEFAULT_BROWSERS.copy())),
            "viewports": list(config.get("viewports", DEFAULT_VIEWPORTS.copy())),
            "display_timezone": config.get("display_timezone", "utc"),
            "run_timeout_seconds": int(config.get("run_timeout_seconds", 600)),
            "max_concurrent_executions": int(config.get("max_concurrent_executions", 4)),
            "scene_host_url": config.get("scene_host_url", "http://host.docker.internal:8000"),
            "capture_post_wait_ms": int(config.get("capture_post_wait_ms", 7000)),
        }

    def set_available_browsers(self, browsers: List[str]) -> Dict[str, Any]:
        trimmed = sorted(set(b.strip().lower() for b in browsers if b.strip()))
        if not trimmed:
            raise ValueError("At least one browser must be selected.")

        used_browsers = set()
        for project in self.list_projects():
            for task in self.list_tasks(project["id"]):
                used_browsers.update(task.get("browsers", []))

        missing = used_browsers - set(trimmed)
        if missing:
            raise ValueError(
                "Cannot remove browsers still used in tasks: " + ", ".join(sorted(missing))
            )

        return self._storage.update_config(browsers=trimmed)

    def set_available_viewports(self, viewports: List[str]) -> Dict[str, Any]:
        cleaned = []
        seen = set()
        for viewport in viewports:
            token = viewport.strip().lower()
            if not token:
                continue
            if "x" not in token:
                raise ValueError(f"Invalid viewport '{viewport}'")
            width_str, height_str = token.split("x", 1)
            if not width_str.isdigit() or not height_str.isdigit():
                raise ValueError(f"Invalid viewport '{viewport}'")
            normalized = f"{int(width_str)}x{int(height_str)}"
            if normalized not in seen:
                seen.add(normalized)
                cleaned.append(normalized)

        if not cleaned:
            raise ValueError("At least one viewport must be provided.")

        used_viewports = set()
        for project in self.list_projects():
            for task in self.list_tasks(project["id"]):
                for viewport in task.get("viewports", []):
                    try:
                        width = int(viewport.get("width"))
                        height = int(viewport.get("height"))
                        used_viewports.add(f"{width}x{height}")
                    except (TypeError, ValueError, AttributeError):
                        continue

        missing = used_viewports - set(cleaned)
        if missing:
            raise ValueError(
                "Cannot remove viewports still used in tasks: " + ", ".join(sorted(missing))
            )

        return self._storage.update_config(viewports=cleaned)

    def set_capture_post_wait_ms(self, milliseconds: int) -> Dict[str, Any]:
        if milliseconds < 0:
            raise ValueError("Capture stabilization delay must be zero or greater.")
        self._storage.update_config(capture_post_wait_ms=int(milliseconds))
        return self.get_config()

    def set_display_timezone(self, timezone_pref: str) -> Dict[str, Any]:
        normalized = timezone_pref.lower()
        if normalized not in {"utc", "local"}:
            raise ValueError("Display timezone must be 'utc' or 'local'.")
        self._storage.update_config(display_timezone=normalized)
        return self.get_config()

    def set_run_timeout_seconds(self, timeout_seconds: int) -> Dict[str, Any]:
        if timeout_seconds <= 0:
            raise ValueError("Run timeout must be a positive number of seconds.")
        self._storage.update_config(run_timeout_seconds=timeout_seconds)
        return self.get_config()

    def set_max_concurrent_executions(self, max_workers: int) -> Dict[str, Any]:
        if max_workers <= 0:
            raise ValueError("Maximum concurrent executions must be a positive integer.")
        if max_workers > 32:
            raise ValueError("Maximum concurrent executions cannot exceed 32 in this environment.")
        self._storage.update_config(max_concurrent_executions=max_workers)
        return self.get_config()

    def set_scene_host_url(self, host_url: str) -> Dict[str, Any]:
        normalized = host_url.strip()
        if not normalized:
            raise ValueError("Scene host URL cannot be blank.")
        self._storage.update_config(scene_host_url=normalized)
        return self.get_config()

    # -- Projects -----------------------------------------------------------------
    def list_projects(self) -> List[Dict[str, Any]]:
        return sorted(self._storage.list("projects"), key=lambda it: it["created_at"])

    def get_project(self, project_id: str) -> Optional[Dict[str, Any]]:
        return self._storage.get("projects", project_id)

    def create_project(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        now = _utcnow()
        project_id = str(uuid.uuid4())
        record = {
            "id": project_id,
            "name": payload["name"],
            "slug": payload["slug"],
            "description": payload.get("description"),
            "created_at": now,
            "updated_at": now,
        }
        return self._storage.upsert("projects", project_id, record)

    def update_project(self, project_id: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        record = self.get_project(project_id)
        if not record:
            return None
        updated = {k: v for k, v in payload.items() if v is not None}
        if "task_js" in updated:
            updated["task_js"] = _sanitize_script(updated["task_js"])
        if "task_actions" in updated:
            updated["task_actions"] = _sanitize_actions(updated["task_actions"])
        record.update(updated)
        record["updated_at"] = _utcnow()
        return self._storage.upsert("projects", project_id, record)

    def delete_project(self, project_id: str) -> None:
        # Cascade delete pages, tasks, batches, runs referencing the project.
        pages = [page["id"] for page in self.list_pages(project_id)]
        tasks = [task["id"] for task in self.list_tasks(project_id)]
        batches = [batch["id"] for batch in self.list_batches(project_id)]
        runs = [run["id"] for run in self.list_runs(project_id=project_id)]
        baselines = [
            baseline["id"] for baseline in self.list_baselines(project_id=project_id)
        ]
        executions = [
            execution["id"]
            for execution in self._storage.list("executions")
            if execution.get("run_id") in runs
        ]

        for collection, ids in [
            ("pages", pages),
            ("tasks", tasks),
            ("batches", batches),
            ("runs", runs),
            ("baselines", baselines),
            ("executions", executions),
        ]:
            self._storage.bulk_delete(collection, ids)

        self._storage.delete("projects", project_id)

    # -- Pages --------------------------------------------------------------------
    def list_pages(self, project_id: str) -> List[Dict[str, Any]]:
        return sorted(
            self._storage.filter("pages", key="project_id", value=project_id),
            key=lambda it: it["created_at"],
        )

    def get_page(self, page_id: str) -> Optional[Dict[str, Any]]:
        return self._storage.get("pages", page_id)

    def create_page(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        now = _utcnow()
        page_id = str(uuid.uuid4())
        record = {
            "id": page_id,
            "project_id": payload["project_id"],
            "name": payload["name"],
            "url": str(payload["url"]),
            "reference_url": str(payload["reference_url"])
            if payload.get("reference_url")
            else None,
            "preparatory_js": _sanitize_script(payload.get("preparatory_js")),
            "preparatory_actions": _sanitize_actions(payload.get("preparatory_actions")),
            "basic_auth_username": payload.get("basic_auth_username") or None,
            "basic_auth_password": payload.get("basic_auth_password") or None,
            "created_at": now,
            "updated_at": now,
        }
        return self._storage.upsert("pages", page_id, record)

    def update_page(self, page_id: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        record = self.get_page(page_id)
        if not record:
            return None
        updates = {}
        for key, value in payload.items():
            if value is None:
                continue
            if key in {"url", "reference_url"}:
                updates[key] = str(value)
            else:
                if key in {"basic_auth_username", "basic_auth_password"} and not value:
                    updates[key] = None
                elif key == "preparatory_js":
                    updates[key] = _sanitize_script(value)
                elif key == "preparatory_actions":
                    updates[key] = _sanitize_actions(value)
                else:
                    updates[key] = value
        record.update(updates)
        record["updated_at"] = _utcnow()
        return self._storage.upsert("pages", page_id, record)

    def delete_page(self, page_id: str) -> None:
        # Remove tasks tied to the page.
        tasks = [
            task["id"]
            for task in self._storage.filter("tasks", key="page_id", value=page_id)
        ]
        self._storage.bulk_delete("tasks", tasks)
        self._storage.delete("pages", page_id)

    # -- Tasks --------------------------------------------------------------------
    def list_tasks(self, project_id: str) -> List[Dict[str, Any]]:
        return sorted(
            self._storage.filter("tasks", key="project_id", value=project_id),
            key=lambda it: it["created_at"],
        )

    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        return self._storage.get("tasks", task_id)

    def create_task(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        now = _utcnow()
        task_id = str(uuid.uuid4())
        record = {
            "id": task_id,
            "project_id": payload["project_id"],
            "page_id": payload["page_id"],
            "name": payload["name"],
            "task_js": _sanitize_script(payload.get("task_js")),
            "browsers": payload.get("browsers", []),
            "viewports": payload.get("viewports", []),
            "task_actions": _sanitize_actions(payload.get("task_actions")),
            "created_at": now,
            "updated_at": now,
        }
        return self._storage.upsert("tasks", task_id, record)

    def update_task(self, task_id: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        record = self.get_task(task_id)
        if not record:
            return None
        record.update({k: v for k, v in payload.items() if v is not None})
        record["updated_at"] = _utcnow()
        return self._storage.upsert("tasks", task_id, record)

    def delete_task(self, task_id: str) -> None:
        # Remove task references from batches.
        batches = self._storage.list("batches")
        updates = []
        for batch in batches:
            if task_id in batch.get("task_ids", []):
                batch["task_ids"] = [tid for tid in batch["task_ids"] if tid != task_id]
                batch["updated_at"] = _utcnow()
                updates.append(batch)
        for batch in updates:
            self._storage.upsert("batches", batch["id"], batch)
        # Remove baseline items for the task.
        baselines = self._storage.list("baselines")
        for baseline in baselines:
            items = baseline.get("items") or []
            filtered = [
                item for item in items if item.get("task_id") != task_id
            ]
            if len(filtered) != len(items):
                baseline["items"] = filtered
                baseline["updated_at"] = _utcnow()
                self._storage.upsert("baselines", baseline["id"], baseline)
        # Remove executions for the task.
        executions = [
            execution["id"]
            for execution in self._storage.list("executions")
            if execution.get("task_id") == task_id
        ]
        self._storage.bulk_delete("executions", executions)
        self._storage.delete("tasks", task_id)

    # -- Batches ------------------------------------------------------------------
    def list_batches(self, project_id: str) -> List[Dict[str, Any]]:
        return sorted(
            self._storage.filter("batches", key="project_id", value=project_id),
            key=lambda it: it["created_at"],
        )

    def get_batch(self, batch_id: str) -> Optional[Dict[str, Any]]:
        return self._storage.get("batches", batch_id)

    def create_batch(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        now = _utcnow()
        batch_id = str(uuid.uuid4())
        record = {
            "id": batch_id,
            "project_id": payload["project_id"],
            "name": payload["name"],
            "description": payload.get("description"),
            "task_ids": payload.get("task_ids", []),
            "jira_issue": payload.get("jira_issue"),
            "run_diff_threshold": payload.get("run_diff_threshold"),
            "execution_diff_threshold": payload.get("execution_diff_threshold"),
            "created_at": now,
            "updated_at": now,
        }
        return self._storage.upsert("batches", batch_id, record)

    def update_batch(self, batch_id: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        record = self.get_batch(batch_id)
        if not record:
            return None
        threshold_keys = {"run_diff_threshold", "execution_diff_threshold"}
        for key in threshold_keys:
            if key in payload:
                record[key] = payload[key]
        record.update({k: v for k, v in payload.items() if v is not None and k not in threshold_keys})
        record["updated_at"] = _utcnow()
        return self._storage.upsert("batches", batch_id, record)

    def delete_batch(self, batch_id: str) -> None:
        # Remove runs tied to the batch.
        runs = [
            run["id"]
            for run in self._storage.filter("runs", key="batch_id", value=batch_id)
        ]
        if runs:
            executions = [
                execution["id"]
                for execution in self._storage.list("executions")
                if execution.get("run_id") in runs
            ]
            self._storage.bulk_delete("executions", executions)
        self._storage.bulk_delete("runs", runs)
        # Remove baselines tied to the batch.
        baselines = [
            baseline["id"]
            for baseline in self._storage.filter("baselines", key="batch_id", value=batch_id)
        ]
        self._storage.bulk_delete("baselines", baselines)
        self._storage.delete("batches", batch_id)

    # -- Baselines ----------------------------------------------------------------
    def list_baselines(
        self,
        *,
        project_id: Optional[str] = None,
        batch_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        items = self._storage.list("baselines")
        if project_id:
            items = [it for it in items if it.get("project_id") == project_id]
        if batch_id:
            items = [it for it in items if it.get("batch_id") == batch_id]
        return sorted(items, key=lambda it: it["created_at"], reverse=True)

    def get_baseline(self, baseline_id: str) -> Optional[Dict[str, Any]]:
        return self._storage.get("baselines", baseline_id)

    def create_baseline(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        now = _utcnow()
        baseline_id = str(uuid.uuid4())
        record = {
            "id": baseline_id,
            "project_id": payload["project_id"],
            "batch_id": payload["batch_id"],
            "run_id": payload.get("run_id"),
            "status": payload.get("status", "pending"),
            "note": payload.get("note"),
            "items": payload.get("items", []),
            "created_at": now,
            "updated_at": now,
        }
        return self._storage.upsert("baselines", baseline_id, record)

    def update_baseline(self, baseline_id: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        record = self.get_baseline(baseline_id)
        if not record:
            return None
        for key, value in payload.items():
            if value is None:
                continue
            if key == "items":
                record["items"] = value
            else:
                record[key] = value
        record["updated_at"] = _utcnow()
        return self._storage.upsert("baselines", baseline_id, record)

    def append_baseline_item(self, baseline_id: str, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        record = self.get_baseline(baseline_id)
        if not record:
            return None
        items = record.get("items") or []
        items.append(item)
        record["items"] = items
        record["updated_at"] = _utcnow()
        return self._storage.upsert("baselines", baseline_id, record)

    def delete_baseline(self, baseline_id: str) -> None:
        store = get_artifact_store()
        store.purge_baseline(baseline_id)
        self._storage.delete("baselines", baseline_id)

    def get_latest_baseline(self, batch_id: str) -> Optional[Dict[str, Any]]:
        baselines = self.list_baselines(batch_id=batch_id)
        return baselines[0] if baselines else None

    # -- Runs ---------------------------------------------------------------------
    def list_runs(
        self,
        *,
        project_id: Optional[str] = None,
        batch_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        items = self._storage.list("runs")
        if project_id:
            items = [it for it in items if it.get("project_id") == project_id]
        if batch_id:
            items = [it for it in items if it.get("batch_id") == batch_id]
        return sorted(items, key=lambda it: it["created_at"], reverse=True)

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        return self._storage.get("runs", run_id)

    def create_run(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        now = _utcnow()
        run_id = str(uuid.uuid4())
        jira_issue = payload.get("jira_issue")
        if not jira_issue and payload.get("batch_id"):
            batch = self.get_batch(payload["batch_id"])
            if batch:
                jira_issue = batch.get("jira_issue")
        summary = payload.get("summary") or {
            "executions_total": 0,
            "executions_finished": 0,
            "executions_failed": 0,
            "executions_cancelled": 0,
            "diff_average": 0.0,
            "diff_maximum": 0.0,
            "diff_samples": 0,
        }
        timeout_seconds = payload.get("timeout_seconds")
        if timeout_seconds is None:
            timeout_seconds = int(self.get_config().get("run_timeout_seconds", 600))
        else:
            timeout_seconds = int(timeout_seconds)
        record = {
            "id": run_id,
            "project_id": payload["project_id"],
            "batch_id": payload["batch_id"],
            "baseline_id": payload.get("baseline_id"),
            "purpose": payload["purpose"],
            "status": payload.get("status", "queued"),
            "requested_by": payload.get("requested_by"),
            "note": payload.get("note"),
            "jira_issue": jira_issue,
            "created_at": now,
            "updated_at": now,
            "summary": summary,
            "timeout_seconds": timeout_seconds,
        }
        return self._storage.upsert("runs", run_id, record)

    def update_run(self, run_id: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        record = self.get_run(run_id)
        if not record:
            return None
        record.update({k: v for k, v in payload.items() if v is not None})
        record["updated_at"] = _utcnow()
        return self._storage.upsert("runs", run_id, record)

    def delete_run(self, run_id: str, *, cascade_baseline: bool = False) -> None:
        run = self.get_run(run_id)
        if not run:
            return
        store = get_artifact_store()
        store.purge_run(run_id)
        executions = [
            execution["id"]
            for execution in self._storage.list("executions")
            if execution.get("run_id") == run_id
        ]
        self._storage.bulk_delete("executions", executions)
        self._storage.delete("runs", run_id)
        if cascade_baseline:
            baseline_id = run.get("baseline_id")
            if baseline_id:
                baseline = self.get_baseline(baseline_id)
                if not baseline:
                    return
                still_linked = any(
                    other.get("baseline_id") == baseline_id and other.get("id") != run_id
                    for other in self._storage.list("runs")
                )
                if baseline.get("run_id") == run_id or not still_linked:
                    self.delete_baseline(baseline_id)

    # -- Executions ----------------------------------------------------------------
    def list_executions(
        self,
        *,
        run_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        items = self._storage.list("executions")
        if run_id:
            items = [it for it in items if it.get("run_id") == run_id]
        return sorted(
            items,
            key=lambda it: (
                it.get("sequence", 0) or 0,
                it["created_at"],
            ),
        )

    def get_execution(self, execution_id: str) -> Optional[Dict[str, Any]]:
        return self._storage.get("executions", execution_id)

    def create_execution(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        now = _utcnow()
        execution_id = str(uuid.uuid4())
        record = {
            "id": execution_id,
            "run_id": payload["run_id"],
            "project_id": payload["project_id"],
            "batch_id": payload["batch_id"],
            "task_id": payload["task_id"],
            "task_name": payload["task_name"],
            "page_id": payload.get("page_id"),
            "browser": payload["browser"],
            "viewport": payload["viewport"],
            "status": payload.get("status", "queued"),
            "sequence": payload.get("sequence"),
            "artifacts": payload.get("artifacts", {}),
            "diff": payload.get("diff"),
            "message": payload.get("message"),
            "created_at": now,
            "updated_at": now,
            "started_at": payload.get("started_at"),
            "completed_at": payload.get("completed_at"),
        }
        return self._storage.upsert("executions", execution_id, record)

    def update_execution(self, execution_id: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        record = self.get_execution(execution_id)
        if not record:
            return None
        for key, value in payload.items():
            if value is None:
                continue
            if key == "artifacts":
                artifacts = record.get("artifacts", {}).copy()
                artifacts.update(value)
                record["artifacts"] = artifacts
            elif key == "diff":
                record["diff"] = value
            else:
                record[key] = value
        record["updated_at"] = _utcnow()
        return self._storage.upsert("executions", execution_id, record)

    def delete_execution(self, execution_id: str) -> None:
        self._storage.delete("executions", execution_id)

    def execution_status_counts(self, run_id: str) -> Dict[str, int]:
        counts = {
            "queued": 0,
            "executing": 0,
            "finished": 0,
            "failed": 0,
            "cancelled": 0,
        }
        for execution in self.list_executions(run_id=run_id):
            status = execution.get("status", "queued")
            if status not in counts:
                continue
            counts[status] += 1
        return counts


_repository: Optional[SceneRepository] = None


def get_repository() -> SceneRepository:
    """FastAPI dependency to retrieve the singleton repository instance."""
    global _repository
    if _repository is None:
        storage_path = Path("dev.dynamodb.json")
        backend = LocalDynamoStorage(storage_path)
        _repository = SceneRepository(backend)
    return _repository


RepositoryDep = Depends(get_repository)
