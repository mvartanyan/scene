from __future__ import annotations

import json
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from fastapi import Depends

from app.constants import DEFAULT_BROWSERS, DEFAULT_VIEWPORTS

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
        "config": {
            "browsers": DEFAULT_BROWSERS.copy(),
            "viewports": DEFAULT_VIEWPORTS.copy(),
        },
    }


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
            if "config" not in state:
                state["config"] = {
                    "browsers": DEFAULT_BROWSERS.copy(),
                    "viewports": DEFAULT_VIEWPORTS.copy(),
                }
            else:
                state_config = state["config"]
                state_config.setdefault("browsers", DEFAULT_BROWSERS.copy())
                state_config.setdefault("viewports", DEFAULT_VIEWPORTS.copy())
            return state

    def _persist(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("w", encoding="utf-8") as handle:
            json.dump(self._state, handle, indent=2, sort_keys=True)

    def _collection(self, name: str) -> Dict[str, Any]:
        return self._state.setdefault(name, {})

    def get_config(self) -> Dict[str, List[str]]:
        return self._state.setdefault(
            "config",
            {
                "browsers": DEFAULT_BROWSERS.copy(),
                "viewports": DEFAULT_VIEWPORTS.copy(),
            },
        )

    def update_config(self, *, browsers: Optional[List[str]] = None, viewports: Optional[List[str]] = None) -> Dict[str, List[str]]:
        with self._lock:
            config = self.get_config()
            if browsers is not None:
                config["browsers"] = browsers
            if viewports is not None:
                config["viewports"] = viewports
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
    def get_config(self) -> Dict[str, List[str]]:
        config = self._storage.get_config()
        return {
            "browsers": list(config.get("browsers", DEFAULT_BROWSERS.copy())),
            "viewports": list(config.get("viewports", DEFAULT_VIEWPORTS.copy())),
        }

    def set_available_browsers(self, browsers: List[str]) -> Dict[str, List[str]]:
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

    def set_available_viewports(self, viewports: List[str]) -> Dict[str, List[str]]:
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
        record.update({k: v for k, v in payload.items() if v is not None})
        record["updated_at"] = _utcnow()
        return self._storage.upsert("projects", project_id, record)

    def delete_project(self, project_id: str) -> None:
        # Cascade delete pages, tasks, batches, runs referencing the project.
        pages = [page["id"] for page in self.list_pages(project_id)]
        tasks = [task["id"] for task in self.list_tasks(project_id)]
        batches = [batch["id"] for batch in self.list_batches(project_id)]
        runs = [run["id"] for run in self.list_runs(project_id=project_id)]

        for collection, ids in [
            ("pages", pages),
            ("tasks", tasks),
            ("batches", batches),
            ("runs", runs),
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
            "preparatory_js": payload.get("preparatory_js"),
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
            "task_js": payload.get("task_js"),
            "browsers": payload.get("browsers", []),
            "viewports": payload.get("viewports", []),
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
            "created_at": now,
            "updated_at": now,
        }
        return self._storage.upsert("batches", batch_id, record)

    def update_batch(self, batch_id: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        record = self.get_batch(batch_id)
        if not record:
            return None
        record.update({k: v for k, v in payload.items() if v is not None})
        record["updated_at"] = _utcnow()
        return self._storage.upsert("batches", batch_id, record)

    def delete_batch(self, batch_id: str) -> None:
        # Remove runs tied to the batch.
        runs = [
            run["id"]
            for run in self._storage.filter("runs", key="batch_id", value=batch_id)
        ]
        self._storage.bulk_delete("runs", runs)
        self._storage.delete("batches", batch_id)

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
            "summary": payload.get("summary", {}),
        }
        return self._storage.upsert("runs", run_id, record)

    def update_run(self, run_id: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        record = self.get_run(run_id)
        if not record:
            return None
        record.update({k: v for k, v in payload.items() if v is not None})
        record["updated_at"] = _utcnow()
        return self._storage.upsert("runs", run_id, record)

    def delete_run(self, run_id: str) -> None:
        self._storage.delete("runs", run_id)


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
