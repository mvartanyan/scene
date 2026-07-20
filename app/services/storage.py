from __future__ import annotations

import hashlib
import hmac
import json
import os
import threading
import uuid
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
from math import ceil
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Tuple

from fastapi import Depends

from app.constants import DEFAULT_BROWSERS, DEFAULT_VIEWPORTS
from app.services.artifacts import get_artifact_store
from app.services.storage_types import (
    InvalidStorageCursorError,
    StorageBackend,
    StorageConflictError,
)

STATE_VERSION = 1
SCENE_STATE_PATH_ENV = "SCENE_STATE_PATH"
SCENE_HOST_URL_ENV = "SCENE_HOST_URL"
SCENE_MAX_CONCURRENT_EXECUTIONS_ENV = "SCENE_MAX_CONCURRENT_EXECUTIONS"
SCENE_RUN_TIMEOUT_SECONDS_ENV = "SCENE_RUN_TIMEOUT_SECONDS"
SCENE_CAPTURE_DELAY_MS_ENV = "SCENE_CAPTURE_DELAY_MS"
SCENE_DIFF_PIXEL_TOLERANCE_ENV = "SCENE_DIFF_PIXEL_TOLERANCE"
DEFAULT_LOCAL_ROOT = Path(".scene")
DEFAULT_STATE_PATH = DEFAULT_LOCAL_ROOT / "dev.dynamodb.json"
SCENE_STATE_BACKEND_ENV = "SCENE_STATE_BACKEND"
SCENE_DYNAMODB_TABLE_ENV = "SCENE_DYNAMODB_TABLE"
SCENE_DYNAMODB_ENDPOINT_URL_ENV = "SCENE_DYNAMODB_ENDPOINT_URL"
SCENE_AWS_REGION_ENV = "AWS_REGION"
DEFAULT_AWS_REGION = "eu-central-1"

CONFIG_COLLECTIONS = ("projects", "pages", "tasks", "batches")
MAX_CONFLICT_RETRIES = 5
TERMINAL_RUN_STATUSES = {"finished", "failed", "cancelled"}
TERMINAL_EXECUTION_STATUSES = {"finished", "failed", "cancelled"}
RUN_IDEMPOTENCY_NAMESPACE = uuid.UUID("6740667e-e6d2-4f80-bbe8-8c73742f38d2")
BASELINE_IDEMPOTENCY_NAMESPACE = uuid.UUID("a2c250c5-8a92-4d83-bc94-34f167285f62")
EXECUTION_IDEMPOTENCY_NAMESPACE = uuid.UUID("7aaef15c-3e65-49f4-b122-49883d68d85e")


def resolve_state_path() -> Path:
    configured = os.environ.get(SCENE_STATE_PATH_ENV)
    if configured:
        return Path(configured).expanduser()
    return DEFAULT_STATE_PATH


def _env_int(name: str, default: int) -> int:
    configured = os.environ.get(name)
    if not configured:
        return default
    try:
        return int(configured)
    except ValueError:
        return default


def _default_config() -> Dict[str, Any]:
    return {
        "browsers": DEFAULT_BROWSERS.copy(),
        "viewports": DEFAULT_VIEWPORTS.copy(),
        "display_timezone": "utc",
        "run_timeout_seconds": _env_int(SCENE_RUN_TIMEOUT_SECONDS_ENV, 600),
        "max_concurrent_executions": _env_int(SCENE_MAX_CONCURRENT_EXECUTIONS_ENV, 4),
        "scene_host_url": os.environ.get(
            SCENE_HOST_URL_ENV,
            "http://host.docker.internal:8000",
        ),
        "capture_post_wait_ms": _env_int(SCENE_CAPTURE_DELAY_MS_ENV, 7000),
        "diff_pixel_tolerance": _env_int(SCENE_DIFF_PIXEL_TOLERANCE_ENV, 0),
    }


def _apply_config_env_overrides(config: Dict[str, Any]) -> Dict[str, Any]:
    overridden = dict(config)
    if os.environ.get(SCENE_HOST_URL_ENV):
        overridden["scene_host_url"] = os.environ[SCENE_HOST_URL_ENV]
    for env_name, key in [
        (SCENE_MAX_CONCURRENT_EXECUTIONS_ENV, "max_concurrent_executions"),
        (SCENE_RUN_TIMEOUT_SECONDS_ENV, "run_timeout_seconds"),
        (SCENE_CAPTURE_DELAY_MS_ENV, "capture_post_wait_ms"),
        (SCENE_DIFF_PIXEL_TOLERANCE_ENV, "diff_pixel_tolerance"),
    ]:
        if os.environ.get(env_name):
            overridden[key] = _env_int(env_name, int(overridden.get(key, 0) or 0))
    return overridden


def _utcnow() -> str:
    """Return timezone-aware ISO timestamp."""
    return datetime.now(tz=timezone.utc).isoformat()


def _future_iso(seconds: int) -> str:
    return datetime.fromtimestamp(
        datetime.now(tz=timezone.utc).timestamp() + max(1, int(seconds)),
        tz=timezone.utc,
    ).isoformat()


def _parse_utc_timestamp(value: object) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _artifact_records(value: object) -> List[Dict[str, Any]]:
    """Collect persisted artifact metadata without inferring storage prefixes."""

    records: List[Dict[str, Any]] = []
    if isinstance(value, dict):
        if value.get("key") or value.get("path"):
            records.append(value)
        else:
            for child in value.values():
                records.extend(_artifact_records(child))
    elif isinstance(value, list):
        for child in value:
            records.extend(_artifact_records(child))
    return records


def _fingerprint(payload: Dict[str, Any], fields: Iterable[str]) -> str:
    canonical = {field: payload.get(field) for field in fields}
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


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
        "config": _default_config(),
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


def _spm_ticket_value(payload: Dict[str, Any]) -> Optional[str]:
    value = payload.get("spm_ticket")
    if value is None:
        value = payload.get("jira_issue")
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return value


def _with_spm_ticket(record: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if record is None:
        return None
    normalized = dict(record)
    normalized["spm_ticket"] = _spm_ticket_value(normalized)
    return normalized


def _apply_spm_ticket_update(record: Dict[str, Any], payload: Dict[str, Any]) -> None:
    if "spm_ticket" not in payload and "jira_issue" not in payload:
        return
    value = _spm_ticket_value(payload)
    record["spm_ticket"] = value
    # Retain the legacy storage key during the compatibility window so older
    # state files and direct consumers keep seeing the same value.
    record["jira_issue"] = value


class LocalDynamoStorage:
    """Very small DynamoDB-like persistence layer backed by a JSON file.

    The goal is to mimic the partitioned collections we expect from Dynamo without
    introducing AWS dependencies. Each top-level collection stores items keyed
    by their primary identifier. All writes are synchronised via an internal lock.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.RLock()
        self._state = self._load()
        self._transaction_depth = 0
        self._transaction_dirty = False

    def _load(self) -> Dict[str, Any]:
        if not self._path.exists():
            return _default_state()
        with self._path.open("r", encoding="utf-8") as handle:
            state = json.load(handle)
            state.setdefault("baselines", {})
            state.setdefault("executions", {})
            if "config" not in state:
                state["config"] = _default_config()
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
                batch.setdefault("spm_ticket", batch.get("jira_issue"))
            for run in state.get("runs", {}).values():
                run.setdefault("spm_ticket", run.get("jira_issue"))
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
        if self._transaction_depth:
            self._transaction_dirty = True
            return
        self._write_state()

    def _write_state(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("w", encoding="utf-8") as handle:
            json.dump(self._state, handle, indent=2, sort_keys=True)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Persist a group of local writes once and roll it back on failure."""
        with self._lock:
            outermost = self._transaction_depth == 0
            snapshot = deepcopy(self._state) if outermost else None
            self._transaction_depth += 1
            try:
                yield
            except Exception:
                self._transaction_depth -= 1
                if outermost:
                    assert snapshot is not None
                    self._state = snapshot
                    self._transaction_dirty = False
                raise
            else:
                self._transaction_depth -= 1
                if outermost and self._transaction_dirty:
                    self._transaction_dirty = False
                    self._write_state()

    def _collection(self, name: str) -> Dict[str, Any]:
        return self._state.setdefault(name, {})

    def get_config(self) -> Dict[str, Any]:
        return _apply_config_env_overrides(
            self._state.setdefault("config", _default_config())
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
        diff_pixel_tolerance: Optional[int] = None,
        **extra_updates: Any,
    ) -> Dict[str, Any]:
        with self._lock:
            config = self._state.setdefault("config", _default_config())
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
            if diff_pixel_tolerance is not None:
                config["diff_pixel_tolerance"] = diff_pixel_tolerance
            for key, value in extra_updates.items():
                if value is not None:
                    config[key] = value
            self._persist()
            return _apply_config_env_overrides(config)

    def upsert(self, collection: str, item_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            current = self._collection(collection).get(item_id)
            record = deepcopy(payload)
            expected_version = record.pop("_version", None)
            if current is None:
                if expected_version is not None:
                    raise StorageConflictError(
                        f"{collection}/{item_id} was deleted before it could be updated"
                    )
                next_version = 1
            else:
                current_version = int(current.get("_version", 0))
                if expected_version is None or int(expected_version) != current_version:
                    raise StorageConflictError(
                        f"{collection}/{item_id} changed during update"
                    )
                next_version = current_version + 1
            record["_version"] = next_version
            self._collection(collection)[item_id] = record
            self._persist()
            return deepcopy(record)

    def get(self, collection: str, item_id: str) -> Optional[Dict[str, Any]]:
        record = self._collection(collection).get(item_id)
        if record is None:
            return None
        copied = deepcopy(record)
        copied.setdefault("_version", 0)
        return copied

    def delete(self, collection: str, item_id: str) -> None:
        with self._lock:
            if item_id in self._collection(collection):
                del self._collection(collection)[item_id]
                self._persist()

    def list(self, collection: str) -> List[Dict[str, Any]]:
        records = []
        for record in self._collection(collection).values():
            copied = deepcopy(record)
            copied.setdefault("_version", 0)
            records.append(copied)
        return records

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

    def query_page(
        self,
        collection: str,
        *,
        key: Optional[str] = None,
        value: Any = None,
        limit: int = 25,
        cursor: Optional[str] = None,
        descending: bool = False,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        records = self.list(collection)
        if key is not None:
            records = [record for record in records if record.get(key) == value]
        records.sort(
            key=lambda record: (
                str(record.get("created_at") or ""),
                str(record.get("id") or ""),
            ),
            reverse=descending,
        )
        try:
            offset = int(cursor or 0)
        except (TypeError, ValueError) as exc:
            raise InvalidStorageCursorError("Invalid pagination cursor") from exc
        if offset < 0:
            raise InvalidStorageCursorError("Invalid pagination cursor")
        page_size = max(1, min(int(limit), 100))
        page = records[offset : offset + page_size]
        next_offset = offset + len(page)
        next_cursor = str(next_offset) if next_offset < len(records) else None
        return page, next_cursor

    def backend_info(self) -> Dict[str, Any]:
        return {"backend": "json", "path": str(self._path)}

    def count(
        self,
        collection: str,
        *,
        key: Optional[str] = None,
        value: Any = None,
    ) -> int:
        if key is None:
            return len(self._collection(collection))
        return sum(
            1
            for record in self._collection(collection).values()
            if record.get(key) == value
        )

    def probe(self) -> Dict[str, Any]:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        return {"ok": True, **self.backend_info()}


class SceneRepository:
    """Repository offering domain-focused helpers on top of a storage backend."""

    def __init__(self, storage: StorageBackend) -> None:
        self._storage = storage

    @contextmanager
    def transaction(self) -> Iterator[None]:
        with self._storage.transaction():
            yield

    def backend_info(self) -> Dict[str, Any]:
        return self._storage.backend_info()

    def probe(self) -> Dict[str, Any]:
        return self._storage.probe()

    def dispatcher_status(self) -> Optional[Dict[str, Any]]:
        return self._storage.get("leases", "k3s-dispatcher")

    def acquire_dispatcher_lease(
        self,
        owner: str,
        *,
        lease_seconds: int = 30,
    ) -> bool:
        now = _utcnow()
        for attempt in range(MAX_CONFLICT_RETRIES):
            record = self._storage.get("leases", "k3s-dispatcher")
            if record:
                current_owner = str(record.get("owner") or "")
                expires_at = str(record.get("expires_at") or "")
                if current_owner != owner and expires_at > now:
                    return False
                if current_owner != owner:
                    for field in (
                        "capabilities",
                        "capabilities_checked_at",
                        "capabilities_ok",
                    ):
                        record.pop(field, None)
                record.update(
                    {
                        "owner": owner,
                        "heartbeat_at": now,
                        "expires_at": _future_iso(lease_seconds),
                    }
                )
            else:
                record = {
                    "id": "k3s-dispatcher",
                    "owner": owner,
                    "created_at": now,
                    "heartbeat_at": now,
                    "expires_at": _future_iso(lease_seconds),
                }
            try:
                self._storage.upsert("leases", "k3s-dispatcher", record)
                return True
            except StorageConflictError:
                if attempt + 1 == MAX_CONFLICT_RETRIES:
                    return False
        return False

    def release_dispatcher_lease(self, owner: str) -> bool:
        released = False

        def mutate(record: Dict[str, Any]) -> Optional[bool]:
            nonlocal released
            released = False
            if record.get("owner") != owner:
                return False
            record["expires_at"] = _utcnow()
            record["released_at"] = _utcnow()
            released = True
            return None

        self._update_record("leases", "k3s-dispatcher", mutate)
        return released

    def report_dispatcher_health(
        self,
        owner: str,
        *,
        capabilities: Dict[str, bool],
    ) -> bool:
        reported = False

        def mutate(record: Dict[str, Any]) -> Optional[bool]:
            nonlocal reported
            reported = False
            if record.get("owner") != owner or str(record.get("expires_at") or "") <= _utcnow():
                return False
            normalized = {
                str(name): bool(allowed)
                for name, allowed in sorted(capabilities.items())
            }
            record["capabilities"] = normalized
            record["capabilities_ok"] = bool(normalized) and all(normalized.values())
            record["capabilities_checked_at"] = _utcnow()
            reported = True
            return None

        self._update_record("leases", "k3s-dispatcher", mutate)
        return reported

    def _update_record(
        self,
        collection: str,
        item_id: str,
        mutate: Callable[[Dict[str, Any]], Optional[bool]],
    ) -> Optional[Dict[str, Any]]:
        for attempt in range(MAX_CONFLICT_RETRIES):
            record = self._storage.get(collection, item_id)
            if record is None:
                return None
            changed = mutate(record)
            if changed is False:
                return record
            try:
                return self._storage.upsert(collection, item_id, record)
            except StorageConflictError:
                if attempt + 1 == MAX_CONFLICT_RETRIES:
                    raise
        raise AssertionError("unreachable")

    def configuration_records(self) -> Dict[str, List[Dict[str, Any]]]:
        return {
            collection: self._storage.list(collection)
            for collection in CONFIG_COLLECTIONS
        }

    def get_configuration_record(
        self,
        collection: str,
        item_id: str,
    ) -> Optional[Dict[str, Any]]:
        if collection not in CONFIG_COLLECTIONS:
            raise ValueError(f"Unsupported configuration collection: {collection}")
        return self._storage.get(collection, item_id)

    def put_configuration_record(
        self,
        collection: str,
        record: Dict[str, Any],
    ) -> Dict[str, Any]:
        if collection not in CONFIG_COLLECTIONS:
            raise ValueError(f"Unsupported configuration collection: {collection}")
        item_id = str(record.get("id") or "").strip()
        if not item_id:
            raise ValueError(f"Imported {collection} record is missing an id")
        candidate = deepcopy(record)
        candidate.pop("_version", None)
        for attempt in range(MAX_CONFLICT_RETRIES):
            current = self._storage.get(collection, item_id)
            if current is not None:
                candidate["_version"] = current.get("_version", 0)
            else:
                candidate.pop("_version", None)
            try:
                return self._storage.upsert(collection, item_id, candidate)
            except StorageConflictError:
                if attempt + 1 == MAX_CONFLICT_RETRIES:
                    raise
        raise AssertionError("unreachable")

    def replace_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        return self._storage.update_config(**config)

    def query_page(
        self,
        collection: str,
        *,
        key: Optional[str] = None,
        value: Any = None,
        limit: int = 25,
        cursor: Optional[str] = None,
        descending: bool = False,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        return self._storage.query_page(
            collection,
            key=key,
            value=value,
            limit=limit,
            cursor=cursor,
            descending=descending,
        )

    def count(
        self,
        collection: str,
        *,
        key: Optional[str] = None,
        value: Any = None,
    ) -> int:
        return self._storage.count(collection, key=key, value=value)

    def numbered_page(
        self,
        collection: str,
        *,
        key: Optional[str] = None,
        value: Any = None,
        page: int = 1,
        page_size: int = 25,
        descending: bool = False,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        resolved_size = max(1, min(int(page_size or 25), 100))
        total = self._storage.count(collection, key=key, value=value)
        total_pages = max(1, ceil(total / resolved_size))
        resolved_page = max(1, min(int(page or 1), total_pages))
        cursor: Optional[str] = None
        for _index in range(1, resolved_page):
            _discarded, cursor = self._storage.query_page(
                collection,
                key=key,
                value=value,
                limit=resolved_size,
                cursor=cursor,
                descending=descending,
            )
            if cursor is None:
                break
        items, next_cursor = self._storage.query_page(
            collection,
            key=key,
            value=value,
            limit=resolved_size,
            cursor=cursor,
            descending=descending,
        )
        start = ((resolved_page - 1) * resolved_size) + 1 if total else 0
        end = min(start + len(items) - 1, total) if total else 0
        return items, {
            "page": resolved_page,
            "page_size": resolved_size,
            "total": total,
            "total_pages": total_pages,
            "start": start,
            "end": end,
            "has_previous": resolved_page > 1,
            "has_next": next_cursor is not None,
            "previous_page": resolved_page - 1 if resolved_page > 1 else None,
            "next_page": resolved_page + 1 if next_cursor is not None else None,
        }

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
            "diff_pixel_tolerance": int(config.get("diff_pixel_tolerance", 0)),
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

    def set_diff_pixel_tolerance(self, tolerance: int) -> Dict[str, Any]:
        if tolerance < 0 or tolerance > 255:
            raise ValueError("Diff pixel tolerance must be between 0 and 255.")
        self._storage.update_config(diff_pixel_tolerance=int(tolerance))
        return self.get_config()

    # -- Projects -----------------------------------------------------------------
    def list_projects(self) -> List[Dict[str, Any]]:
        return sorted(self._storage.list("projects"), key=lambda it: it["created_at"])

    def _consistent_runs(
        self,
        *,
        project_id: Optional[str] = None,
        batch_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        return [
            normalized
            for item in self._storage.list("runs")
            if (normalized := _with_spm_ticket(item)) is not None
            and (not project_id or item.get("project_id") == project_id)
            and (not batch_id or item.get("batch_id") == batch_id)
        ]

    def _consistent_scope(
        self,
        collection: str,
        *,
        key: str,
        value: str,
    ) -> List[Dict[str, Any]]:
        return [
            record
            for record in self._storage.list(collection)
            if str(record.get(key) or "") == str(value)
        ]

    def _run_execution_records(self, run_id: str) -> List[Dict[str, Any]]:
        return [
            execution
            for execution in self._storage.list("executions")
            if execution.get("run_id") == run_id
        ]

    @staticmethod
    def _assert_run_deletable(
        run: Dict[str, Any],
        executions: Iterable[Dict[str, Any]],
    ) -> None:
        execution_records = list(executions)
        if run.get("status") in {"queued", "executing"} or any(
            execution.get("status") in {"queued", "executing"}
            for execution in execution_records
        ):
            raise ValueError("Cannot delete a run while it is active.")
        if any(
            (execution.get("kubernetes_job_name") or execution.get("kubernetes_secret_name"))
            and not execution.get("kubernetes_cleaned_at")
            for execution in execution_records
        ):
            raise ValueError("Cannot delete a run while Kubernetes cleanup is pending.")
        now = datetime.now(tz=timezone.utc)
        active_transfer_expiries = []
        for execution in execution_records:
            transfer = execution.get("artifact_transfer")
            if not isinstance(transfer, dict):
                continue
            expires_at = _parse_utc_timestamp(transfer.get("expires_at"))
            if expires_at and expires_at > now:
                active_transfer_expiries.append(expires_at)
        if active_transfer_expiries:
            retry_after = max(active_transfer_expiries).isoformat()
            raise ValueError(
                "Cannot delete a run while an artifact upload URL is still valid; "
                f"retry after {retry_after}."
            )

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
        updated = {k: v for k, v in payload.items() if v is not None}
        if "task_js" in updated:
            updated["task_js"] = _sanitize_script(updated["task_js"])
        if "task_actions" in updated:
            updated["task_actions"] = _sanitize_actions(updated["task_actions"])

        def mutate(record: Dict[str, Any]) -> None:
            record.update(updated)
            record["updated_at"] = _utcnow()

        return self._update_record("projects", project_id, mutate)

    def delete_project(self, project_id: str) -> None:
        active_runs = self.list_active_runs(project_id=project_id)
        if active_runs:
            raise ValueError("Cannot delete a project while it has active runs.")
        pages = [
            page["id"]
            for page in self._consistent_scope(
                "pages", key="project_id", value=project_id
            )
        ]
        tasks = [
            task["id"]
            for task in self._consistent_scope(
                "tasks", key="project_id", value=project_id
            )
        ]
        batches = [
            batch["id"]
            for batch in self._consistent_scope(
                "batches", key="project_id", value=project_id
            )
        ]
        run_records = self._consistent_runs(project_id=project_id)
        execution_records = {
            str(run["id"]): self._run_execution_records(str(run["id"]))
            for run in run_records
        }
        for run in run_records:
            self._assert_run_deletable(run, execution_records[str(run["id"])])
        baseline_records = self._consistent_scope(
            "baselines", key="project_id", value=project_id
        )
        store = get_artifact_store()
        for run in run_records:
            store.purge_run(
                str(run["id"]),
                _artifact_records(execution_records[str(run["id"])]),
            )
        for baseline in baseline_records:
            store.purge_baseline(
                str(baseline["id"]),
                _artifact_records(baseline),
            )
        self._storage.bulk_delete(
            "executions",
            [
                execution["id"]
                for records in execution_records.values()
                for execution in records
            ],
        )
        self._storage.bulk_delete("runs", [run["id"] for run in run_records])
        self._storage.bulk_delete(
            "baselines", [baseline["id"] for baseline in baseline_records]
        )
        for collection, ids in [("pages", pages), ("tasks", tasks), ("batches", batches)]:
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
        updates = {}
        for key, value in payload.items():
            if key in {"url", "reference_url"}:
                updates[key] = str(value) if value is not None else None
            else:
                if key in {"basic_auth_username", "basic_auth_password"} and not value:
                    updates[key] = None
                elif key == "preparatory_js":
                    updates[key] = _sanitize_script(value)
                elif key == "preparatory_actions":
                    updates[key] = _sanitize_actions(value)
                else:
                    updates[key] = value
        def mutate(record: Dict[str, Any]) -> None:
            record.update(updates)
            record["updated_at"] = _utcnow()

        return self._update_record("pages", page_id, mutate)

    def delete_page(self, page_id: str) -> None:
        # Configuration deletion must not erase execution or baseline history.
        task_ids = [
            str(task["id"])
            for task in self._storage.list("tasks")
            if task.get("page_id") == page_id
        ]
        for task_id in task_ids:
            self.delete_task(task_id)
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
        def mutate(record: Dict[str, Any]) -> None:
            for key, value in payload.items():
                if key == "task_js":
                    record[key] = _sanitize_script(value)
                elif key == "task_actions":
                    record[key] = _sanitize_actions(value)
                else:
                    record[key] = value
            record["updated_at"] = _utcnow()

        return self._update_record("tasks", task_id, mutate)

    def delete_task(self, task_id: str) -> None:
        # Batches are mutable configuration. Runs, executions, baselines, and
        # their artifact descriptors are historical records and remain intact.
        for batch in self._storage.list("batches"):
            if task_id not in batch.get("task_ids", []):
                continue

            def remove_reference(record: Dict[str, Any]) -> None:
                record["task_ids"] = [
                    existing_id
                    for existing_id in record.get("task_ids", [])
                    if existing_id != task_id
                ]
                record["updated_at"] = _utcnow()

            self._update_record("batches", str(batch["id"]), remove_reference)
        self._storage.delete("tasks", task_id)

    # -- Batches ------------------------------------------------------------------
    def list_batches(self, project_id: str) -> List[Dict[str, Any]]:
        return sorted(
            [
                batch
                for batch in (
                    _with_spm_ticket(item)
                    for item in self._storage.filter("batches", key="project_id", value=project_id)
                )
                if batch is not None
            ],
            key=lambda it: it["created_at"],
        )

    def get_batch(self, batch_id: str) -> Optional[Dict[str, Any]]:
        return _with_spm_ticket(self._storage.get("batches", batch_id))

    def create_batch(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        now = _utcnow()
        batch_id = str(uuid.uuid4())
        spm_ticket = _spm_ticket_value(payload)
        record = {
            "id": batch_id,
            "project_id": payload["project_id"],
            "name": payload["name"],
            "description": payload.get("description"),
            "task_ids": payload.get("task_ids", []),
            "spm_ticket": spm_ticket,
            "jira_issue": spm_ticket,
            "run_diff_threshold": payload.get("run_diff_threshold"),
            "execution_diff_threshold": payload.get("execution_diff_threshold"),
            "created_at": now,
            "updated_at": now,
        }
        return self._storage.upsert("batches", batch_id, record)

    def update_batch(self, batch_id: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        threshold_keys = {"run_diff_threshold", "execution_diff_threshold"}

        def mutate(record: Dict[str, Any]) -> None:
            for key in threshold_keys:
                if key in payload:
                    record[key] = payload[key]
            for key, value in payload.items():
                if key not in threshold_keys and key not in {"spm_ticket", "jira_issue"}:
                    record[key] = value
            _apply_spm_ticket_update(record, payload)
            record["updated_at"] = _utcnow()

        return self._update_record("batches", batch_id, mutate)

    def delete_batch(self, batch_id: str) -> None:
        active_runs = self.list_active_runs(batch_id=batch_id)
        if active_runs:
            raise ValueError("Cannot delete a batch while it has active runs.")
        run_records = self._consistent_runs(batch_id=batch_id)
        execution_records = {
            str(run["id"]): self._run_execution_records(str(run["id"]))
            for run in run_records
        }
        for run in run_records:
            self._assert_run_deletable(run, execution_records[str(run["id"])])
        runs = [run["id"] for run in run_records]
        baseline_records = self._consistent_scope(
            "baselines", key="batch_id", value=batch_id
        )
        store = get_artifact_store()
        for run in run_records:
            store.purge_run(
                str(run["id"]),
                _artifact_records(execution_records[str(run["id"])]),
            )
        for baseline in baseline_records:
            store.purge_baseline(
                str(baseline["id"]),
                _artifact_records(baseline),
            )
        self._storage.bulk_delete(
            "executions",
            [
                execution["id"]
                for records in execution_records.values()
                for execution in records
            ],
        )
        self._storage.bulk_delete("runs", runs)
        self._storage.bulk_delete(
            "baselines", [baseline["id"] for baseline in baseline_records]
        )
        self._storage.delete("batches", batch_id)

    # -- Baselines ----------------------------------------------------------------
    def list_baselines(
        self,
        *,
        project_id: Optional[str] = None,
        batch_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        if batch_id:
            items = self._storage.filter("baselines", key="batch_id", value=batch_id)
            if project_id:
                items = [it for it in items if it.get("project_id") == project_id]
        elif project_id:
            items = self._storage.filter("baselines", key="project_id", value=project_id)
        else:
            items = self._storage.list("baselines")
        return sorted(items, key=lambda it: it["created_at"], reverse=True)

    def get_baseline(self, baseline_id: str) -> Optional[Dict[str, Any]]:
        return self._storage.get("baselines", baseline_id)

    def create_baseline(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        now = _utcnow()
        run_id = payload.get("run_id")
        baseline_id = (
            str(
                uuid.uuid5(
                    BASELINE_IDEMPOTENCY_NAMESPACE,
                    f"{payload['project_id']}:{payload['batch_id']}:{run_id}",
                )
            )
            if run_id
            else str(uuid.uuid4())
        )
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
        try:
            return self._storage.upsert("baselines", baseline_id, record)
        except StorageConflictError:
            existing = self.get_baseline(baseline_id)
            if run_id and existing and existing.get("run_id") == run_id:
                return existing
            raise

    def update_baseline(self, baseline_id: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        def mutate(record: Dict[str, Any]) -> None:
            for key, value in payload.items():
                if value is None:
                    continue
                record[key] = value
            record["updated_at"] = _utcnow()

        return self._update_record("baselines", baseline_id, mutate)

    def append_baseline_item(self, baseline_id: str, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        def identity(value: Dict[str, Any]) -> Tuple[Any, Any, Any, Any]:
            viewport = value.get("viewport") or {}
            return (
                value.get("task_id"),
                value.get("browser"),
                viewport.get("width"),
                viewport.get("height"),
            )

        def mutate(record: Dict[str, Any]) -> None:
            items = list(record.get("items") or [])
            item_identity = identity(item)
            for index, existing in enumerate(items):
                if identity(existing) == item_identity:
                    items[index] = item
                    break
            else:
                items.append(item)
            record["items"] = items
            record["updated_at"] = _utcnow()

        return self._update_record("baselines", baseline_id, mutate)

    def delete_baseline(self, baseline_id: str) -> None:
        baseline = self.get_baseline(baseline_id)
        if not baseline:
            return
        linked_runs = [
            run
            for run in self._storage.list("runs")
            if str(run.get("baseline_id") or "") == str(baseline_id)
        ]
        if linked_runs:
            raise ValueError("Cannot delete a baseline while it is referenced by a run.")
        store = get_artifact_store()
        store.purge_baseline(baseline_id, _artifact_records(baseline))
        self._storage.delete("baselines", baseline_id)

    def get_latest_baseline(self, batch_id: str) -> Optional[Dict[str, Any]]:
        # Baseline selection affects comparison correctness, so it must not use
        # an eventually consistent batch index or an incomplete baseline.
        baselines = sorted(
            [
                baseline
                for baseline in self._storage.list("baselines")
                if baseline.get("batch_id") == batch_id
                and baseline.get("status") == "completed"
            ],
            key=lambda item: item["created_at"],
            reverse=True,
        )
        return baselines[0] if baselines else None

    # -- Runs ---------------------------------------------------------------------
    def list_runs(
        self,
        *,
        project_id: Optional[str] = None,
        batch_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        if batch_id:
            items = self._storage.filter("runs", key="batch_id", value=batch_id)
            if project_id:
                items = [it for it in items if it.get("project_id") == project_id]
        elif project_id:
            items = self._storage.filter("runs", key="project_id", value=project_id)
        else:
            items = self._storage.list("runs")
        normalized = [
            run
            for run in (_with_spm_ticket(item) for item in items)
            if run is not None
        ]
        return sorted(normalized, key=lambda it: it["created_at"], reverse=True)

    def list_active_runs(
        self,
        *,
        project_id: Optional[str] = None,
        batch_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        # Lifecycle guards cannot rely on eventually consistent DynamoDB GSIs.
        runs = [
            normalized
            for item in self._storage.list("runs")
            if (normalized := _with_spm_ticket(item)) is not None
            and (not project_id or item.get("project_id") == project_id)
            and (not batch_id or item.get("batch_id") == batch_id)
        ]
        return [run for run in runs if run.get("status") in {"queued", "executing"}]

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        return _with_spm_ticket(self._storage.get("runs", run_id))

    def create_run(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        now = _utcnow()
        idempotency_key = str(payload.get("idempotency_key") or "").strip() or None
        run_id = (
            str(
                uuid.uuid5(
                    RUN_IDEMPOTENCY_NAMESPACE,
                    f"{payload['project_id']}:{payload['batch_id']}:{idempotency_key}",
                )
            )
            if idempotency_key
            else str(uuid.uuid4())
        )
        spm_ticket = _spm_ticket_value(payload)
        if not spm_ticket and payload.get("batch_id"):
            batch = self.get_batch(payload["batch_id"])
            if batch:
                spm_ticket = batch.get("spm_ticket")
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
            "spm_ticket": spm_ticket,
            "jira_issue": spm_ticket,
            "created_at": now,
            "updated_at": now,
            "summary": summary,
            "timeout_seconds": timeout_seconds,
            "task_ids": list(payload["task_ids"]) if payload.get("task_ids") is not None else None,
            "idempotency_key": idempotency_key,
        }
        if idempotency_key:
            record["idempotency_fingerprint"] = _fingerprint(
                record,
                (
                    "project_id",
                    "batch_id",
                    "baseline_id",
                    "purpose",
                    "requested_by",
                    "note",
                    "spm_ticket",
                    "timeout_seconds",
                    "task_ids",
                ),
            )
        try:
            return self._storage.upsert("runs", run_id, record)
        except StorageConflictError:
            existing = self.get_run(run_id)
            if (
                idempotency_key
                and existing
                and existing.get("idempotency_key") == idempotency_key
                and existing.get("idempotency_fingerprint")
                == record.get("idempotency_fingerprint")
            ):
                return existing
            raise

    def update_run(self, run_id: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        def mutate(record: Dict[str, Any]) -> None:
            requested_status = payload.get("status")
            current_status = record.get("status")
            status_conflict = (
                current_status in TERMINAL_RUN_STATUSES
                and requested_status is not None
                and requested_status != current_status
            )
            record.update(
                {
                    k: v
                    for k, v in payload.items()
                    if v is not None
                    and k not in {"spm_ticket", "jira_issue"}
                    and not (status_conflict and k == "status")
                }
            )
            _apply_spm_ticket_update(record, payload)
            record["updated_at"] = _utcnow()

        return self._update_record("runs", run_id, mutate)

    def reopen_run_for_retry(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Explicitly reopen a failed or cancelled run before adding retry work."""

        reopened = False

        def mutate(record: Dict[str, Any]) -> Optional[bool]:
            nonlocal reopened
            reopened = False
            if record.get("status") not in {"failed", "cancelled"}:
                return False
            record["status"] = "executing"
            for field in (
                "cancellation_requested_at",
                "completed_at",
                "timeout_deadline",
                "timeout_deadline_iso",
                "timeout_reason",
                "timeout_notified",
            ):
                record.pop(field, None)
            record["updated_at"] = _utcnow()
            reopened = True
            return None

        result = self._update_record("runs", run_id, mutate)
        return result if reopened else None

    def delete_run(self, run_id: str, *, cascade_baseline: bool = False) -> None:
        run = self.get_run(run_id)
        if not run:
            return
        execution_records = self._run_execution_records(run_id)
        self._assert_run_deletable(run, execution_records)
        store = get_artifact_store()
        baseline_to_delete: Optional[Dict[str, Any]] = None
        if cascade_baseline and run.get("baseline_id"):
            baseline = self.get_baseline(str(run["baseline_id"]))
            if baseline:
                still_linked = any(
                    other.get("baseline_id") == baseline["id"]
                    and other.get("id") != run_id
                    for other in self._storage.list("runs")
                )
                if not still_linked:
                    baseline_to_delete = baseline
        store.purge_run(run_id, _artifact_records(execution_records))
        if baseline_to_delete:
            store.purge_baseline(
                str(baseline_to_delete["id"]),
                _artifact_records(baseline_to_delete),
            )
        executions = [execution["id"] for execution in execution_records]
        self._storage.bulk_delete("executions", executions)
        self._storage.delete("runs", run_id)
        if baseline_to_delete:
            self._storage.delete("baselines", str(baseline_to_delete["id"]))

    # -- Executions ----------------------------------------------------------------
    def list_executions(
        self,
        *,
        run_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        if run_id:
            # Execution lifecycle decisions must see newly-created records even
            # before DynamoDB's run GSI has converged.
            items = [
                item
                for item in self._storage.list("executions")
                if item.get("run_id") == run_id
            ]
        else:
            items = self._storage.list("executions")
        return sorted(
            items,
            key=lambda it: (
                it.get("sequence", 0) or 0,
                it["created_at"],
            ),
        )

    def get_execution(self, execution_id: str) -> Optional[Dict[str, Any]]:
        return self._storage.get("executions", execution_id)

    def list_execution_records_bounded(
        self,
        *,
        statuses: Optional[Iterable[str]] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        wanted = set(statuses or [])
        records: List[Dict[str, Any]] = []
        cursor: Optional[str] = None
        page_size = max(1, min(int(limit), 100))
        while len(records) < limit:
            page, cursor = self.query_page(
                "executions",
                limit=page_size,
                cursor=cursor,
            )
            for record in page:
                if not wanted or str(record.get("status")) in wanted:
                    records.append(record)
                    if len(records) >= limit:
                        break
            if not cursor:
                break
        return records

    def list_cleanup_pending_executions(self, *, limit: int = 100) -> List[Dict[str, Any]]:
        """Return terminal Kubernetes executions whose owned resources need deletion."""

        records: List[Dict[str, Any]] = []
        cursor: Optional[str] = None
        result_limit = max(1, int(limit))
        while len(records) < result_limit:
            page, cursor = self.query_page(
                "executions",
                limit=100,
                cursor=cursor,
            )
            for record in page:
                if (
                    str(record.get("status")) in TERMINAL_EXECUTION_STATUSES
                    and (
                        record.get("kubernetes_job_name")
                        or record.get("kubernetes_secret_name")
                    )
                    and not record.get("kubernetes_cleaned_at")
                ):
                    records.append(record)
                    if len(records) >= result_limit:
                        break
            if not cursor:
                break
        return records

    def list_pending_callback_finalizations(self, *, limit: int = 100) -> List[Dict[str, Any]]:
        """Return accepted callbacks, including terminal records left by a crash."""

        return [
            record
            for record in self._storage.list("executions")
            if record.get("callback_state") == "accepted"
        ][: max(1, int(limit))]

    def list_dispatchable_executions(self, *, limit: int = 25) -> List[Dict[str, Any]]:
        now = _utcnow()
        records: List[Dict[str, Any]] = []
        cursor: Optional[str] = None
        result_limit = max(0, int(limit))
        if result_limit == 0:
            return records
        while len(records) < result_limit:
            page, cursor = self.query_page(
                "executions",
                limit=100,
                cursor=cursor,
            )
            for record in page:
                if (
                    record.get("status") == "queued"
                    and not record.get("cancellation_requested_at")
                    and (
                        not record.get("dispatch_claim_owner")
                        or str(record.get("dispatch_lease_expires_at") or "") <= now
                    )
                ):
                    records.append(record)
                    if len(records) >= result_limit:
                        break
            if not cursor:
                break
        return records

    def claim_execution(
        self,
        execution_id: str,
        *,
        owner: str,
        lease_seconds: int = 60,
    ) -> Optional[Dict[str, Any]]:
        now = _utcnow()
        claimed = False

        def mutate(record: Dict[str, Any]) -> Optional[bool]:
            nonlocal claimed
            claimed = False
            if record.get("status") != "queued" or record.get("cancellation_requested_at"):
                return False
            current_owner = str(record.get("dispatch_claim_owner") or "")
            expires_at = str(record.get("dispatch_lease_expires_at") or "")
            if current_owner and current_owner != owner and expires_at > now:
                return False
            resumable = bool(
                record.get("kubernetes_job_name")
                and record.get("kubernetes_secret_name")
                and record.get("callback_token_sha256")
                and record.get("runner_spec_digest")
            )
            generation = int(record.get("dispatch_generation") or 0)
            if not resumable:
                generation += 1
            suffix = str(execution_id).replace("-", "")[:20]
            record.update(
                {
                    "dispatch_claim_owner": owner,
                    "dispatch_generation": generation,
                    "dispatch_claimed_at": now,
                    "dispatch_lease_expires_at": _future_iso(lease_seconds),
                    "kubernetes_job_name": record.get("kubernetes_job_name")
                    if resumable
                    else f"scene-exec-{suffix}-g{generation}",
                    "kubernetes_secret_name": record.get("kubernetes_secret_name")
                    if resumable
                    else f"scene-exec-{suffix}-g{generation}",
                }
            )
            claimed = True
            return None

        result = self._update_record("executions", execution_id, mutate)
        return result if claimed else None

    def renew_execution_claim(
        self,
        execution_id: str,
        *,
        owner: str,
        generation: int,
        lease_seconds: int = 60,
    ) -> bool:
        renewed = False

        def mutate(record: Dict[str, Any]) -> Optional[bool]:
            nonlocal renewed
            renewed = False
            if (
                record.get("dispatch_claim_owner") != owner
                or int(record.get("dispatch_generation") or 0) != int(generation)
                or record.get("status") in TERMINAL_EXECUTION_STATUSES
            ):
                return False
            record["dispatch_lease_expires_at"] = _future_iso(lease_seconds)
            renewed = True
            return None

        self._update_record("executions", execution_id, mutate)
        return renewed

    def configure_execution_callback(
        self,
        execution_id: str,
        *,
        owner: str,
        generation: int,
        token_sha256: str,
        expires_at: str,
        artifact_transfer: Optional[Dict[str, Any]] = None,
        runner_spec_digest: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        configured = False

        def mutate(record: Dict[str, Any]) -> Optional[bool]:
            nonlocal configured
            configured = False
            if (
                record.get("dispatch_claim_owner") != owner
                or int(record.get("dispatch_generation") or 0) != int(generation)
                or record.get("status") != "queued"
                or record.get("cancellation_requested_at")
            ):
                return False
            record.update(
                {
                    "callback_token_sha256": token_sha256,
                    "callback_expires_at": expires_at,
                    "callback_state": "pending",
                }
            )
            if artifact_transfer is not None:
                record["artifact_transfer"] = artifact_transfer
            if runner_spec_digest:
                record["runner_spec_digest"] = runner_spec_digest
            configured = True
            return None

        result = self._update_record("executions", execution_id, mutate)
        return result if configured else None

    def mark_execution_dispatched(
        self,
        execution_id: str,
        *,
        owner: str,
        generation: int,
    ) -> Optional[Dict[str, Any]]:
        dispatched = False

        def mutate(record: Dict[str, Any]) -> Optional[bool]:
            nonlocal dispatched
            dispatched = False
            if (
                record.get("dispatch_claim_owner") != owner
                or int(record.get("dispatch_generation") or 0) != int(generation)
                or record.get("status") != "queued"
                or record.get("cancellation_requested_at")
            ):
                return False
            record.update(
                {
                    "status": "executing",
                    "started_at": record.get("started_at") or _utcnow(),
                    "dispatched_at": _utcnow(),
                    "dispatch_lease_expires_at": _future_iso(120),
                }
            )
            dispatched = True
            return None

        result = self._update_record("executions", execution_id, mutate)
        return result if dispatched else None

    def request_execution_cancellation(
        self,
        execution_id: str,
        *,
        reason: str,
    ) -> Optional[Dict[str, Any]]:
        requested = False

        def mutate(record: Dict[str, Any]) -> Optional[bool]:
            nonlocal requested
            requested = False
            if (
                record.get("status") in TERMINAL_EXECUTION_STATUSES
                or record.get("callback_state") in {"accepted", "finalized"}
            ):
                return False
            record["cancellation_requested_at"] = record.get("cancellation_requested_at") or _utcnow()
            record["message"] = reason
            requested = True
            return None

        result = self._update_record("executions", execution_id, mutate)
        return result if requested else None

    def finalize_execution_infrastructure_failure(
        self,
        execution_id: str,
        *,
        status: str,
        completed_at: str,
        message: str,
    ) -> Optional[Dict[str, Any]]:
        """Terminalize an execution unless a callback was already accepted."""

        if status not in {"failed", "cancelled"}:
            raise ValueError("Infrastructure failure status must be failed or cancelled.")
        finalized = False

        def mutate(record: Dict[str, Any]) -> Optional[bool]:
            nonlocal finalized
            finalized = False
            if (
                record.get("status") in TERMINAL_EXECUTION_STATUSES
                or record.get("callback_state") in {"accepted", "finalized"}
            ):
                return False
            record.update(
                {
                    "status": status,
                    "completed_at": completed_at,
                    "message": message,
                    "updated_at": _utcnow(),
                }
            )
            finalized = True
            return None

        result = self._update_record("executions", execution_id, mutate)
        return result if finalized else None

    def accept_execution_callback(
        self,
        execution_id: str,
        *,
        run_id: str,
        generation: int,
        token_sha256: str,
        result: Dict[str, Any],
    ) -> Tuple[str, Optional[Dict[str, Any]]]:
        canonical = json.dumps(result, sort_keys=True, separators=(",", ":"), default=str)
        result_digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        outcome = "invalid"

        def mutate(record: Dict[str, Any]) -> Optional[bool]:
            nonlocal outcome
            outcome = "invalid"
            if (
                str(record.get("run_id")) != str(run_id)
                or int(record.get("dispatch_generation") or 0) != int(generation)
                or not hmac.compare_digest(
                    str(record.get("callback_token_sha256") or ""),
                    str(token_sha256),
                )
            ):
                return False
            existing_digest = str(record.get("callback_result_digest") or "")
            if existing_digest:
                outcome = "duplicate" if hmac.compare_digest(existing_digest, result_digest) else "conflict"
                return False
            if (
                str(record.get("callback_expires_at") or "") < _utcnow()
                or record.get("cancellation_requested_at")
                or record.get("status") in TERMINAL_EXECUTION_STATUSES
            ):
                return False
            record.update(
                {
                    "callback_state": "accepted",
                    "callback_received_at": _utcnow(),
                    "callback_result_digest": result_digest,
                    "callback_result": result,
                }
            )
            outcome = "accepted"
            return None

        saved = self._update_record("executions", execution_id, mutate)
        return outcome, saved

    def recover_execution_callback(
        self,
        execution_id: str,
        *,
        generation: int,
        result: Dict[str, Any],
    ) -> Tuple[str, Optional[Dict[str, Any]]]:
        canonical = json.dumps(result, sort_keys=True, separators=(",", ":"), default=str)
        result_digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        outcome = "invalid"

        def mutate(record: Dict[str, Any]) -> Optional[bool]:
            nonlocal outcome
            outcome = "invalid"
            if int(record.get("dispatch_generation") or 0) != int(generation):
                return False
            existing_digest = str(record.get("callback_result_digest") or "")
            if existing_digest:
                outcome = "duplicate" if hmac.compare_digest(existing_digest, result_digest) else "conflict"
                return False
            if (
                record.get("cancellation_requested_at")
                or record.get("status") in TERMINAL_EXECUTION_STATUSES
            ):
                return False
            record.update(
                {
                    "callback_state": "accepted",
                    "callback_received_at": _utcnow(),
                    "callback_result_digest": result_digest,
                    "callback_result": result,
                    "callback_recovered_from": "s3_result",
                }
            )
            outcome = "accepted"
            return None

        saved = self._update_record("executions", execution_id, mutate)
        return outcome, saved

    def reserve_execution_retry(
        self,
        execution_id: str,
    ) -> Tuple[str, Optional[Dict[str, Any]]]:
        """Atomically reserve the sole deterministic retry child for an execution."""

        outcome = "invalid"

        def mutate(record: Dict[str, Any]) -> Optional[bool]:
            nonlocal outcome
            outcome = "invalid"
            existing_id = str(record.get("superseded_by_execution_id") or "")
            if existing_id:
                outcome = "existing"
                return False
            if record.get("status") not in {"failed", "cancelled"}:
                return False
            if (
                record.get("kubernetes_job_name")
                or record.get("kubernetes_secret_name")
            ) and not record.get("kubernetes_cleaned_at"):
                outcome = "cleanup_pending"
                return False
            retry_key = f"retry:{execution_id}"
            retry_id = str(
                uuid.uuid5(
                    EXECUTION_IDEMPOTENCY_NAMESPACE,
                    f"{record['run_id']}:{retry_key}",
                )
            )
            record["superseded_by_execution_id"] = retry_id
            record["retry_reserved_at"] = _utcnow()
            outcome = "reserved"
            return None

        saved = self._update_record("executions", execution_id, mutate)
        if saved is None:
            saved = self.get_execution(execution_id)
        return outcome, saved

    def claim_callback_finalization(
        self,
        execution_id: str,
        *,
        owner: str,
        lease_seconds: int = 300,
    ) -> Optional[Dict[str, Any]]:
        """Claim or renew exclusive processing of an accepted callback."""

        now = _utcnow()
        claimed = False

        def mutate(record: Dict[str, Any]) -> Optional[bool]:
            nonlocal claimed
            claimed = False
            if record.get("callback_state") != "accepted":
                return False
            current_owner = str(record.get("callback_finalizer_owner") or "")
            expires_at = str(record.get("callback_finalizer_lease_expires_at") or "")
            if current_owner and current_owner != owner and expires_at > now:
                return False
            record.update(
                {
                    "callback_finalizer_owner": owner,
                    "callback_finalizer_lease_expires_at": _future_iso(lease_seconds),
                    "callback_finalization_started_at": record.get(
                        "callback_finalization_started_at"
                    )
                    or now,
                }
            )
            claimed = True
            return None

        result = self._update_record("executions", execution_id, mutate)
        return result if claimed else None

    def mark_callback_finalized(
        self,
        execution_id: str,
        digest: str,
        *,
        owner: str,
    ) -> bool:
        finalized = False

        def mutate(record: Dict[str, Any]) -> Optional[bool]:
            nonlocal finalized
            finalized = False
            if (
                record.get("callback_finalizer_owner") != owner
                or not hmac.compare_digest(
                    str(record.get("callback_result_digest") or ""),
                    str(digest),
                )
                or record.get("status") not in TERMINAL_EXECUTION_STATUSES
            ):
                return False
            if record.get("callback_state") == "finalized":
                finalized = True
                return False
            if record.get("callback_state") != "accepted":
                return False
            record["callback_state"] = "finalized"
            record["callback_finalized_at"] = _utcnow()
            record["callback_finalizer_lease_expires_at"] = _utcnow()
            finalized = True
            return None

        self._update_record("executions", execution_id, mutate)
        return finalized

    def create_execution(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        now = _utcnow()
        idempotency_key = str(payload.get("idempotency_key") or "").strip() or None
        execution_id = (
            str(
                uuid.uuid5(
                    EXECUTION_IDEMPOTENCY_NAMESPACE,
                    f"{payload['run_id']}:{idempotency_key}",
                )
            )
            if idempotency_key
            else str(uuid.uuid4())
        )
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
            "idempotency_key": idempotency_key,
            "configuration_snapshot": payload.get("configuration_snapshot"),
            "retry_of_execution_id": payload.get("retry_of_execution_id"),
        }
        if idempotency_key:
            record["idempotency_fingerprint"] = _fingerprint(
                record,
                (
                    "run_id",
                    "project_id",
                    "batch_id",
                    "task_id",
                    "page_id",
                    "browser",
                    "viewport",
                    "sequence",
                ),
            )
        try:
            return self._storage.upsert("executions", execution_id, record)
        except StorageConflictError:
            existing = self.get_execution(execution_id)
            if (
                idempotency_key
                and existing
                and existing.get("idempotency_key") == idempotency_key
                and existing.get("idempotency_fingerprint")
                == record.get("idempotency_fingerprint")
            ):
                return existing
            raise

    def update_execution(self, execution_id: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        def mutate(record: Dict[str, Any]) -> Optional[bool]:
            requested_status = payload.get("status")
            current_status = record.get("status")
            if (
                current_status in TERMINAL_EXECUTION_STATUSES
                and requested_status is not None
                and requested_status != current_status
            ):
                return False
            for key, value in payload.items():
                if value is None:
                    continue
                if key == "artifacts":
                    artifacts = record.get("artifacts", {}).copy()
                    artifacts.update(value)
                    record["artifacts"] = artifacts
                else:
                    record[key] = value
            record["updated_at"] = _utcnow()
            return None

        return self._update_record("executions", execution_id, mutate)

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
            if execution.get("superseded_by_execution_id"):
                continue
            status = execution.get("status", "queued")
            if status in counts:
                counts[status] += 1
        return counts


_repository: Optional[SceneRepository] = None


def _build_storage_backend() -> StorageBackend:
    backend_name = os.environ.get(SCENE_STATE_BACKEND_ENV, "json").strip().lower()
    if backend_name in {"json", "local"}:
        return LocalDynamoStorage(resolve_state_path())
    if backend_name != "dynamodb":
        raise RuntimeError(
            f"Unsupported SCENE_STATE_BACKEND '{backend_name}'. Use json or dynamodb."
        )
    table_name = os.environ.get(SCENE_DYNAMODB_TABLE_ENV, "").strip()
    if not table_name:
        raise RuntimeError(
            "SCENE_DYNAMODB_TABLE is required when SCENE_STATE_BACKEND=dynamodb"
        )
    from app.services.dynamodb_storage import DynamoDBStorage

    return DynamoDBStorage(
        table_name=table_name,
        region_name=os.environ.get(SCENE_AWS_REGION_ENV, DEFAULT_AWS_REGION),
        endpoint_url=os.environ.get(SCENE_DYNAMODB_ENDPOINT_URL_ENV) or None,
        default_config_factory=_default_config,
        config_override=_apply_config_env_overrides,
    )


def get_repository() -> SceneRepository:
    """FastAPI dependency to retrieve the singleton repository instance."""
    global _repository
    if _repository is None:
        _repository = SceneRepository(_build_storage_backend())
    return _repository


RepositoryDep = Depends(get_repository)
