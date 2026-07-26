from __future__ import annotations

import os
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Dict, Iterable, Iterator, Mapping, Optional, Tuple


SCENE_METRICS_RECORD_LIMIT_ENV = "SCENE_METRICS_RECORD_LIMIT"
DEFAULT_METRICS_RECORD_LIMIT = 10_000
MAX_METRICS_RECORD_LIMIT = 50_000

RUN_STATUSES = ("queued", "executing", "finished", "failed", "cancelled", "unknown")
EXECUTION_STATUSES = RUN_STATUSES
CALLBACK_OUTCOMES = ("accepted", "duplicate", "conflict", "invalid")
JOB_TERMINAL_REASONS = (
    "succeeded",
    "failed",
    "deadline_exceeded",
    "image_pull",
    "oom_killed",
    "evicted",
    "unschedulable",
    "missing",
    "unknown",
)

BACKEND_DURATION_BUCKETS = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.0,
    4.0,
)
RUN_DURATION_BUCKETS = (
    1.0,
    5.0,
    15.0,
    30.0,
    60.0,
    120.0,
    300.0,
    600.0,
    1_800.0,
    3_600.0,
)

_ALLOWED_BACKENDS = {
    "dynamodb",
    "s3",
    "kubernetes",
    "json",
    "filesystem",
    "docker",
    "unknown",
    "other",
}
_ALLOWED_OPERATIONS = {
    "validate",
    "read",
    "write",
    "delete",
    "query",
    "count",
    "batch_delete",
    "upload",
    "head",
    "download",
    "presign_get",
    "presign_put",
    "list_versions",
    "delete_versions",
    "job_create",
    "job_delete",
    "job_status",
    "logs",
    "permissions",
    "readiness_probe",
    "other",
}


def _bounded_label(value: object, allowed: set[str]) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in allowed else "other"


class BackendOperationRegistry:
    """Process-local backend counters suitable for per-pod Prometheus scraping."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: Dict[Tuple[str, str, str], int] = {}
        self._histograms: Dict[Tuple[str, str], Dict[str, object]] = {}

    def observe(
        self,
        *,
        backend: str,
        operation: str,
        duration_seconds: float,
        success: bool,
    ) -> None:
        bounded_backend = _bounded_label(backend, _ALLOWED_BACKENDS)
        bounded_operation = _bounded_label(operation, _ALLOWED_OPERATIONS)
        outcome = "success" if success else "error"
        duration = max(0.0, min(float(duration_seconds), 86_400.0))
        with self._lock:
            counter_key = (bounded_backend, bounded_operation, outcome)
            self._counters[counter_key] = self._counters.get(counter_key, 0) + 1
            histogram_key = (bounded_backend, bounded_operation)
            histogram = self._histograms.setdefault(
                histogram_key,
                {
                    "count": 0,
                    "sum": 0.0,
                    "buckets": [0 for _bucket in BACKEND_DURATION_BUCKETS],
                },
            )
            histogram["count"] = int(histogram["count"]) + 1
            histogram["sum"] = float(histogram["sum"]) + duration
            buckets = histogram["buckets"]
            if isinstance(buckets, list):
                for index, upper_bound in enumerate(BACKEND_DURATION_BUCKETS):
                    if duration <= upper_bound:
                        buckets[index] = int(buckets[index]) + 1

    def snapshot(self) -> Dict[str, object]:
        with self._lock:
            return {
                "counters": dict(self._counters),
                "histograms": {
                    key: {
                        "count": int(value["count"]),
                        "sum": float(value["sum"]),
                        "buckets": list(value["buckets"]),
                    }
                    for key, value in self._histograms.items()
                },
            }

    def reset(self) -> None:
        with self._lock:
            self._counters.clear()
            self._histograms.clear()


_BACKEND_REGISTRY = BackendOperationRegistry()


@contextmanager
def observe_backend_operation(backend: str, operation: str) -> Iterator[None]:
    started_at = time.monotonic()
    success = False
    try:
        yield
        success = True
    finally:
        _BACKEND_REGISTRY.observe(
            backend=backend,
            operation=operation,
            duration_seconds=time.monotonic() - started_at,
            success=success,
        )


def record_backend_operation(
    *,
    backend: str,
    operation: str,
    duration_seconds: float,
    success: bool,
) -> None:
    _BACKEND_REGISTRY.observe(
        backend=backend,
        operation=operation,
        duration_seconds=duration_seconds,
        success=success,
    )


def backend_metrics_snapshot() -> Dict[str, object]:
    return _BACKEND_REGISTRY.snapshot()


def reset_backend_metrics_for_tests() -> None:
    _BACKEND_REGISTRY.reset()


def metrics_record_limit() -> int:
    raw = os.environ.get(SCENE_METRICS_RECORD_LIMIT_ENV)
    try:
        configured = int(raw) if raw else DEFAULT_METRICS_RECORD_LIMIT
    except ValueError:
        configured = DEFAULT_METRICS_RECORD_LIMIT
    return max(100, min(configured, MAX_METRICS_RECORD_LIMIT))


def _parse_timestamp(value: object) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _duration_seconds(
    record: Mapping[str, object],
    *,
    start_fields: Iterable[str],
    end_fields: Iterable[str],
) -> Optional[float]:
    started_at = next(
        (
            parsed
            for field in start_fields
            if (parsed := _parse_timestamp(record.get(field))) is not None
        ),
        None,
    )
    completed_at = next(
        (
            parsed
            for field in end_fields
            if (parsed := _parse_timestamp(record.get(field))) is not None
        ),
        None,
    )
    if started_at is None or completed_at is None or completed_at < started_at:
        return None
    return min((completed_at - started_at).total_seconds(), 31_536_000.0)


def _bounded_records(
    repo: object,
    collection: str,
    *,
    limit: int,
) -> Tuple[list[Dict[str, object]], int, bool]:
    records: list[Dict[str, object]] = []
    cursor: Optional[str] = None
    target = limit + 1
    while len(records) < target:
        page, cursor = repo.query_page(
            collection,
            limit=min(100, target - len(records)),
            cursor=cursor,
            descending=True,
        )
        records.extend(dict(record) for record in page)
        if not cursor:
            break
    truncated = len(records) > limit or cursor is not None
    return records[:limit], len(records), truncated


def _status_counts(
    records: Iterable[Mapping[str, object]],
    statuses: Tuple[str, ...],
) -> Dict[str, int]:
    counts = {status: 0 for status in statuses}
    known = set(statuses) - {"unknown"}
    for record in records:
        status = str(record.get("status") or "")
        counts[status if status in known else "unknown"] += 1
    return counts


def _artifact_totals(values: Iterable[object]) -> Tuple[int, int]:
    seen: set[Tuple[str, str, str]] = set()
    count = 0
    size_bytes = 0

    def visit(value: object) -> None:
        nonlocal count
        nonlocal size_bytes
        if isinstance(value, Mapping):
            path = str(value.get("key") or value.get("path") or "")
            if path:
                identity = (
                    str(value.get("storage") or ""),
                    path,
                    str(value.get("version_id") or ""),
                )
                if identity in seen:
                    return
                seen.add(identity)
                count += 1
                try:
                    size_bytes += max(0, int(value.get("size_bytes") or 0))
                except (TypeError, ValueError):
                    pass
                return
            for child in value.values():
                visit(child)
        elif isinstance(value, (list, tuple)):
            for child in value:
                visit(child)

    for item in values:
        visit(item)
    return count, size_bytes


def _safe_operational_counters(repo: object) -> Dict[str, int]:
    callback = getattr(repo, "operational_metrics", None)
    if not callable(callback):
        return {}
    record = callback() or {}
    raw_counters = record.get("counters") if isinstance(record, Mapping) else {}
    if not isinstance(raw_counters, Mapping):
        return {}
    counters: Dict[str, int] = {}
    for key, value in raw_counters.items():
        try:
            counters[str(key)] = max(0, int(value))
        except (TypeError, ValueError):
            continue
    return counters


def _safe_webhook_worker_status(repo: object) -> Dict[str, object]:
    callback = getattr(repo, "webhook_worker_status", None)
    if not callable(callback):
        return {}
    record = callback() or {}
    return dict(record) if isinstance(record, Mapping) else {}


def collect_repository_metrics(repo: object, *, limit: Optional[int] = None) -> Dict[str, object]:
    """Collect a bounded, cached-by-caller operational view of retained state."""

    started_at = time.monotonic()
    resolved_limit = metrics_record_limit() if limit is None else max(1, int(limit))
    runs, run_total, runs_truncated = _bounded_records(
        repo,
        "runs",
        limit=resolved_limit,
    )
    executions, execution_total, executions_truncated = _bounded_records(
        repo,
        "executions",
        limit=resolved_limit,
    )
    baselines, baseline_total, baselines_truncated = _bounded_records(
        repo,
        "baselines",
        limit=resolved_limit,
    )
    now = datetime.now(timezone.utc)
    queued = [
        record
        for record in executions
        if str(record.get("status") or "") == "queued"
    ]
    queued_ages = [
        max(0.0, (now - created_at).total_seconds())
        for record in queued
        if (created_at := _parse_timestamp(record.get("created_at"))) is not None
        and created_at <= now
    ]
    run_durations = [
        duration
        for record in runs
        if str(record.get("status") or "") in {"finished", "failed", "cancelled"}
        and (
            duration := _duration_seconds(
                record,
                start_fields=("started_at", "created_at"),
                end_fields=("completed_at", "updated_at"),
            )
        )
        is not None
    ]
    execution_durations = [
        duration
        for record in executions
        if str(record.get("status") or "") in {"finished", "failed", "cancelled"}
        and (
            duration := _duration_seconds(
                record,
                start_fields=("started_at", "created_at"),
                end_fields=("completed_at", "updated_at"),
            )
        )
        is not None
    ]
    artifact_count, artifact_bytes = _artifact_totals([*executions, *baselines])
    dispatcher_status = repo.dispatcher_status() or {}
    dispatcher_metrics = (
        dispatcher_status.get("metrics")
        if isinstance(dispatcher_status, Mapping)
        else {}
    )
    if not isinstance(dispatcher_metrics, Mapping):
        dispatcher_metrics = {}
    return {
        "record_limit": resolved_limit,
        "collection_duration_seconds": max(0.0, time.monotonic() - started_at),
        "runs": {
            "total": run_total,
            "statuses": _status_counts(runs, RUN_STATUSES),
            "truncated": runs_truncated,
            "durations": run_durations,
        },
        "executions": {
            "total": execution_total,
            "statuses": _status_counts(executions, EXECUTION_STATUSES),
            "truncated": executions_truncated,
            "durations": execution_durations,
        },
        "baselines": {
            "total": baseline_total,
            "truncated": baselines_truncated,
        },
        "queue": {
            "depth": len(queued),
            "oldest_age_seconds": max(queued_ages, default=0.0),
            "truncated": executions_truncated,
        },
        "artifacts": {
            "count": artifact_count,
            "size_bytes": artifact_bytes,
            "truncated": executions_truncated or baselines_truncated,
        },
        "counters": _safe_operational_counters(repo),
        "dispatcher": dict(dispatcher_metrics),
        "webhook": _safe_webhook_worker_status(repo),
    }
