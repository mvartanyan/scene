from __future__ import annotations

import asyncio
import copy
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, Iterable, Mapping, Optional, Tuple

from fastapi import APIRouter
from fastapi.responses import JSONResponse, Response
from starlette.concurrency import run_in_threadpool

from app.services.artifacts import get_artifact_store
from app.services.operational_metrics import (
    BACKEND_DURATION_BUCKETS,
    CALLBACK_OUTCOMES,
    EXECUTION_STATUSES,
    JOB_TERMINAL_REASONS,
    RUN_DURATION_BUCKETS,
    RUN_STATUSES,
    backend_metrics_snapshot,
    collect_repository_metrics,
    record_backend_operation,
)
from app.services.orchestrator import get_orchestrator
from app.services.storage import STATE_VERSION, SceneRepository, get_repository


router = APIRouter(tags=["operations"])

_PROCESS_STARTED_AT = time.monotonic()
_PROCESS_STARTED_UNIX = time.time()
_MAX_LEASE_SECONDS_METRIC = 86_400.0
_READINESS_CHECK_TIMEOUT_SECONDS = 4.0
_READINESS_CACHE_SECONDS = 1.0
_METRICS_COLLECTION_TIMEOUT_SECONDS = 2.0
_METRICS_CACHE_SECONDS = 5.0
_DISPATCHER_HEARTBEAT_MAX_AGE = timedelta(seconds=15)
_DISPATCHER_CAPABILITY_MAX_AGE = timedelta(seconds=45)
_WEBHOOK_HEARTBEAT_MAX_AGE = timedelta(seconds=15)

_readiness_loop: Optional[asyncio.AbstractEventLoop] = None
_readiness_refresh_task: Optional[asyncio.Task[Dict[str, object]]] = None
_readiness_refresh_timed_out = False
_readiness_snapshot: Optional[Dict[str, object]] = None
_readiness_snapshot_at = 0.0

_metrics_loop: Optional[asyncio.AbstractEventLoop] = None
_metrics_refresh_task: Optional[asyncio.Task[Dict[str, object]]] = None
_metrics_refresh_timed_out = False
_metrics_snapshot: Optional[Dict[str, object]] = None
_metrics_snapshot_at = 0.0


def _check_result(
    ok: bool,
    *,
    backend: Optional[str] = None,
    reason: Optional[str] = None,
    **details: object,
) -> Dict[str, object]:
    result: Dict[str, object] = {"ok": ok}
    if backend:
        result["backend"] = backend
    if reason:
        result["reason"] = reason
    result.update(details)
    return result


def _backend_name(value: object) -> Optional[str]:
    if not isinstance(value, Mapping):
        return None
    backend = value.get("backend")
    return str(backend) if backend else None


async def _probe(
    callback: Callable[[], object],
    *,
    default_backend: Optional[str] = None,
) -> Dict[str, object]:
    started_at = time.monotonic()
    try:
        result = await run_in_threadpool(callback)
    except Exception:  # noqa: BLE001 - readiness must report outages, not propagate them.
        record_backend_operation(
            backend=default_backend or "unknown",
            operation="readiness_probe",
            duration_seconds=time.monotonic() - started_at,
            success=False,
        )
        return _check_result(False, backend=default_backend, reason="probe_failed")

    backend = _backend_name(result) or default_backend
    ok = not isinstance(result, Mapping) or result.get("ok") is not False
    record_backend_operation(
        backend=backend or "unknown",
        operation="readiness_probe",
        duration_seconds=time.monotonic() - started_at,
        success=bool(ok),
    )
    return _check_result(bool(ok), backend=backend, reason=None if ok else "probe_failed")


async def _runner_check() -> Dict[str, object]:
    fallback_backend = os.environ.get("SCENE_RUNNER_BACKEND", "docker").strip().lower()
    started_at = time.monotonic()

    def runner_readiness() -> object:
        return get_orchestrator().deployment_readiness()

    try:
        report = await run_in_threadpool(runner_readiness)
        payload = report.as_dict() if hasattr(report, "as_dict") else report
    except Exception:  # noqa: BLE001 - readiness must remain available on runner failures.
        record_backend_operation(
            backend="kubernetes" if fallback_backend == "k3s" else fallback_backend,
            operation="readiness_probe",
            duration_seconds=time.monotonic() - started_at,
            success=False,
        )
        return _check_result(False, backend=fallback_backend, reason="probe_failed")

    if not isinstance(payload, Mapping):
        record_backend_operation(
            backend="kubernetes" if fallback_backend == "k3s" else fallback_backend,
            operation="readiness_probe",
            duration_seconds=time.monotonic() - started_at,
            success=False,
        )
        return _check_result(False, backend=fallback_backend, reason="invalid_report")

    config = payload.get("config")
    backend = fallback_backend
    if isinstance(config, Mapping) and config.get("backend"):
        backend = str(config["backend"])

    issue_codes = []
    issues = payload.get("issues")
    if isinstance(issues, (list, tuple)):
        for issue in issues:
            if isinstance(issue, Mapping) and issue.get("code"):
                issue_codes.append(str(issue["code"]))

    ok = payload.get("ok") is True
    record_backend_operation(
        backend="kubernetes" if backend == "k3s" else backend,
        operation="readiness_probe",
        duration_seconds=time.monotonic() - started_at,
        success=ok,
    )
    return _check_result(
        ok,
        backend=backend,
        reason=None if ok else "configuration_invalid",
        issues=issue_codes,
    )


def _parse_timestamp(value: object) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


async def _dispatcher_check(
    repo: Optional[SceneRepository],
    *,
    runner_backend: str,
) -> Dict[str, object]:
    if runner_backend != "k3s":
        return _check_result(True, required=False, lease_seconds_remaining=0.0)
    if repo is None:
        return _check_result(
            False,
            required=True,
            reason="state_backend_unavailable",
            lease_seconds_remaining=0.0,
        )

    try:
        status = await run_in_threadpool(repo.dispatcher_status)
    except Exception:  # noqa: BLE001 - readiness must report outages, not propagate them.
        return _check_result(
            False,
            required=True,
            reason="probe_failed",
            lease_seconds_remaining=0.0,
        )

    if not isinstance(status, Mapping):
        return _check_result(
            False,
            required=True,
            reason="heartbeat_missing",
            lease_seconds_remaining=0.0,
        )

    heartbeat_at = _parse_timestamp(status.get("heartbeat_at"))
    expires_at = _parse_timestamp(status.get("expires_at"))
    capabilities_checked_at = _parse_timestamp(status.get("capabilities_checked_at"))
    now = datetime.now(timezone.utc)
    heartbeat_valid = bool(
        status.get("owner")
        and heartbeat_at
        and expires_at
        and heartbeat_at <= expires_at
        and heartbeat_at <= now
        and heartbeat_at > now - _DISPATCHER_HEARTBEAT_MAX_AGE
        and expires_at > now
    )
    capabilities_valid = bool(
        status.get("capabilities_ok") is True
        and capabilities_checked_at
        and capabilities_checked_at <= now
        and capabilities_checked_at > now - _DISPATCHER_CAPABILITY_MAX_AGE
    )
    valid = heartbeat_valid and capabilities_valid
    remaining = 0.0
    if expires_at:
        remaining = max(0.0, min((expires_at - now).total_seconds(), _MAX_LEASE_SECONDS_METRIC))
    return _check_result(
        valid,
        required=True,
        reason=(
            None
            if valid
            else "permissions_unavailable"
            if heartbeat_valid
            else "heartbeat_stale"
        ),
        lease_seconds_remaining=round(remaining, 3),
    )


def _webhook_enabled() -> bool:
    return os.environ.get("SCENE_WEBHOOK_ENABLED", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


async def _webhook_check(
    repo: Optional[SceneRepository],
) -> Dict[str, object]:
    if not _webhook_enabled():
        return _check_result(
            True,
            required=False,
            configured=True,
            lease_seconds_remaining=0.0,
        )
    if repo is None:
        return _check_result(
            False,
            required=False,
            configured=False,
            reason="state_backend_unavailable",
            lease_seconds_remaining=0.0,
        )
    try:
        status = await run_in_threadpool(repo.webhook_worker_status)
    except Exception:  # noqa: BLE001 - readiness must remain available.
        return _check_result(
            False,
            required=False,
            configured=False,
            reason="probe_failed",
            lease_seconds_remaining=0.0,
        )
    if not isinstance(status, Mapping):
        return _check_result(
            False,
            required=False,
            configured=False,
            reason="heartbeat_missing",
            lease_seconds_remaining=0.0,
        )
    heartbeat_at = _parse_timestamp(status.get("heartbeat_at"))
    expires_at = _parse_timestamp(status.get("expires_at"))
    now = datetime.now(timezone.utc)
    heartbeat_valid = bool(
        status.get("owner")
        and heartbeat_at
        and expires_at
        and heartbeat_at <= expires_at
        and heartbeat_at <= now
        and heartbeat_at > now - _WEBHOOK_HEARTBEAT_MAX_AGE
        and expires_at > now
    )
    configured = bool(
        status.get("enabled") is True and status.get("configured_ok") is True
    )
    remaining = 0.0
    if expires_at:
        remaining = max(
            0.0,
            min((expires_at - now).total_seconds(), _MAX_LEASE_SECONDS_METRIC),
        )
    return _check_result(
        heartbeat_valid and configured,
        required=False,
        configured=configured,
        reason=(
            None
            if heartbeat_valid and configured
            else "configuration_invalid"
            if heartbeat_valid
            else "heartbeat_stale"
        ),
        lease_seconds_remaining=round(remaining, 3),
    )


def _configured_backends() -> Tuple[str, str, str]:
    state_backend = os.environ.get("SCENE_STATE_BACKEND", "json").strip().lower()
    artifact_backend = os.environ.get("SCENE_ARTIFACT_STORAGE", "filesystem").strip().lower()
    if artifact_backend == "object":
        artifact_backend = "s3"
    runner_backend = os.environ.get("SCENE_RUNNER_BACKEND", "docker").strip().lower()
    return state_backend, artifact_backend, runner_backend


def _unavailable_readiness(reason: str) -> Dict[str, object]:
    state_backend, artifact_backend, runner_backend = _configured_backends()
    dispatcher_required = runner_backend == "k3s"
    return {
        "status": "not_ready",
        "checks": {
            "state": _check_result(False, backend=state_backend, reason=reason),
            "artifacts": _check_result(False, backend=artifact_backend, reason=reason),
            "runner": _check_result(False, backend=runner_backend, reason=reason),
            "dispatcher": _check_result(
                not dispatcher_required,
                required=dispatcher_required,
                reason=reason if dispatcher_required else None,
                lease_seconds_remaining=0.0,
            ),
            "webhook": _check_result(
                not _webhook_enabled(),
                required=False,
                configured=not _webhook_enabled(),
                reason=reason if _webhook_enabled() else None,
                lease_seconds_remaining=0.0,
            ),
        },
    }


async def _state_check(
    repo: Optional[SceneRepository],
    *,
    state_backend: str,
) -> Tuple[Dict[str, object], Optional[SceneRepository]]:
    resolved_repo = repo
    if resolved_repo is None:
        try:
            resolved_repo = await run_in_threadpool(get_repository)
        except Exception:  # noqa: BLE001
            resolved_repo = None
    if resolved_repo is None:
        return (
            _check_result(False, backend=state_backend, reason="probe_failed"),
            None,
        )
    return (
        await _probe(resolved_repo.probe, default_backend=state_backend),
        resolved_repo,
    )


async def _collect_readiness_uncached(
    repo: Optional[SceneRepository] = None,
) -> Dict[str, object]:
    state_backend, artifact_backend, _runner_backend = _configured_backends()
    state_result, artifact_check, runner_check = await asyncio.gather(
        _state_check(repo, state_backend=state_backend),
        _probe(lambda: get_artifact_store().probe(), default_backend=artifact_backend),
        _runner_check(),
    )
    state_check, resolved_repo = state_result
    dispatcher_check = await _dispatcher_check(
        resolved_repo,
        runner_backend=str(runner_check.get("backend") or ""),
    )
    webhook_check = await _webhook_check(resolved_repo)
    checks = {
        "state": state_check,
        "artifacts": artifact_check,
        "runner": runner_check,
        "dispatcher": dispatcher_check,
        "webhook": webhook_check,
    }
    ready = all(
        bool(check["ok"])
        for check in checks.values()
        if check.get("required", True) is not False
    )
    return {
        "status": "ready" if ready else "not_ready",
        "checks": checks,
    }


def _ensure_readiness_loop() -> None:
    global _readiness_loop
    global _readiness_refresh_task
    global _readiness_refresh_timed_out
    global _readiness_snapshot
    global _readiness_snapshot_at

    loop = asyncio.get_running_loop()
    if _readiness_loop is loop:
        return
    _readiness_loop = loop
    _readiness_refresh_task = None
    _readiness_refresh_timed_out = False
    _readiness_snapshot = None
    _readiness_snapshot_at = 0.0


def _store_readiness_snapshot(report: Dict[str, object]) -> None:
    global _readiness_snapshot
    global _readiness_snapshot_at

    _readiness_snapshot = copy.deepcopy(report)
    _readiness_snapshot_at = time.monotonic()


def _finish_readiness_refresh(task: asyncio.Task[Dict[str, object]]) -> None:
    global _readiness_refresh_task
    global _readiness_refresh_timed_out

    if _readiness_refresh_task is not task:
        return
    if task.cancelled():
        _readiness_refresh_task = None
        _readiness_refresh_timed_out = False
        return
    try:
        report = task.result()
    except Exception:  # noqa: BLE001 - operations endpoints must fail closed.
        report = _unavailable_readiness("probe_failed")
    _store_readiness_snapshot(report)
    _readiness_refresh_task = None
    _readiness_refresh_timed_out = False


def _start_readiness_refresh(
    repo: Optional[SceneRepository],
) -> asyncio.Task[Dict[str, object]]:
    global _readiness_refresh_task
    global _readiness_refresh_timed_out

    task = asyncio.create_task(_collect_readiness_uncached(repo))
    _readiness_refresh_task = task
    _readiness_refresh_timed_out = False
    task.add_done_callback(_finish_readiness_refresh)
    return task


def _snapshot_for_metrics() -> Dict[str, object]:
    _ensure_readiness_loop()
    task = _readiness_refresh_task
    if task is not None and task.done():
        _finish_readiness_refresh(task)
    if _readiness_snapshot is None:
        return _unavailable_readiness("not_checked")
    return copy.deepcopy(_readiness_snapshot)


async def collect_readiness(repo: Optional[SceneRepository] = None) -> Dict[str, object]:
    global _readiness_refresh_timed_out

    _ensure_readiness_loop()
    task = _readiness_refresh_task
    if task is not None and task.done():
        _finish_readiness_refresh(task)
        task = None

    if task is not None:
        if _readiness_refresh_timed_out:
            return _snapshot_for_metrics()
    elif (
        _readiness_snapshot is not None
        and time.monotonic() - _readiness_snapshot_at < _READINESS_CACHE_SECONDS
    ):
        return copy.deepcopy(_readiness_snapshot)
    else:
        task = _start_readiness_refresh(repo)

    try:
        return copy.deepcopy(
            await asyncio.wait_for(
                asyncio.shield(task),
                timeout=_READINESS_CHECK_TIMEOUT_SECONDS,
            )
        )
    except asyncio.TimeoutError:
        if task.done():
            _finish_readiness_refresh(task)
            return _snapshot_for_metrics()
        report = _unavailable_readiness("probe_failed")
        if _readiness_refresh_task is task:
            _readiness_refresh_timed_out = True
            _store_readiness_snapshot(report)
        return report
    except Exception:  # noqa: BLE001 - operations endpoints must fail closed.
        report = _unavailable_readiness("probe_failed")
        if _readiness_refresh_task is task:
            _store_readiness_snapshot(report)
        return report


def _unavailable_metrics(reason: str) -> Dict[str, object]:
    empty_statuses = {status: 0 for status in RUN_STATUSES}
    return {
        "available": False,
        "stale": False,
        "reason": reason,
        "record_limit": 0,
        "collection_duration_seconds": 0.0,
        "runs": {
            "total": 0,
            "statuses": dict(empty_statuses),
            "truncated": False,
            "durations": [],
        },
        "executions": {
            "total": 0,
            "statuses": dict(empty_statuses),
            "truncated": False,
            "durations": [],
        },
        "baselines": {"total": 0, "truncated": False},
        "queue": {"depth": 0, "oldest_age_seconds": 0.0, "truncated": False},
        "artifacts": {"count": 0, "size_bytes": 0, "truncated": False},
        "counters": {},
        "dispatcher": {},
        "webhook": {},
    }


async def _collect_metrics_uncached(
    repo: Optional[SceneRepository] = None,
) -> Dict[str, object]:
    resolved_repo = repo
    if resolved_repo is None:
        resolved_repo = await run_in_threadpool(get_repository)
    snapshot = await run_in_threadpool(collect_repository_metrics, resolved_repo)
    snapshot["available"] = True
    snapshot["stale"] = False
    return snapshot


def _ensure_metrics_loop() -> None:
    global _metrics_loop
    global _metrics_refresh_task
    global _metrics_refresh_timed_out
    global _metrics_snapshot
    global _metrics_snapshot_at

    loop = asyncio.get_running_loop()
    if _metrics_loop is loop:
        return
    _metrics_loop = loop
    _metrics_refresh_task = None
    _metrics_refresh_timed_out = False
    _metrics_snapshot = None
    _metrics_snapshot_at = 0.0


def _store_metrics_snapshot(report: Dict[str, object]) -> None:
    global _metrics_snapshot
    global _metrics_snapshot_at

    _metrics_snapshot = copy.deepcopy(report)
    _metrics_snapshot_at = time.monotonic()


def _finish_metrics_refresh(task: asyncio.Task[Dict[str, object]]) -> None:
    global _metrics_refresh_task
    global _metrics_refresh_timed_out

    if _metrics_refresh_task is not task:
        return
    if task.cancelled():
        _metrics_refresh_task = None
        _metrics_refresh_timed_out = False
        return
    try:
        report = task.result()
    except Exception:  # noqa: BLE001 - metrics collection must fail closed.
        report = _unavailable_metrics("collection_failed")
    _store_metrics_snapshot(report)
    _metrics_refresh_task = None
    _metrics_refresh_timed_out = False


def _start_metrics_refresh(
    repo: Optional[SceneRepository],
) -> asyncio.Task[Dict[str, object]]:
    global _metrics_refresh_task
    global _metrics_refresh_timed_out

    task = asyncio.create_task(_collect_metrics_uncached(repo))
    _metrics_refresh_task = task
    _metrics_refresh_timed_out = False
    task.add_done_callback(_finish_metrics_refresh)
    return task


def _operational_snapshot_for_metrics() -> Dict[str, object]:
    _ensure_metrics_loop()
    task = _metrics_refresh_task
    if task is not None and task.done():
        _finish_metrics_refresh(task)
    if _metrics_snapshot is None:
        return _unavailable_metrics("not_checked")
    return copy.deepcopy(_metrics_snapshot)


async def collect_operational_metrics(
    repo: Optional[SceneRepository] = None,
) -> Dict[str, object]:
    global _metrics_refresh_timed_out

    _ensure_metrics_loop()
    task = _metrics_refresh_task
    if task is not None and task.done():
        _finish_metrics_refresh(task)
        task = None
    if task is not None:
        if _metrics_refresh_timed_out:
            stale = _operational_snapshot_for_metrics()
            stale["stale"] = True
            return stale
    elif (
        _metrics_snapshot is not None
        and time.monotonic() - _metrics_snapshot_at < _METRICS_CACHE_SECONDS
    ):
        return copy.deepcopy(_metrics_snapshot)
    else:
        task = _start_metrics_refresh(repo)

    try:
        return copy.deepcopy(
            await asyncio.wait_for(
                asyncio.shield(task),
                timeout=_METRICS_COLLECTION_TIMEOUT_SECONDS,
            )
        )
    except asyncio.TimeoutError:
        if task.done():
            _finish_metrics_refresh(task)
            return _operational_snapshot_for_metrics()
        _metrics_refresh_timed_out = True
        stale = _operational_snapshot_for_metrics()
        stale["stale"] = True
        if _metrics_snapshot is None:
            stale["reason"] = "collection_timeout"
        return stale
    except Exception:  # noqa: BLE001 - metrics endpoint must remain available.
        report = _unavailable_metrics("collection_failed")
        if _metrics_refresh_task is task:
            _store_metrics_snapshot(report)
        return report


def _build_identity() -> Dict[str, object]:
    state_backend, artifact_backend, runner_backend = _configured_backends()
    return {
        "version": os.environ.get("SCENE_VERSION", "unknown"),
        "git_sha": os.environ.get("SCENE_GIT_SHA", "unknown"),
        "build_time": os.environ.get("SCENE_BUILD_TIME", "unknown"),
        "environment": os.environ.get("SCENE_ENV", "development"),
        "app_image": os.environ.get("SCENE_APP_IMAGE", "unknown"),
        "runner_image": os.environ.get("SCENE_RUNNER_IMAGE", "unknown"),
        "backends": {
            "state": state_backend,
            "artifacts": artifact_backend,
            "runner": runner_backend,
        },
        "state_schema_version": STATE_VERSION,
    }


def _escape_label(value: object) -> str:
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace("\n", "\\n")
        .replace('"', '\\"')
    )


def _metric(
    name: str,
    value: float,
    *,
    labels: Optional[Mapping[str, object]] = None,
) -> str:
    rendered_labels = ""
    if labels:
        rendered_labels = "{" + ",".join(
            f'{key}="{_escape_label(labels[key])}"' for key in sorted(labels)
        ) + "}"
    return f"{name}{rendered_labels} {float(value):g}"


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _histogram_lines(
    name: str,
    help_text: str,
    values: Iterable[object],
    *,
    buckets: Tuple[float, ...],
    labels: Optional[Mapping[str, object]] = None,
) -> list[str]:
    observations = [
        max(0.0, min(_number(value), 31_536_000.0))
        for value in values
    ]
    lines = [f"# HELP {name} {help_text}", f"# TYPE {name} histogram"]
    for upper_bound in buckets:
        bucket_labels = dict(labels or {})
        bucket_labels["le"] = f"{upper_bound:g}"
        lines.append(
            _metric(
                f"{name}_bucket",
                sum(1 for value in observations if value <= upper_bound),
                labels=bucket_labels,
            )
        )
    infinite_labels = dict(labels or {})
    infinite_labels["le"] = "+Inf"
    lines.extend(
        [
            _metric(f"{name}_bucket", len(observations), labels=infinite_labels),
            _metric(f"{name}_sum", sum(observations), labels=labels),
            _metric(f"{name}_count", len(observations), labels=labels),
        ]
    )
    return lines


def _render_backend_metrics(lines: list[str]) -> None:
    snapshot = backend_metrics_snapshot()
    counters = snapshot.get("counters")
    lines.extend(
        [
            (
                "# HELP scene_backend_operations_total Process-local backend "
                "operations by bounded backend, operation, and outcome."
            ),
            "# TYPE scene_backend_operations_total counter",
        ]
    )
    if isinstance(counters, Mapping):
        for key, value in sorted(counters.items(), key=lambda item: str(item[0])):
            if not isinstance(key, tuple) or len(key) != 3:
                continue
            backend, operation, outcome = key
            lines.append(
                _metric(
                    "scene_backend_operations_total",
                    _number(value),
                    labels={
                        "backend": backend,
                        "operation": operation,
                        "outcome": outcome,
                    },
                )
            )

    histograms = snapshot.get("histograms")
    lines.extend(
        [
            (
                "# HELP scene_backend_operation_duration_seconds Process-local "
                "backend operation latency."
            ),
            "# TYPE scene_backend_operation_duration_seconds histogram",
        ]
    )
    if not isinstance(histograms, Mapping):
        return
    for key, raw in sorted(histograms.items(), key=lambda item: str(item[0])):
        if not isinstance(key, tuple) or len(key) != 2 or not isinstance(raw, Mapping):
            continue
        backend, operation = key
        common_labels = {"backend": backend, "operation": operation}
        raw_buckets = raw.get("buckets")
        cumulative = list(raw_buckets) if isinstance(raw_buckets, list) else []
        for index, upper_bound in enumerate(BACKEND_DURATION_BUCKETS):
            labels = dict(common_labels)
            labels["le"] = f"{upper_bound:g}"
            value = cumulative[index] if index < len(cumulative) else 0
            lines.append(
                _metric(
                    "scene_backend_operation_duration_seconds_bucket",
                    _number(value),
                    labels=labels,
                )
            )
        labels = dict(common_labels)
        labels["le"] = "+Inf"
        lines.extend(
            [
                _metric(
                    "scene_backend_operation_duration_seconds_bucket",
                    _number(raw.get("count")),
                    labels=labels,
                ),
                _metric(
                    "scene_backend_operation_duration_seconds_sum",
                    _number(raw.get("sum")),
                    labels=common_labels,
                ),
                _metric(
                    "scene_backend_operation_duration_seconds_count",
                    _number(raw.get("count")),
                    labels=common_labels,
                ),
            ]
        )


def _render_metrics(
    readiness: Mapping[str, object],
    operational: Mapping[str, object],
) -> str:
    checks = readiness.get("checks")
    check_map = checks if isinstance(checks, Mapping) else {}
    identity = _build_identity()
    backends = _mapping(identity.get("backends"))
    lines = [
        "# HELP scene_process_up Whether the SCENE web process is running.",
        "# TYPE scene_process_up gauge",
        _metric("scene_process_up", 1.0),
        "# HELP scene_process_start_time_seconds Unix start time of this SCENE web process.",
        "# TYPE scene_process_start_time_seconds gauge",
        _metric("scene_process_start_time_seconds", _PROCESS_STARTED_UNIX),
        "# HELP scene_process_uptime_seconds Time since this SCENE web process started.",
        "# TYPE scene_process_uptime_seconds gauge",
        _metric(
            "scene_process_uptime_seconds",
            max(0.0, time.monotonic() - _PROCESS_STARTED_AT),
        ),
        "# HELP scene_build_info Immutable SCENE build and backend identity.",
        "# TYPE scene_build_info gauge",
        _metric(
            "scene_build_info",
            1.0,
            labels={
                "environment": identity.get("environment", "development"),
                "git_sha": identity.get("git_sha", "unknown"),
                "version": identity.get("version", "unknown"),
                "state_backend": backends.get("state", "unknown"),
                "artifact_backend": backends.get("artifacts", "unknown"),
                "runner_backend": backends.get("runner", "unknown"),
            },
        ),
        "# HELP scene_state_schema_info Active SCENE persisted-state schema version.",
        "# TYPE scene_state_schema_info gauge",
        _metric(
            "scene_state_schema_info",
            1.0,
            labels={"version": identity.get("state_schema_version", "unknown")},
        ),
        "# HELP scene_ready Whether all required SCENE dependencies are ready.",
        "# TYPE scene_ready gauge",
        _metric("scene_ready", 1.0 if readiness.get("status") == "ready" else 0.0),
        "# HELP scene_dependency_ready Whether a bounded SCENE dependency is ready.",
        "# TYPE scene_dependency_ready gauge",
    ]
    for dependency in ("state", "artifacts", "runner", "dispatcher", "webhook"):
        check = check_map.get(dependency)
        ok = isinstance(check, Mapping) and check.get("ok") is True
        lines.append(
            _metric(
                "scene_dependency_ready",
                1.0 if ok else 0.0,
                labels={"dependency": dependency},
            )
        )

    dispatcher = check_map.get("dispatcher")
    dispatcher_required = isinstance(dispatcher, Mapping) and dispatcher.get("required") is True
    remaining = (
        dispatcher.get("lease_seconds_remaining", 0.0)
        if isinstance(dispatcher, Mapping)
        else 0.0
    )
    try:
        remaining_value = max(0.0, min(float(remaining), _MAX_LEASE_SECONDS_METRIC))
    except (TypeError, ValueError):
        remaining_value = 0.0
    lines.extend(
        [
            (
                "# HELP scene_dispatcher_required Whether this process requires "
                "a dispatcher heartbeat."
            ),
            "# TYPE scene_dispatcher_required gauge",
            _metric("scene_dispatcher_required", 1.0 if dispatcher_required else 0.0),
            (
                "# HELP scene_dispatcher_lease_seconds_remaining Remaining "
                "dispatcher lease lifetime, capped at one day."
            ),
            "# TYPE scene_dispatcher_lease_seconds_remaining gauge",
            _metric("scene_dispatcher_lease_seconds_remaining", remaining_value),
        ]
    )

    runs = _mapping(operational.get("runs"))
    executions = _mapping(operational.get("executions"))
    baselines = _mapping(operational.get("baselines"))
    queue = _mapping(operational.get("queue"))
    artifacts = _mapping(operational.get("artifacts"))
    lines.extend(
        [
            "# HELP scene_metrics_collection_available Whether the bounded state snapshot succeeded.",
            "# TYPE scene_metrics_collection_available gauge",
            _metric(
                "scene_metrics_collection_available",
                1.0 if operational.get("available") is True else 0.0,
            ),
            "# HELP scene_metrics_collection_stale Whether a prior bounded state snapshot is being served.",
            "# TYPE scene_metrics_collection_stale gauge",
            _metric(
                "scene_metrics_collection_stale",
                1.0 if operational.get("stale") is True else 0.0,
            ),
            "# HELP scene_metrics_record_limit Maximum records inspected per retained collection.",
            "# TYPE scene_metrics_record_limit gauge",
            _metric("scene_metrics_record_limit", _number(operational.get("record_limit"))),
            "# HELP scene_metrics_collection_duration_seconds Duration of the latest state collection.",
            "# TYPE scene_metrics_collection_duration_seconds gauge",
            _metric(
                "scene_metrics_collection_duration_seconds",
                _number(operational.get("collection_duration_seconds")),
            ),
            "# HELP scene_metrics_collection_truncated Whether a retained collection exceeded the bounded window.",
            "# TYPE scene_metrics_collection_truncated gauge",
        ]
    )
    for collection, value in (
        ("runs", runs),
        ("executions", executions),
        ("baselines", baselines),
        ("artifacts", artifacts),
    ):
        lines.append(
            _metric(
                "scene_metrics_collection_truncated",
                1.0 if value.get("truncated") is True else 0.0,
                labels={"collection": collection},
            )
        )

    lines.extend(
        [
            "# HELP scene_runs Retained SCENE runs.",
            "# TYPE scene_runs gauge",
            _metric("scene_runs", _number(runs.get("total"))),
            "# HELP scene_run_status Retained runs by bounded status in the metrics window.",
            "# TYPE scene_run_status gauge",
        ]
    )
    run_statuses = _mapping(runs.get("statuses"))
    for status in RUN_STATUSES:
        lines.append(
            _metric(
                "scene_run_status",
                _number(run_statuses.get(status)),
                labels={"status": status},
            )
        )
    lines.extend(
        [
            "# HELP scene_executions Retained SCENE executions.",
            "# TYPE scene_executions gauge",
            _metric("scene_executions", _number(executions.get("total"))),
            "# HELP scene_execution_status Retained executions by bounded status in the metrics window.",
            "# TYPE scene_execution_status gauge",
        ]
    )
    execution_statuses = _mapping(executions.get("statuses"))
    for status in EXECUTION_STATUSES:
        lines.append(
            _metric(
                "scene_execution_status",
                _number(execution_statuses.get(status)),
                labels={"status": status},
            )
        )
    lines.extend(
        [
            "# HELP scene_baselines Retained SCENE baselines.",
            "# TYPE scene_baselines gauge",
            _metric("scene_baselines", _number(baselines.get("total"))),
            "# HELP scene_queue_depth Current queued execution depth in the bounded state snapshot.",
            "# TYPE scene_queue_depth gauge",
            _metric(
                "scene_queue_depth",
                _number(queue.get("depth")),
                labels={"kind": "execution"},
            ),
            "# HELP scene_queue_oldest_age_seconds Age of the oldest queued execution in the bounded state snapshot.",
            "# TYPE scene_queue_oldest_age_seconds gauge",
            _metric(
                "scene_queue_oldest_age_seconds",
                _number(queue.get("oldest_age_seconds")),
                labels={"kind": "execution"},
            ),
            "# HELP scene_artifacts Retained artifact records in the bounded state snapshot.",
            "# TYPE scene_artifacts gauge",
            _metric("scene_artifacts", _number(artifacts.get("count"))),
            "# HELP scene_artifact_bytes Retained artifact bytes recorded in the bounded state snapshot.",
            "# TYPE scene_artifact_bytes gauge",
            _metric("scene_artifact_bytes", _number(artifacts.get("size_bytes"))),
        ]
    )
    lines.extend(
        _histogram_lines(
            "scene_run_duration_seconds",
            "Duration of retained terminal runs in the bounded metrics window.",
            runs.get("durations") if isinstance(runs.get("durations"), list) else [],
            buckets=RUN_DURATION_BUCKETS,
        )
    )
    lines.extend(
        _histogram_lines(
            "scene_execution_duration_seconds",
            "Duration of retained terminal executions in the bounded metrics window.",
            (
                executions.get("durations")
                if isinstance(executions.get("durations"), list)
                else []
            ),
            buckets=RUN_DURATION_BUCKETS,
        )
    )

    counters = _mapping(operational.get("counters"))
    lines.extend(
        [
            "# HELP scene_callbacks_total Durable completion callback outcomes.",
            "# TYPE scene_callbacks_total counter",
        ]
    )
    for outcome in CALLBACK_OUTCOMES:
        lines.append(
            _metric(
                "scene_callbacks_total",
                _number(counters.get(f"callback_{outcome}_total")),
                labels={"outcome": outcome},
            )
        )

    dispatcher_metrics = _mapping(operational.get("dispatcher"))
    dispatcher_counters = _mapping(dispatcher_metrics.get("counters"))
    dispatcher_counter_metrics = (
        ("cycles_total", "scene_dispatcher_cycles_total"),
        ("cycle_failures_total", "scene_dispatcher_cycle_failures_total"),
        ("dispatch_total", "scene_dispatch_total"),
        ("reconcile_total", "scene_reconcile_total"),
        ("callbacks_finalized_total", "scene_callbacks_finalized_total"),
        ("cleanup_total", "scene_kubernetes_cleanup_total"),
    )
    for source, name in dispatcher_counter_metrics:
        lines.extend(
            [
                f"# HELP {name} Durable dispatcher counter {source}.",
                f"# TYPE {name} counter",
                _metric(name, _number(dispatcher_counters.get(source))),
            ]
        )
    webhook_metrics = _mapping(operational.get("webhook"))
    lines.extend(
        [
            "# HELP scene_runner_jobs_total Runner Job lifecycle transitions.",
            "# TYPE scene_runner_jobs_total counter",
        ]
    )
    for event, source in (
        ("created", "jobs_created_total"),
        ("adopted", "jobs_adopted_total"),
        ("scheduled", "jobs_scheduled_total"),
        ("started", "jobs_started_total"),
    ):
        lines.append(
            _metric(
                "scene_runner_jobs_total",
                _number(dispatcher_counters.get(source)),
                labels={"event": event},
            )
        )
    terminal_counts = _mapping(dispatcher_metrics.get("job_terminal"))
    lines.extend(
        [
            "# HELP scene_runner_job_terminal_total Terminal runner Jobs by bounded reason.",
            "# TYPE scene_runner_job_terminal_total counter",
        ]
    )
    for reason in JOB_TERMINAL_REASONS:
        lines.append(
            _metric(
                "scene_runner_job_terminal_total",
                _number(terminal_counts.get(reason)),
                labels={"reason": reason},
            )
        )
    lines.extend(
        [
            "# HELP scene_dispatcher_cycle_duration_seconds Duration of the latest dispatcher cycle.",
            "# TYPE scene_dispatcher_cycle_duration_seconds gauge",
            _metric(
                "scene_dispatcher_cycle_duration_seconds",
                _number(dispatcher_metrics.get("last_cycle_duration_seconds")),
            ),
            "# HELP scene_kubernetes_errors_total Dispatcher Kubernetes operation failures.",
            "# TYPE scene_kubernetes_errors_total counter",
            _metric(
                "scene_kubernetes_errors_total",
                _number(dispatcher_counters.get("kubernetes_errors_total")),
            ),
            "# HELP scene_webhook_worker_enabled Whether the SCENE-14 webhook worker is enabled.",
            "# TYPE scene_webhook_worker_enabled gauge",
            _metric(
                "scene_webhook_worker_enabled",
                1.0 if webhook_metrics.get("enabled") is True else 0.0,
            ),
            "# HELP scene_webhook_delivery_queue_depth Pending durable webhook deliveries.",
            "# TYPE scene_webhook_delivery_queue_depth gauge",
            _metric(
                "scene_webhook_delivery_queue_depth",
                _number(webhook_metrics.get("queue_depth")),
            ),
            "# HELP scene_webhook_delivery_oldest_age_seconds Age of the oldest pending webhook delivery.",
            "# TYPE scene_webhook_delivery_oldest_age_seconds gauge",
            _metric(
                "scene_webhook_delivery_oldest_age_seconds",
                _number(webhook_metrics.get("oldest_pending_age_seconds")),
            ),
            "# HELP scene_webhook_deliveries_total Durable webhook delivery outcomes.",
            "# TYPE scene_webhook_deliveries_total counter",
            _metric(
                "scene_webhook_deliveries_total",
                _number(counters.get("webhook_attempt_total")),
                labels={"outcome": "attempt"},
            ),
            _metric(
                "scene_webhook_deliveries_total",
                _number(counters.get("webhook_success_total")),
                labels={"outcome": "success"},
            ),
            _metric(
                "scene_webhook_deliveries_total",
                _number(counters.get("webhook_failure_total")),
                labels={"outcome": "failure"},
            ),
        ]
    )
    _render_backend_metrics(lines)
    return "\n".join(lines) + "\n"


@router.get("/healthz", include_in_schema=False)
async def healthz() -> JSONResponse:
    return JSONResponse(
        {"status": "ok"},
        headers={"Cache-Control": "no-store"},
    )


@router.get("/readyz", include_in_schema=False)
async def readyz() -> JSONResponse:
    report = await collect_readiness()
    return JSONResponse(
        report,
        status_code=200 if report["status"] == "ready" else 503,
        headers={"Cache-Control": "no-store"},
    )


@router.get("/version", include_in_schema=False)
async def version() -> JSONResponse:
    return JSONResponse(
        _build_identity(),
        headers={"Cache-Control": "no-store"},
    )


@router.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    readiness = _snapshot_for_metrics()
    operational = await collect_operational_metrics()
    return Response(
        _render_metrics(readiness, operational),
        media_type="text/plain; version=0.0.4",
        headers={"Cache-Control": "no-store"},
    )
