from __future__ import annotations

import asyncio
import copy
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, Mapping, Optional, Tuple

from fastapi import APIRouter
from fastapi.responses import JSONResponse, Response
from starlette.concurrency import run_in_threadpool

from app.services.artifacts import get_artifact_store
from app.services.orchestrator import get_orchestrator
from app.services.storage import SceneRepository, get_repository


router = APIRouter(tags=["operations"])

_PROCESS_STARTED_AT = time.monotonic()
_MAX_LEASE_SECONDS_METRIC = 86_400.0
_READINESS_CHECK_TIMEOUT_SECONDS = 4.0
_READINESS_CACHE_SECONDS = 1.0
_DISPATCHER_HEARTBEAT_MAX_AGE = timedelta(seconds=15)
_DISPATCHER_CAPABILITY_MAX_AGE = timedelta(seconds=45)

_readiness_loop: Optional[asyncio.AbstractEventLoop] = None
_readiness_refresh_task: Optional[asyncio.Task[Dict[str, object]]] = None
_readiness_refresh_timed_out = False
_readiness_snapshot: Optional[Dict[str, object]] = None
_readiness_snapshot_at = 0.0


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
    try:
        result = await run_in_threadpool(callback)
    except Exception:  # noqa: BLE001 - readiness must report outages, not propagate them.
        return _check_result(False, backend=default_backend, reason="probe_failed")

    backend = _backend_name(result) or default_backend
    ok = not isinstance(result, Mapping) or result.get("ok") is not False
    return _check_result(bool(ok), backend=backend, reason=None if ok else "probe_failed")


async def _runner_check() -> Dict[str, object]:
    fallback_backend = os.environ.get("SCENE_RUNNER_BACKEND", "docker").strip().lower()

    def runner_readiness() -> object:
        return get_orchestrator().deployment_readiness()

    try:
        report = await run_in_threadpool(runner_readiness)
        payload = report.as_dict() if hasattr(report, "as_dict") else report
    except Exception:  # noqa: BLE001 - readiness must remain available on runner failures.
        return _check_result(False, backend=fallback_backend, reason="probe_failed")

    if not isinstance(payload, Mapping):
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
    checks = {
        "state": state_check,
        "artifacts": artifact_check,
        "runner": runner_check,
        "dispatcher": dispatcher_check,
    }
    ready = all(bool(check["ok"]) for check in checks.values())
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


def _build_identity() -> Dict[str, str]:
    return {
        "version": os.environ.get("SCENE_VERSION", "unknown"),
        "git_sha": os.environ.get("SCENE_GIT_SHA", "unknown"),
        "build_time": os.environ.get("SCENE_BUILD_TIME", "unknown"),
        "environment": os.environ.get("SCENE_ENV", "development"),
    }


def _metric(name: str, value: float, *, dependency: Optional[str] = None) -> str:
    labels = f'{{dependency="{dependency}"}}' if dependency else ""
    return f"{name}{labels} {value:g}"


def _render_metrics(readiness: Mapping[str, object]) -> str:
    checks = readiness.get("checks")
    check_map = checks if isinstance(checks, Mapping) else {}
    lines = [
        "# HELP scene_process_up Whether the SCENE web process is running.",
        "# TYPE scene_process_up gauge",
        _metric("scene_process_up", 1.0),
        "# HELP scene_process_uptime_seconds Time since this SCENE web process started.",
        "# TYPE scene_process_uptime_seconds gauge",
        _metric(
            "scene_process_uptime_seconds",
            max(0.0, time.monotonic() - _PROCESS_STARTED_AT),
        ),
        "# HELP scene_ready Whether all required SCENE dependencies are ready.",
        "# TYPE scene_ready gauge",
        _metric("scene_ready", 1.0 if readiness.get("status") == "ready" else 0.0),
        "# HELP scene_dependency_ready Whether a bounded SCENE dependency is ready.",
        "# TYPE scene_dependency_ready gauge",
    ]
    for dependency in ("state", "artifacts", "runner", "dispatcher"):
        check = check_map.get(dependency)
        ok = isinstance(check, Mapping) and check.get("ok") is True
        lines.append(
            _metric(
                "scene_dependency_ready",
                1.0 if ok else 0.0,
                dependency=dependency,
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
    report = _snapshot_for_metrics()
    return Response(
        _render_metrics(report),
        media_type="text/plain; version=0.0.4",
        headers={"Cache-Control": "no-store"},
    )
