from __future__ import annotations

from typing import Dict, List, Optional, Any

import asyncio
import hashlib
import json
import time

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from app.schemas import ExecutionStatus, RunPurpose, RunStatus, TaskExecution
from app.services.artifacts import get_artifact_store
from app.services.orchestrator import get_orchestrator
from app.services.storage import RepositoryDep, SceneRepository
from app.templating import templates

router = APIRouter(tags=["runs"])

ARTIFACT_ORDER = [
    "observed",
    "baseline",
    "reference",
    "diff",
    "heatmap",
    "trace",
    "video",
    "log",
]

RUN_STATUS_BADGES = {
    RunStatus.queued.value: "secondary",
    RunStatus.executing.value: "primary",
    RunStatus.finished.value: "success",
    RunStatus.failed.value: "danger",
    RunStatus.cancelled.value: "warning",
}

EXEC_STATUS_BADGES = {
    ExecutionStatus.queued.value: "secondary",
    ExecutionStatus.executing.value: "primary",
    ExecutionStatus.finished.value: "success",
    ExecutionStatus.failed.value: "danger",
    ExecutionStatus.cancelled.value: "warning",
}

DIFF_BADGES = {
    "no_diff": "success",
    "minor_diff": "warning",
    "major_diff": "warning",
}

DIFF_LABELS = {
    "no_diff": "No Diff",
    "minor_diff": "Minor Diff",
    "major_diff": "Major Diff",
}


def _hash_signature(value: Any) -> str:
    serialized = json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha1(serialized.encode("utf-8")).hexdigest()


def _runs_log_signature(runs: List[Dict[str, object]]) -> str:
    snapshot: List[Dict[str, object]] = []
    for item in runs:
        snapshot.append(
            {
                "id": item.get("id"),
                "project_id": item.get("project_id"),
                "batch_id": item.get("batch_id"),
                "status": item.get("status"),
                "updated_at": item.get("updated_at"),
                "counts": item.get("counts"),
                "summary": item.get("summary"),
                "diff_average": item.get("diff_average"),
                "diff_grade": item.get("diff_grade"),
                "diff_label": item.get("diff_label"),
                "diff_threshold": item.get("diff_threshold"),
                "timeout_reason": item.get("timeout_reason"),
                "timeout_notified": item.get("timeout_notified"),
            }
        )
    return _hash_signature(snapshot)


def _run_overlay_signature(
    *,
    run: Dict[str, object],
    baseline: Optional[Dict[str, object]],
    executions: List[Dict[str, object]],
    counts: Dict[str, int],
) -> str:
    snapshot = {
        "run": {
            "id": run.get("id"),
            "status": run.get("status"),
            "updated_at": run.get("updated_at"),
            "summary": run.get("summary"),
            "diff_average": run.get("diff_average"),
            "diff_maximum": run.get("diff_maximum"),
            "diff_grade": run.get("diff_grade"),
            "diff_label": run.get("diff_label"),
            "diff_threshold": run.get("diff_threshold"),
            "timeout_seconds": run.get("timeout_seconds"),
            "note": run.get("note"),
            "jira_issue": run.get("jira_issue"),
            "purpose": run.get("purpose"),
            "baseline_id": run.get("baseline_id"),
        },
        "baseline": {
            "id": baseline.get("id") if baseline else None,
            "status": baseline.get("status") if baseline else None,
            "updated_at": baseline.get("updated_at") if baseline else None,
        },
        "counts": counts,
        "executions": [
            {
                "id": execution.get("id"),
                "status": execution.get("status"),
                "diff_grade": execution.get("diff_grade"),
                "diff_level": execution.get("diff_level"),
                "started_at": execution.get("started_at"),
                "completed_at": execution.get("completed_at"),
                "message_excerpt": execution.get("message_excerpt"),
                "artifacts": sorted((execution.get("artifacts") or {}).keys()),
            }
            for execution in executions
        ],
    }
    return _hash_signature(snapshot)


def _format_baseline_label(baseline: Dict[str, object]) -> str:
    created_at = baseline.get("created_at") or "Unknown"
    status_raw = str(baseline.get("status") or "pending")
    status = status_raw.replace("_", " ").title()
    baseline_id = str(baseline.get("id") or "")
    short_id = baseline_id[:8] if baseline_id else ""
    parts = [created_at, status]
    if short_id:
        parts.append(short_id)
    return " · ".join(parts)


def _coerce_float(value: Optional[object]) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _classify_diff(level: float, threshold: Optional[float]) -> str:
    if level <= 0.0:
        return "no_diff"
    threshold_value = _coerce_float(threshold)
    if threshold_value <= 0.0:
        return "major_diff"
    return "major_diff" if level > threshold_value else "minor_diff"


def _annotate_run(
    repo: SceneRepository,
    run: Dict[str, object],
    project_lookup: Dict[str, Dict[str, object]],
    *,
    counts: Optional[Dict[str, int]] = None,
) -> Dict[str, object]:
    project = project_lookup.get(run["project_id"])
    run["project_name"] = project["name"] if project else run["project_id"]

    counts = counts or repo.execution_status_counts(run["id"])
    total = sum(counts.values())
    summary = run.get("summary") or {}
    summary.update(
        {
            "executions_total": total,
            "executions_finished": counts[ExecutionStatus.finished.value],
            "executions_failed": counts[ExecutionStatus.failed.value],
            "executions_cancelled": counts[ExecutionStatus.cancelled.value],
        }
    )
    run["summary"] = summary
    run["diff_average"] = _coerce_float(summary.get("diff_average"))
    run["diff_maximum"] = _coerce_float(summary.get("diff_maximum"))
    run["diff_samples"] = int(summary.get("diff_samples") or 0)
    run["counts"] = counts
    run["status_badge"] = RUN_STATUS_BADGES.get(run.get("status"), "secondary")
    return run


def _collect_runs(
    repo: SceneRepository,
    project_lookup: Dict[str, Dict[str, object]],
    *,
    filter_project_id: Optional[str] = None,
    filter_status: Optional[str] = None,
) -> List[Dict[str, object]]:
    raw_runs = repo.list_runs(project_id=filter_project_id) if filter_project_id else repo.list_runs()
    runs = [dict(run) for run in raw_runs]
    if filter_status:
        runs = [run for run in runs if run.get("status") == filter_status]

    for run in runs:
        counts = repo.execution_status_counts(run["id"])
        _annotate_run(repo, run, project_lookup, counts=counts)
        batch = repo.get_batch(run.get("batch_id")) if run.get("batch_id") else None
        threshold = None
        if batch and batch.get("run_diff_threshold") is not None:
            threshold = _coerce_float(batch.get("run_diff_threshold"))
        grade = _classify_diff(run.get("diff_average", 0.0), threshold)
        run["diff_grade"] = grade
        run["diff_badge"] = DIFF_BADGES.get(grade, "secondary")
        run["diff_label"] = DIFF_LABELS.get(grade, "Diff")
        run["diff_threshold"] = threshold
    return runs


def _build_runs_dashboard_context(
    repo: SceneRepository,
    *,
    filter_project_id: Optional[str] = None,
    filter_status: Optional[str] = None,
    selected_run_id: Optional[str] = None,
    launch_defaults: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    config = repo.get_config()
    projects = repo.list_projects()
    project_lookup = {proj["id"]: proj for proj in projects}
    project_batches: Dict[str, List[Dict[str, object]]] = {
        proj["id"]: repo.list_batches(proj["id"]) for proj in projects
    }

    batch_baselines: Dict[str, List[Dict[str, str]]] = {}
    for batches in project_batches.values():
        for batch in batches:
            baselines = repo.list_baselines(batch_id=batch["id"])
            batch_baselines[batch["id"]] = [
                {
                    "id": item["id"],
                    "label": _format_baseline_label(item),
                }
                for item in baselines
            ]

    project_options = [{"id": proj["id"], "name": proj["name"]} for proj in projects]
    batch_options_map: Dict[str, List[Dict[str, str]]] = {
        proj["id"]: [
            {
                "id": batch["id"],
                "name": batch["name"],
            }
            for batch in project_batches.get(proj["id"], [])
        ]
        for proj in projects
    }

    defaults_raw: Dict[str, object] = dict(launch_defaults or {})
    defaults: Dict[str, str] = {
        "project_id": str(defaults_raw.get("project_id") or ""),
        "batch_id": str(defaults_raw.get("batch_id") or ""),
        "purpose": str(defaults_raw.get("purpose") or RunPurpose.comparison.value),
        "baseline_id": str(defaults_raw.get("baseline_id") or ""),
        "baseline_input": str(defaults_raw.get("baseline_input") or defaults_raw.get("baseline_id") or ""),
        "requested_by": str(defaults_raw.get("requested_by") or ""),
        "jira_issue": str(defaults_raw.get("jira_issue") or ""),
        "note": str(defaults_raw.get("note") or ""),
        "timeout_seconds": str(
            defaults_raw.get("timeout_seconds") or config.get("run_timeout_seconds", 600)
        ),
    }

    first_project_id = project_options[0]["id"] if project_options else ""
    if not defaults["project_id"] or defaults["project_id"] not in batch_options_map:
        defaults["project_id"] = first_project_id

    project_batches_for_default = batch_options_map.get(defaults["project_id"], [])
    first_batch_id = project_batches_for_default[0]["id"] if project_batches_for_default else ""
    if (
        not defaults["batch_id"]
        or all(item["id"] != defaults["batch_id"] for item in project_batches_for_default)
    ):
        defaults["batch_id"] = first_batch_id

    launch_metadata = {
        "projects": project_options,
        "batches": batch_options_map,
        "baselines": batch_baselines,
        "defaults": {
            "project_id": defaults["project_id"],
            "batch_id": defaults["batch_id"],
            "baseline_id": defaults.get("baseline_id", ""),
            "baseline_input": defaults.get("baseline_input", ""),
            "purpose": defaults.get("purpose", RunPurpose.comparison.value),
        },
    }

    runs = _collect_runs(
        repo,
        project_lookup,
        filter_project_id=filter_project_id,
        filter_status=filter_status,
    )

    if selected_run_id:
        if not any(run["id"] == selected_run_id for run in runs):
            selected_run_id = None
    if not selected_run_id and runs:
        selected_run_id = runs[0]["id"]

    snapshot_hash = _runs_log_signature(runs)

    return {
        "runs": runs,
        "projects": projects,
        "project_batches": project_batches,
        "purposes": list(RunPurpose),
        "statuses": list(RunStatus),
        "filter_project_id": filter_project_id,
        "filter_status": filter_status,
        "selected_run_id": selected_run_id,
        "default_timeout_seconds": config.get("run_timeout_seconds", 600),
        "snapshot_hash": snapshot_hash,
        "batch_baselines": batch_baselines,
        "launch_defaults": defaults,
        "launch_metadata_json": json.dumps(launch_metadata, sort_keys=True),
    }


def _build_run_context(repo: SceneRepository, run_id: str) -> Dict[str, object]:
    record = repo.get_run(run_id)
    if not record:
        raise HTTPException(status_code=404, detail="Run not found")
    run = dict(record)

    project = repo.get_project(run["project_id"])
    batch = repo.get_batch(run["batch_id"]) if run.get("batch_id") else None
    baseline = repo.get_baseline(run["baseline_id"]) if run.get("baseline_id") else None
    counts = repo.execution_status_counts(run_id)

    project_lookup = {run["project_id"]: project} if project else {}
    _annotate_run(repo, run, project_lookup, counts=counts)

    execution_threshold_raw = batch.get("execution_diff_threshold") if batch else None
    run_threshold_raw = batch.get("run_diff_threshold") if batch else None
    execution_threshold_value = _coerce_float(execution_threshold_raw) if execution_threshold_raw is not None else None
    run_threshold_value = _coerce_float(run_threshold_raw) if run_threshold_raw is not None else None

    run_diff_average = _coerce_float(run.get("diff_average"))
    run_diff_maximum = _coerce_float(run.get("diff_maximum"))
    run_grade = _classify_diff(run_diff_average, run_threshold_value)
    run["diff_average"] = round(run_diff_average, 4)
    run["diff_maximum"] = round(run_diff_maximum, 4)
    run["diff_grade"] = run_grade
    run["diff_badge"] = DIFF_BADGES.get(run_grade, "secondary")
    run["diff_label"] = DIFF_LABELS.get(run_grade, "Diff")
    run["diff_threshold"] = run_threshold_value
    run["diff_samples"] = int(run.get("diff_samples") or run.get("summary", {}).get("diff_samples") or 0)

    executions = [dict(execution) for execution in repo.list_executions(run_id=run_id)]
    for execution in executions:
        execution["status_badge"] = EXEC_STATUS_BADGES.get(execution.get("status"), "secondary")
        execution["can_cancel"] = execution.get("status") in {
            ExecutionStatus.queued.value,
            ExecutionStatus.executing.value,
        }
        diff_level = execution.get("diff_level")
        if diff_level is None:
            diff_level = (execution.get("diff") or {}).get("percentage")
        diff_value = _coerce_float(diff_level)
        execution["diff_level"] = round(diff_value, 4)
        diff_grade = _classify_diff(diff_value, execution_threshold_value)
        execution["diff_grade"] = diff_grade
        execution["diff_badge"] = DIFF_BADGES.get(diff_grade, "secondary")
        execution["diff_label"] = DIFF_LABELS.get(diff_grade, "Diff")
        execution["diff_threshold"] = execution_threshold_value
        message = execution.get("message")
        if message:
            first_line = message.splitlines()[0].strip()
            execution["message_excerpt"] = first_line[:240]
        else:
            execution["message_excerpt"] = None

    can_cancel_run = run.get("status") in {
        RunStatus.queued.value,
        RunStatus.executing.value,
    }

    return {
        "run": run,
        "project": project,
        "batch": batch,
        "baseline": baseline,
        "executions": executions,
        "counts": counts,
        "can_cancel_run": can_cancel_run,
        "artifact_order": ARTIFACT_ORDER,
        "execution_diff_threshold": execution_threshold_value,
        "run_diff_threshold": run_threshold_value,
    }


@router.get("/runs", response_class=HTMLResponse)
async def runs_home(
    request: Request,
    filter_project_id: Optional[str] = None,
    filter_status: Optional[RunStatus] = None,
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    context = _build_runs_dashboard_context(
        repo,
        filter_project_id=filter_project_id,
        filter_status=filter_status.value if filter_status else None,
        selected_run_id=None,
    )
    context["request"] = request
    return templates.TemplateResponse("runs/index.html", context)


@router.get("/runs/fragment", response_class=HTMLResponse)
async def runs_fragment(
    request: Request,
    filter_project_id: Optional[str] = None,
    filter_status: Optional[str] = None,
    selected_run_id: Optional[str] = None,
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    context = _build_runs_dashboard_context(
        repo,
        filter_project_id=filter_project_id,
        filter_status=filter_status,
        selected_run_id=selected_run_id,
    )
    context["request"] = request
    return templates.TemplateResponse("runs/_dashboard.html", context)


@router.get("/runs/log", response_class=HTMLResponse)
async def runs_log(
    request: Request,
    filter_project_id: Optional[str] = None,
    filter_status: Optional[str] = None,
    selected_run_id: Optional[str] = None,
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    orchestrator = get_orchestrator()
    orchestrator.reconcile()
    projects = repo.list_projects()
    project_lookup = {proj["id"]: proj for proj in projects}
    runs = _collect_runs(
        repo,
        project_lookup,
        filter_project_id=filter_project_id,
        filter_status=filter_status,
    )
    timeout_toasts: List[str] = []
    for run in runs:
        reason = run.get("timeout_reason")
        notified = run.get("timeout_notified")
        if reason and not notified:
            project_label = run.get("project_name") or "Run"
            timeout_toasts.append(f"{project_label}: {reason}")
            repo.update_run(run["id"], {"timeout_notified": True})
            run["timeout_notified"] = True
    context = {
        "request": request,
        "runs": runs,
        "filter_project_id": filter_project_id,
        "filter_status": filter_status,
        "selected_run_id": selected_run_id,
        "timeout_toasts": timeout_toasts,
    }
    snapshot_hash = _runs_log_signature(runs)
    context["snapshot_hash"] = snapshot_hash
    response = templates.TemplateResponse("runs/_run_list.html", context)
    response.headers["X-Scene-Run-Hash"] = snapshot_hash
    return response


@router.post("/runs/launch", response_class=HTMLResponse)
async def launch_run(
    request: Request,
    project_id: str = Form(...),
    batch_id: str = Form(...),
    purpose: RunPurpose = Form(RunPurpose.comparison.value),
    baseline_id: Optional[str] = Form(None),
    note: Optional[str] = Form(None),
    jira_issue: Optional[str] = Form(None),
    requested_by: Optional[str] = Form(None),
    timeout_seconds: Optional[int] = Form(None),
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    if not repo.get_project(project_id):
        raise HTTPException(status_code=404, detail="Project not found")
    if not repo.get_batch(batch_id):
        raise HTTPException(status_code=404, detail="Batch not found")

    default_timeout = int(repo.get_config().get("run_timeout_seconds", 600))
    resolved_timeout = default_timeout
    if timeout_seconds is not None:
        try:
            resolved_timeout = int(timeout_seconds)
        except (TypeError, ValueError) as exc:  # noqa: PERF203
            raise HTTPException(status_code=400, detail="Timeout must be a number of seconds.") from exc
        if resolved_timeout <= 0:
            raise HTTPException(status_code=400, detail="Timeout must be a positive number of seconds.")

    baseline_input_raw = baseline_id or ""

    payload = {
        "project_id": project_id,
        "batch_id": batch_id,
        "purpose": purpose.value if isinstance(purpose, RunPurpose) else purpose,
        "requested_by": requested_by or "dashboard",
        "note": note,
        "jira_issue": jira_issue,
        "timeout_seconds": resolved_timeout,
    }
    if baseline_id:
        resolved_baseline = repo.get_baseline(baseline_id)
        if not resolved_baseline:
            all_baselines = repo.list_baselines(batch_id=batch_id) if batch_id else repo.list_baselines(project_id=project_id)
            matches = [item for item in all_baselines if item.get("id", "").startswith(baseline_id)]
            if not matches:
                raise HTTPException(status_code=404, detail="Baseline not found for provided id/prefix")
            if len(matches) > 1:
                raise HTTPException(status_code=400, detail="Baseline id prefix is ambiguous; provide the full id")
            resolved_baseline = matches[0]
        payload["baseline_id"] = resolved_baseline["id"]

    new_run = repo.create_run(payload)
    orchestrator = get_orchestrator()
    orchestrator.enqueue(new_run["id"])

    launch_defaults = {
        "project_id": project_id,
        "batch_id": batch_id,
        "purpose": payload["purpose"],
        "baseline_id": payload.get("baseline_id", ""),
        "baseline_input": baseline_input_raw,
        "requested_by": requested_by or "",
        "jira_issue": jira_issue or "",
        "note": note or "",
        "timeout_seconds": str(resolved_timeout),
    }

    context = _build_runs_dashboard_context(
        repo,
        filter_project_id=project_id,
        selected_run_id=new_run["id"],
        launch_defaults=launch_defaults,
    )
    context["request"] = request
    return templates.TemplateResponse("runs/_dashboard.html", context)


@router.get("/runs/{run_id}/overlay", response_class=HTMLResponse)
async def run_overlay(
    run_id: str,
    request: Request,
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    context = _build_run_context(repo, run_id)
    context["request"] = request
    overlay_hash = _run_overlay_signature(
        run=context["run"],
        baseline=context.get("baseline"),
        executions=context["executions"],
        counts=context["counts"],
    )
    context["overlay_hash"] = overlay_hash
    response = templates.TemplateResponse("runs/_run_detail.html", context)
    response.headers["X-Scene-Run-Hash"] = overlay_hash
    response.headers["X-Scene-Run-Id"] = context["run"]["id"]
    return response


@router.post("/runs/{run_id}/cancel", response_class=HTMLResponse)
async def cancel_run(
    run_id: str,
    request: Request,
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    run_record = repo.get_run(run_id)
    if not run_record:
        raise HTTPException(status_code=404, detail="Run not found")
    if run_record.get("status") == RunStatus.finished.value:
        raise HTTPException(status_code=400, detail="Finished runs cannot be cancelled.")
    orchestrator = get_orchestrator()
    orchestrator.cancel_run(run_id)
    context = _build_run_context(repo, run_id)
    context["request"] = request
    return templates.TemplateResponse("runs/_run_detail.html", context)


@router.post("/runs/{run_id}/executions/{execution_id}/cancel", response_class=HTMLResponse)
async def cancel_execution(
    run_id: str,
    execution_id: str,
    request: Request,
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    execution = repo.get_execution(execution_id)
    if not execution or execution.get("run_id") != run_id:
        raise HTTPException(status_code=404, detail="Execution not found")
    run_record = repo.get_run(run_id)
    if not run_record:
        raise HTTPException(status_code=404, detail="Run not found")
    if run_record.get("status") == RunStatus.finished.value:
        raise HTTPException(status_code=400, detail="Finished runs cannot be modified.")
    orchestrator = get_orchestrator()
    orchestrator.cancel_execution(execution_id)
    context = _build_run_context(repo, run_id)
    context["request"] = request
    return templates.TemplateResponse("runs/_run_detail.html", context)


@router.post("/runs/{run_id}/delete", response_class=HTMLResponse)
async def delete_run_entry(
    run_id: str,
    request: Request,
    filter_project_id: Optional[str] = Form(None),
    filter_status: Optional[str] = Form(None),
    selected_run_id: Optional[str] = Form(None),
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    run = repo.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    orchestrator = get_orchestrator()
    orchestrator.cancel_run(run_id)
    repo.delete_run(run_id, cascade_baseline=True)
    context = _build_runs_dashboard_context(
        repo,
        filter_project_id=filter_project_id,
        filter_status=filter_status,
        selected_run_id=selected_run_id if selected_run_id != run_id else None,
    )
    if context["selected_run_id"] is None and context["runs"]:
        context["selected_run_id"] = context["runs"][0]["id"]
    context["request"] = request
    return templates.TemplateResponse("runs/_dashboard.html", context)


@router.get("/runs/{run_id}/executions/{execution_id}/viewer", response_class=HTMLResponse)
async def execution_viewer(
    run_id: str,
    execution_id: str,
    request: Request,
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    run_context = _build_run_context(repo, run_id)
    execution = next((item for item in run_context["executions"] if item["id"] == execution_id), None)
    if not execution:
        raise HTTPException(status_code=404, detail="Execution not found")
    context = {
        "request": request,
        "run": run_context["run"],
        "execution": execution,
        "baseline": run_context["baseline"],
        "artifact_order": ARTIFACT_ORDER,
        "execution_diff_threshold": run_context.get("execution_diff_threshold"),
        "run_diff_threshold": run_context.get("run_diff_threshold"),
    }
    return templates.TemplateResponse("runs/_execution_viewer.html", context)


@router.get("/runs/{run_id}/executions/{execution_id}/log", response_class=HTMLResponse)
async def execution_log(
    run_id: str,
    execution_id: str,
    request: Request,
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    run = repo.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    execution = repo.get_execution(execution_id)
    if not execution or execution.get("run_id") != run_id:
        raise HTTPException(status_code=404, detail="Execution not found")
    artifacts = execution.get("artifacts", {}) or {}
    log_artifact = artifacts.get("log")
    log_text = "Log not available."
    log_length = 0
    artifact_path = None
    if log_artifact:
        store = get_artifact_store()
        artifact_path = store.root / log_artifact.get("path", "")
        if artifact_path.exists():
            log_text = artifact_path.read_text(encoding="utf-8", errors="replace")
            log_length = artifact_path.stat().st_size
        else:
            log_text = "Log file missing on disk."
    context = {
        "request": request,
        "log_text": log_text,
        "run_id": run_id,
        "execution_id": execution_id,
        "log_length": log_length,
    }
    return templates.TemplateResponse("runs/_execution_log.html", context)


@router.get("/runs/{run_id}/executions/{execution_id}/log/stream")
async def execution_log_stream(
    run_id: str,
    execution_id: str,
    since: int = 0,
    timeout: int = 25,
    repo: SceneRepository = RepositoryDep,
) -> JSONResponse:
    execution = repo.get_execution(execution_id)
    if not execution or execution.get("run_id") != run_id:
        raise HTTPException(status_code=404, detail="Execution not found")
    artifacts = execution.get("artifacts", {}) or {}
    log_artifact = artifacts.get("log")
    store = get_artifact_store()
    path = store.root / log_artifact.get("path", "") if log_artifact else None

    start = time.monotonic()
    last_size = since
    text = "Log not available."
    while True:
        exists = path is not None and path.exists()
        size = path.stat().st_size if exists else 0
        if size != last_size or time.monotonic() - start >= timeout:
            if exists:
                text = path.read_text(encoding="utf-8", errors="replace")
            else:
                text = "Log file missing on disk." if log_artifact else "Log not available."
            return JSONResponse({"text": text, "length": size, "exists": exists})
        await asyncio.sleep(1)


@router.post("/runs/{run_id}/executions/{execution_id}/retry", response_class=HTMLResponse)
async def retry_execution(
    run_id: str,
    execution_id: str,
    request: Request,
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    execution = repo.get_execution(execution_id)
    if not execution or execution.get("run_id") != run_id:
        raise HTTPException(status_code=404, detail="Execution not found")
    run_record = repo.get_run(run_id)
    if not run_record:
        raise HTTPException(status_code=404, detail="Run not found")
    if run_record.get("status") == RunStatus.finished.value:
        raise HTTPException(status_code=400, detail="Finished runs cannot be retried.")
    orchestrator = get_orchestrator()
    try:
        orchestrator.retry_execution(execution_id)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    context = _build_run_context(repo, run_id)
    context["request"] = request
    return templates.TemplateResponse("runs/_run_detail.html", context)
