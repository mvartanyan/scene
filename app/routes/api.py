from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import PlainTextResponse

from app.schemas import (
    AgentManifest,
    AgentManifestEndpoint,
    AgentSetupEntityResult,
    AgentSetupRequest,
    AgentSetupResponse,
    Batch,
    BatchComparisonRunCreate,
    BatchCreate,
    BatchUpdate,
    Baseline,
    BaselineOption,
    BaselineStatus,
    CheckCandidate,
    ExecutionCallbackRequest,
    ExecutionArtifactSet,
    ExecutionLog,
    ExecutionStatus,
    IntegrationRunResult,
    Page,
    PageCreate,
    PageUpdate,
    Project,
    ProjectCreate,
    ProjectUpdate,
    Run,
    RunCreate,
    RunArtifacts,
    RunDetail,
    RunFailureStatus,
    RunPurpose,
    RunUpdate,
    RunStatus,
    SceneConfig,
    SceneConfigUpdate,
    Task,
    TaskCreate,
    TaskUpdate,
    TaskExecution,
)
from app.services.agent_auth import SCENE_API_TOKEN_ENV, require_agent_api_token
from app.services.artifacts import get_artifact_store
from app.services.orchestrator import get_orchestrator
from app.services.storage import RepositoryDep, SceneRepository

router = APIRouter(prefix="/api", tags=["api"])

AGENT_DOCS_PATH = Path("docs/agent-api.md")

ARTIFACT_URL_PRIORITY = [
    "diff",
    "heatmap",
    "observed",
    "baseline",
    "reference",
    "trace",
    "video",
    "log",
]

AgentAuthDep = Depends(require_agent_api_token)


def _ensure_project(repo: SceneRepository, project_id: str) -> None:
    if not repo.get_project(project_id):
        raise HTTPException(status_code=404, detail="Project not found")


def _coerce_float(value: Optional[object]) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _absolute_url(request: Request, url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    if url.startswith(("http://", "https://")):
        return url
    return urljoin(str(request.base_url), url.lstrip("/"))


def _public_artifact_info(request: Request, artifact: Dict[str, object]) -> Dict[str, object]:
    payload = dict(artifact)
    url = payload.get("url")
    if not url and payload.get("path"):
        url = f"/artifacts/{payload['path']}"
    payload["url"] = _absolute_url(request, str(url)) if url else None
    return payload


def _apply_config_update(
    repo: SceneRepository,
    payload: SceneConfigUpdate,
    *,
    update_runtime: bool = True,
) -> Dict[str, object]:
    orchestrator = get_orchestrator() if update_runtime else None
    data = {
        key: value
        for key, value in payload.model_dump(exclude_unset=True).items()
        if value is not None
    }
    if "browsers" in data:
        repo.set_available_browsers(data["browsers"])
    if "viewports" in data:
        repo.set_available_viewports(data["viewports"])
    if "display_timezone" in data:
        repo.set_display_timezone(data["display_timezone"])
    if "run_timeout_seconds" in data:
        repo.set_run_timeout_seconds(data["run_timeout_seconds"])
    if "max_concurrent_executions" in data:
        repo.set_max_concurrent_executions(data["max_concurrent_executions"])
        if orchestrator:
            orchestrator.update_concurrency(data["max_concurrent_executions"])
    if "scene_host_url" in data:
        repo.set_scene_host_url(data["scene_host_url"])
        if orchestrator:
            orchestrator.update_scene_host(data["scene_host_url"])
    if "capture_post_wait_ms" in data:
        repo.set_capture_post_wait_ms(data["capture_post_wait_ms"])
        if orchestrator:
            orchestrator.update_capture_delay(data["capture_post_wait_ms"])
    if "diff_pixel_tolerance" in data:
        repo.set_diff_pixel_tolerance(data["diff_pixel_tolerance"])
        if orchestrator:
            orchestrator.update_diff_pixel_tolerance(data["diff_pixel_tolerance"])
    return repo.get_config()


def _find_by_field(
    records: List[Dict[str, object]], field: str, value: object
) -> Optional[Dict[str, object]]:
    for record in records:
        if record.get(field) == value:
            return record
    return None


def _entity_result(record: Dict[str, object], action: str) -> AgentSetupEntityResult:
    return AgentSetupEntityResult(
        id=str(record["id"]),
        name=str(record.get("name") or record["id"]),
        action=action,
    )


def _build_run_detail(repo: SceneRepository, run_id: str) -> RunDetail:
    run = repo.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    baseline = repo.get_baseline(str(run["baseline_id"])) if run.get("baseline_id") else None
    return RunDetail(
        run=run,
        project=repo.get_project(str(run["project_id"])),
        batch=repo.get_batch(str(run["batch_id"])) if run.get("batch_id") else None,
        baseline=baseline,
        executions=repo.list_executions(run_id=run_id),
        counts=repo.execution_status_counts(run_id),
    )


def _log_payload(
    repo: SceneRepository,
    request: Request,
    *,
    run_id: str,
    execution_id: str,
) -> ExecutionLog:
    run = repo.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    execution = repo.get_execution(execution_id)
    if not execution or execution.get("run_id") != run_id:
        raise HTTPException(status_code=404, detail="Execution not found")

    artifacts = execution.get("artifacts", {}) or {}
    log_artifact = artifacts.get("log")
    if not isinstance(log_artifact, dict):
        return ExecutionLog(
            run_id=run_id,
            execution_id=execution_id,
            exists=False,
            length=0,
            text="Log not available.",
        )

    store = get_artifact_store()
    relative_path = str(log_artifact.get("path") or "")
    artifact_path = store.root / relative_path
    exists = artifact_path.exists()
    if exists:
        text = artifact_path.read_text(encoding="utf-8", errors="replace")
        length = artifact_path.stat().st_size
    else:
        text = "Log file missing on disk."
        length = 0
    url = log_artifact.get("url") or f"/artifacts/{relative_path}"
    return ExecutionLog(
        run_id=run_id,
        execution_id=execution_id,
        exists=exists,
        length=length,
        text=text,
        artifact_path=relative_path,
        artifact_url=_absolute_url(request, str(url)),
    )


def _completed_baselines(repo: SceneRepository, batch_id: str) -> List[Dict[str, object]]:
    return [
        baseline
        for baseline in repo.list_baselines(batch_id=batch_id)
        if baseline.get("status") == BaselineStatus.completed.value
    ]


def _latest_completed_baseline(
    repo: SceneRepository, batch_id: str
) -> Optional[Dict[str, object]]:
    baselines = _completed_baselines(repo, batch_id)
    return baselines[0] if baselines else None


def _viewport_dimensions(value: object) -> Optional[Tuple[int, int]]:
    if not isinstance(value, dict):
        return None
    try:
        return int(value.get("width")), int(value.get("height"))
    except (TypeError, ValueError):
        return None


def _batch_requirement_keys(
    repo: SceneRepository, batch: Dict[str, object]
) -> List[Tuple[str, str, int, int]]:
    keys: List[Tuple[str, str, int, int]] = []
    for task_id in batch.get("task_ids") or []:
        task = repo.get_task(str(task_id))
        if not task:
            continue
        for browser in task.get("browsers") or []:
            if not isinstance(browser, str) or not browser:
                continue
            for viewport in task.get("viewports") or []:
                dimensions = _viewport_dimensions(viewport)
                if dimensions:
                    keys.append((str(task_id), browser, dimensions[0], dimensions[1]))
    return keys


def _baseline_coverage_keys(baseline: Dict[str, object]) -> set[Tuple[str, str, int, int]]:
    keys: set[Tuple[str, str, int, int]] = set()
    for item in baseline.get("items") or []:
        if not isinstance(item, dict):
            continue
        artifacts = item.get("artifacts")
        if not isinstance(artifacts, dict):
            continue
        baseline_artifact = artifacts.get("baseline")
        if not isinstance(baseline_artifact, dict) or not baseline_artifact.get("path"):
            continue
        browser = item.get("browser")
        task_id = item.get("task_id")
        dimensions = _viewport_dimensions(item.get("viewport"))
        if not isinstance(task_id, str) or not isinstance(browser, str) or not dimensions:
            continue
        keys.add((task_id, browser, dimensions[0], dimensions[1]))
    return keys


def _baseline_coverage_gaps(
    repo: SceneRepository,
    batch: Dict[str, object],
    baseline: Dict[str, object],
) -> List[str]:
    covered = _baseline_coverage_keys(baseline)
    gaps: List[str] = []
    for task_id, browser, width, height in _batch_requirement_keys(repo, batch):
        if (task_id, browser, width, height) in covered:
            continue
        task = repo.get_task(task_id)
        task_name = str(task.get("name") if task else task_id)
        gaps.append(f"{task_name} / {browser} / {width}x{height}")
    return gaps


def _build_check_candidate(
    repo: SceneRepository, batch: Dict[str, object]
) -> CheckCandidate:
    project_id = str(batch.get("project_id") or "")
    project = repo.get_project(project_id)
    task_ids = list(batch.get("task_ids") or [])
    missing_task_ids = [task_id for task_id in task_ids if not repo.get_task(str(task_id))]
    requirements = _batch_requirement_keys(repo, batch)
    completed_baselines = _completed_baselines(repo, str(batch["id"]))
    unavailable_reasons: List[str] = []
    if not task_ids:
        unavailable_reasons.append("batch_has_no_tasks")
    if missing_task_ids:
        unavailable_reasons.append("batch_references_missing_tasks")
    if task_ids and not missing_task_ids and not requirements:
        unavailable_reasons.append("batch_has_no_execution_targets")
    if not completed_baselines:
        unavailable_reasons.append("batch_has_no_completed_baseline")
    elif _baseline_coverage_gaps(repo, batch, completed_baselines[0]):
        unavailable_reasons.append("latest_baseline_missing_coverage")
    return CheckCandidate(
        project_id=project_id,
        project_name=str(project.get("name") if project else project_id),
        batch_id=str(batch["id"]),
        batch_name=str(batch.get("name") or batch["id"]),
        task_count=len(task_ids),
        latest_baseline_id=str(completed_baselines[0]["id"]) if completed_baselines else None,
        completed_baseline_count=len(completed_baselines),
        run_diff_threshold=batch.get("run_diff_threshold"),
        execution_diff_threshold=batch.get("execution_diff_threshold"),
        can_compare=not unavailable_reasons,
        unavailable_reasons=unavailable_reasons,
    )


def _execution_diff_level(execution: Dict[str, object]) -> float:
    if execution.get("diff_level") is not None:
        return _coerce_float(execution.get("diff_level"))
    diff = execution.get("diff")
    if isinstance(diff, dict):
        if diff.get("diff_level") is not None:
            return _coerce_float(diff.get("diff_level"))
        return _coerce_float(diff.get("percentage"))
    return 0.0


def _result_artifact_url(
    request: Request, executions: List[Dict[str, object]]
) -> Optional[str]:
    for execution in executions:
        artifacts = execution.get("artifacts") or {}
        if not isinstance(artifacts, dict):
            continue
        for key in ARTIFACT_URL_PRIORITY:
            artifact = artifacts.get(key)
            if not isinstance(artifact, dict):
                continue
            url = artifact.get("url")
            if not url and artifact.get("path"):
                url = f"/artifacts/{artifact['path']}"
            if url:
                return _absolute_url(request, str(url))
    return None


def _result_viewer_url(
    request: Request, run_id: str, executions: List[Dict[str, object]]
) -> Optional[str]:
    for execution in executions:
        artifacts = execution.get("artifacts") or {}
        if artifacts:
            return _absolute_url(
                request,
                f"/runs/{run_id}/executions/{execution['id']}/viewer",
            )
    return None


def _failure_statuses(
    run: Dict[str, object], executions: List[Dict[str, object]]
) -> List[RunFailureStatus]:
    failures: List[RunFailureStatus] = []
    run_status = str(run.get("status") or "")
    if run_status in {RunStatus.failed.value, RunStatus.cancelled.value}:
        failures.append(
            RunFailureStatus(
                scope="run",
                status=run_status,
                message=run.get("note"),
            )
        )
    for execution in executions:
        execution_status = str(execution.get("status") or "")
        if execution_status not in {
            ExecutionStatus.failed.value,
            ExecutionStatus.cancelled.value,
        }:
            continue
        failures.append(
            RunFailureStatus(
                scope="execution",
                status=execution_status,
                message=execution.get("message"),
                execution_id=str(execution.get("id") or ""),
                task_id=str(execution.get("task_id") or ""),
                task_name=execution.get("task_name"),
                browser=execution.get("browser"),
                viewport=execution.get("viewport"),
            )
        )
    return failures


def _threshold_result(
    run: Dict[str, object],
    executions: List[Dict[str, object]],
    *,
    run_threshold: Optional[float],
    execution_threshold: Optional[float],
) -> tuple[Optional[bool], List[str]]:
    if run.get("status") != RunStatus.finished.value:
        return None, []

    threshold_failures: List[str] = []
    evaluated = False
    diff_average = _coerce_float((run.get("summary") or {}).get("diff_average"))
    if run_threshold is not None:
        evaluated = True
        if diff_average > run_threshold:
            threshold_failures.append(
                f"run_diff_average {diff_average:.4f} exceeds threshold {run_threshold:.4f}"
            )
    if run_threshold is not None or execution_threshold is not None:
        for execution in executions:
            execution_status = str(execution.get("status") or "")
            if execution_status in {ExecutionStatus.failed.value, ExecutionStatus.cancelled.value}:
                threshold_failures.append(
                    f"execution {execution.get('id')} ended with status {execution_status}"
                )
            elif (
                execution_status == ExecutionStatus.finished.value
                and execution.get("diff_level") is None
                and not isinstance(execution.get("diff"), dict)
            ):
                threshold_failures.append(
                    f"execution {execution.get('id')} has no diff result"
                )
    if execution_threshold is not None:
        evaluated = True
        for execution in executions:
            if execution.get("diff_level") is None and not isinstance(execution.get("diff"), dict):
                continue
            diff_level = _execution_diff_level(execution)
            if diff_level > execution_threshold:
                threshold_failures.append(
                    "execution "
                    f"{execution.get('id')} diff {diff_level:.4f} exceeds threshold "
                    f"{execution_threshold:.4f}"
                )

    if not evaluated:
        return None, []
    return not threshold_failures, threshold_failures


def _build_integration_run_result(
    repo: SceneRepository,
    request: Request,
    run: Dict[str, object],
) -> IntegrationRunResult:
    batch = repo.get_batch(str(run["batch_id"]))
    executions = [dict(item) for item in repo.list_executions(run_id=str(run["id"]))]
    counts = repo.execution_status_counts(str(run["id"]))
    summary = dict(run.get("summary") or {})
    actual_total = sum(counts.values())
    executions_total = actual_total or int(summary.get("executions_total") or 0)
    executions_finished = counts[ExecutionStatus.finished.value]
    executions_failed = counts[ExecutionStatus.failed.value]
    executions_cancelled = counts[ExecutionStatus.cancelled.value]
    if actual_total == 0:
        executions_finished = int(summary.get("executions_finished") or 0)
        executions_failed = int(summary.get("executions_failed") or 0)
        executions_cancelled = int(summary.get("executions_cancelled") or 0)

    run_threshold = None
    execution_threshold = None
    if batch:
        if batch.get("run_diff_threshold") is not None:
            run_threshold = _coerce_float(batch.get("run_diff_threshold"))
        if batch.get("execution_diff_threshold") is not None:
            execution_threshold = _coerce_float(batch.get("execution_diff_threshold"))

    threshold_passed, threshold_failures = _threshold_result(
        run,
        executions,
        run_threshold=run_threshold,
        execution_threshold=execution_threshold,
    )

    return IntegrationRunResult(
        run_id=str(run["id"]),
        status=run.get("status", RunStatus.queued.value),
        batch_id=str(run["batch_id"]),
        baseline_id=run.get("baseline_id"),
        spm_ticket=run.get("spm_ticket"),
        executions_total=executions_total,
        executions_finished=executions_finished,
        executions_failed=executions_failed,
        executions_cancelled=executions_cancelled,
        diff_average=_coerce_float(summary.get("diff_average")),
        diff_maximum=_coerce_float(summary.get("diff_maximum")),
        run_diff_threshold=run_threshold,
        execution_diff_threshold=execution_threshold,
        threshold_passed=threshold_passed,
        threshold_failures=threshold_failures,
        artifact_url=_result_artifact_url(request, executions),
        viewer_url=_result_viewer_url(request, str(run["id"]), executions),
        failure_statuses=_failure_statuses(run, executions),
    )


@router.get(
    "/agent/manifest",
    response_model=AgentManifest,
    operation_id="get_agent_manifest",
)
async def get_agent_manifest() -> AgentManifest:
    protected = bool(os.environ.get(SCENE_API_TOKEN_ENV))
    endpoints = [
        AgentManifestEndpoint(
            method="GET",
            path="/openapi.json",
            description="Machine-readable OpenAPI schema for SCENE.",
        ),
        AgentManifestEndpoint(
            method="GET",
            path="/api/config",
            description="Read global SCENE runtime/configuration settings.",
        ),
        AgentManifestEndpoint(
            method="PATCH",
            path="/api/config",
            description="Update global SCENE runtime/configuration settings.",
            auth_required=True,
        ),
        AgentManifestEndpoint(
            method="POST",
            path="/api/agent/setup",
            description="Idempotently upsert a project, pages, tasks, and batches.",
            auth_required=True,
        ),
        AgentManifestEndpoint(
            method="POST",
            path="/api/batches/{batch_id}/comparison-runs",
            description="Launch an unattended comparison run from a completed baseline.",
            auth_required=True,
        ),
        AgentManifestEndpoint(
            method="POST",
            path="/api/runs",
            description="Launch a baseline-recording run for a configured batch.",
            auth_required=True,
        ),
        AgentManifestEndpoint(
            method="GET",
            path="/api/runs/{run_id}/result",
            description="Read SPM-friendly run metrics, thresholds, failures, and artifact links.",
        ),
        AgentManifestEndpoint(
            method="GET",
            path="/api/runs/{run_id}/artifacts",
            description="Read structured artifact metadata and viewer/log URLs.",
        ),
        AgentManifestEndpoint(
            method="POST",
            path="/api/executions/{execution_id}/retry",
            description="Retry an execution while its run is still modifiable.",
            auth_required=True,
        ),
    ]
    return AgentManifest(
        name="SCENE Agent Control Plane",
        version="0.1.0",
        openapi_url="/openapi.json",
        docs_url="/api/agent/docs",
        auth={
            "type": "bearer",
            "env_var": SCENE_API_TOKEN_ENV,
            "required_when_configured": True,
            "currently_configured": protected,
        },
        capabilities=[
            "config",
            "project_crud",
            "page_crud",
            "task_crud",
            "batch_crud",
            "baseline_read_delete",
            "run_launch",
            "run_status",
            "run_result",
            "run_artifacts",
            "run_cancel_delete",
            "execution_cancel_retry",
            "orchestrator_readiness",
            "idempotent_setup",
            "mcp",
        ],
        endpoints=endpoints,
        mcp_server={
            "command": "python -m scene_mcp.server",
            "env": {
                "SCENE_BASE_URL": "http://127.0.0.1:8000",
                "SCENE_API_TOKEN": "<same token configured on the SCENE app>",
            },
            "tools": [
                "scene_get_manifest",
                "scene_apply_setup",
                "scene_list_projects",
                "scene_list_batches",
                "scene_record_baseline",
                "scene_run_batch",
                "scene_get_run_status",
                "scene_get_run_result",
                "scene_get_artifacts",
                "scene_cancel_run",
                "scene_retry_execution",
            ],
        },
    )


@router.get(
    "/agent/docs",
    response_class=PlainTextResponse,
    operation_id="get_agent_docs",
)
async def get_agent_docs() -> PlainTextResponse:
    if not AGENT_DOCS_PATH.exists():
        raise HTTPException(status_code=404, detail="Agent API docs not found")
    return PlainTextResponse(
        AGENT_DOCS_PATH.read_text(encoding="utf-8"),
        media_type="text/markdown; charset=utf-8",
    )


@router.get("/config", response_model=SceneConfig, operation_id="get_scene_config")
async def get_scene_config(repo: SceneRepository = RepositoryDep) -> SceneConfig:
    return repo.get_config()


@router.patch("/config", response_model=SceneConfig, operation_id="update_scene_config")
async def update_scene_config(
    payload: SceneConfigUpdate,
    repo: SceneRepository = RepositoryDep,
    _auth: None = AgentAuthDep,
) -> SceneConfig:
    try:
        return _apply_config_update(repo, payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post(
    "/agent/setup",
    response_model=AgentSetupResponse,
    status_code=200,
    operation_id="apply_agent_setup",
)
async def apply_agent_setup(
    payload: AgentSetupRequest,
    repo: SceneRepository = RepositoryDep,
    _auth: None = AgentAuthDep,
) -> AgentSetupResponse:
    config_result = None
    if payload.config:
        try:
            config_result = _apply_config_update(repo, payload.config)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    project_payload = payload.project.model_dump()
    existing_project = _find_by_field(repo.list_projects(), "slug", payload.project.slug)
    if existing_project:
        project = repo.update_project(str(existing_project["id"]), project_payload)
        assert project is not None
        project_action = "updated"
    else:
        project = repo.create_project(project_payload)
        project_action = "created"
    project_id = str(project["id"])

    page_results: List[AgentSetupEntityResult] = []
    for page_payload in payload.pages:
        existing_page = _find_by_field(repo.list_pages(project_id), "name", page_payload.name)
        page_data = page_payload.model_dump()
        page_data["project_id"] = project_id
        if existing_page:
            page = repo.update_page(str(existing_page["id"]), page_data)
            assert page is not None
            page_results.append(_entity_result(page, "updated"))
        else:
            page = repo.create_page(page_data)
            page_results.append(_entity_result(page, "created"))

    page_lookup = {str(page["name"]): page for page in repo.list_pages(project_id)}
    task_results: List[AgentSetupEntityResult] = []
    for task_payload in payload.tasks:
        page = page_lookup.get(task_payload.page_name)
        if not page:
            raise HTTPException(
                status_code=400,
                detail=f"Task '{task_payload.name}' references unknown page '{task_payload.page_name}'",
            )
        existing_task = _find_by_field(repo.list_tasks(project_id), "name", task_payload.name)
        task_data = task_payload.model_dump(exclude={"page_name"})
        task_data["project_id"] = project_id
        task_data["page_id"] = page["id"]
        if existing_task:
            task = repo.update_task(str(existing_task["id"]), task_data)
            assert task is not None
            task_results.append(_entity_result(task, "updated"))
        else:
            task = repo.create_task(task_data)
            task_results.append(_entity_result(task, "created"))

    task_lookup = {str(task["name"]): task for task in repo.list_tasks(project_id)}
    batch_results: List[AgentSetupEntityResult] = []
    for batch_payload in payload.batches:
        missing = [name for name in batch_payload.task_names if name not in task_lookup]
        if missing:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Batch '{batch_payload.name}' references unknown tasks: "
                    + ", ".join(missing)
                ),
            )
        existing_batch = _find_by_field(repo.list_batches(project_id), "name", batch_payload.name)
        batch_data = batch_payload.model_dump(exclude={"task_names"})
        batch_data["project_id"] = project_id
        batch_data["task_ids"] = [str(task_lookup[name]["id"]) for name in batch_payload.task_names]
        if existing_batch:
            batch = repo.update_batch(str(existing_batch["id"]), batch_data)
            assert batch is not None
            batch_results.append(_entity_result(batch, "updated"))
        else:
            batch = repo.create_batch(batch_data)
            batch_results.append(_entity_result(batch, "created"))

    return AgentSetupResponse(
        project=_entity_result(project, project_action),
        config=config_result,
        pages=page_results,
        tasks=task_results,
        batches=batch_results,
    )


@router.get("/projects", response_model=List[Project])
async def list_projects(repo: SceneRepository = RepositoryDep) -> List[Project]:
    return repo.list_projects()


@router.post("/projects", response_model=Project, status_code=201)
async def create_project(
    payload: ProjectCreate,
    repo: SceneRepository = RepositoryDep,
    _auth: None = AgentAuthDep,
) -> Project:
    return repo.create_project(payload.model_dump())


@router.get("/projects/{project_id}", response_model=Project)
async def get_project(project_id: str, repo: SceneRepository = RepositoryDep) -> Project:
    project = repo.get_project(project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    return project


@router.patch("/projects/{project_id}", response_model=Project)
async def update_project(
    project_id: str,
    payload: ProjectUpdate,
    repo: SceneRepository = RepositoryDep,
    _auth: None = AgentAuthDep,
) -> Project:
    record = repo.update_project(project_id, payload.model_dump(exclude_unset=True))
    if not record:
        raise HTTPException(status_code=404, detail="Project not found")
    return record


@router.delete("/projects/{project_id}", status_code=204)
async def delete_project(
    project_id: str,
    repo: SceneRepository = RepositoryDep,
    _auth: None = AgentAuthDep,
) -> None:
    repo.delete_project(project_id)


# Pages ---------------------------------------------------------------------------
@router.get("/projects/{project_id}/pages", response_model=List[Page])
async def list_pages(project_id: str, repo: SceneRepository = RepositoryDep) -> List[Page]:
    _ensure_project(repo, project_id)
    return repo.list_pages(project_id)


@router.post("/pages", response_model=Page, status_code=201)
async def create_page(
    payload: PageCreate,
    repo: SceneRepository = RepositoryDep,
    _auth: None = AgentAuthDep,
) -> Page:
    _ensure_project(repo, payload.project_id)
    return repo.create_page(payload.model_dump())


@router.get("/pages/{page_id}", response_model=Page)
async def get_page(page_id: str, repo: SceneRepository = RepositoryDep) -> Page:
    page = repo.get_page(page_id)
    if not page:
        raise HTTPException(status_code=404, detail="Page not found")
    return page


@router.patch("/pages/{page_id}", response_model=Page)
async def update_page(
    page_id: str,
    payload: PageUpdate,
    repo: SceneRepository = RepositoryDep,
    _auth: None = AgentAuthDep,
) -> Page:
    record = repo.update_page(page_id, payload.model_dump(exclude_unset=True))
    if not record:
        raise HTTPException(status_code=404, detail="Page not found")
    return record


@router.delete("/pages/{page_id}", status_code=204)
async def delete_page(
    page_id: str,
    repo: SceneRepository = RepositoryDep,
    _auth: None = AgentAuthDep,
) -> None:
    repo.delete_page(page_id)


# Tasks ---------------------------------------------------------------------------
@router.get("/projects/{project_id}/tasks", response_model=List[Task])
async def list_tasks(project_id: str, repo: SceneRepository = RepositoryDep) -> List[Task]:
    _ensure_project(repo, project_id)
    return repo.list_tasks(project_id)


@router.post("/tasks", response_model=Task, status_code=201)
async def create_task(
    payload: TaskCreate,
    repo: SceneRepository = RepositoryDep,
    _auth: None = AgentAuthDep,
) -> Task:
    _ensure_project(repo, payload.project_id)
    if not repo.get_page(payload.page_id):
        raise HTTPException(status_code=404, detail="Page not found")
    return repo.create_task(payload.model_dump())


@router.get("/tasks/{task_id}", response_model=Task)
async def get_task(task_id: str, repo: SceneRepository = RepositoryDep) -> Task:
    task = repo.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


@router.patch("/tasks/{task_id}", response_model=Task)
async def update_task(
    task_id: str,
    payload: TaskUpdate,
    repo: SceneRepository = RepositoryDep,
    _auth: None = AgentAuthDep,
) -> Task:
    record = repo.update_task(task_id, payload.model_dump(exclude_unset=True))
    if not record:
        raise HTTPException(status_code=404, detail="Task not found")
    return record


@router.delete("/tasks/{task_id}", status_code=204)
async def delete_task(
    task_id: str,
    repo: SceneRepository = RepositoryDep,
    _auth: None = AgentAuthDep,
) -> None:
    repo.delete_task(task_id)


# Batches -------------------------------------------------------------------------
@router.get("/projects/{project_id}/batches", response_model=List[Batch])
async def list_batches(project_id: str, repo: SceneRepository = RepositoryDep) -> List[Batch]:
    _ensure_project(repo, project_id)
    return repo.list_batches(project_id)


@router.post("/batches", response_model=Batch, status_code=201)
async def create_batch(
    payload: BatchCreate,
    repo: SceneRepository = RepositoryDep,
    _auth: None = AgentAuthDep,
) -> Batch:
    _ensure_project(repo, payload.project_id)
    return repo.create_batch(payload.model_dump())


@router.get("/batches", response_model=List[Batch])
async def list_all_batches(
    project_id: Optional[str] = None,
    repo: SceneRepository = RepositoryDep,
) -> List[Batch]:
    if project_id:
        _ensure_project(repo, project_id)
        return repo.list_batches(project_id)
    return [
        batch
        for project in repo.list_projects()
        for batch in repo.list_batches(project["id"])
    ]


@router.get("/batches/{batch_id}", response_model=Batch)
async def get_batch(batch_id: str, repo: SceneRepository = RepositoryDep) -> Batch:
    batch = repo.get_batch(batch_id)
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found")
    return batch


@router.patch("/batches/{batch_id}", response_model=Batch)
async def update_batch(
    batch_id: str,
    payload: BatchUpdate,
    repo: SceneRepository = RepositoryDep,
    _auth: None = AgentAuthDep,
) -> Batch:
    record = repo.update_batch(batch_id, payload.model_dump(exclude_unset=True))
    if not record:
        raise HTTPException(status_code=404, detail="Batch not found")
    return record


@router.delete("/batches/{batch_id}", status_code=204)
async def delete_batch(
    batch_id: str,
    repo: SceneRepository = RepositoryDep,
    _auth: None = AgentAuthDep,
) -> None:
    repo.delete_batch(batch_id)


@router.get("/check-candidates", response_model=List[CheckCandidate])
async def list_check_candidates(
    project_id: Optional[str] = None,
    repo: SceneRepository = RepositoryDep,
) -> List[CheckCandidate]:
    if project_id:
        _ensure_project(repo, project_id)
        batches = repo.list_batches(project_id)
    else:
        batches = [
            batch
            for project in repo.list_projects()
            for batch in repo.list_batches(project["id"])
        ]
    return [_build_check_candidate(repo, batch) for batch in batches]


@router.get(
    "/projects/{project_id}/check-candidates",
    response_model=List[CheckCandidate],
)
async def list_project_check_candidates(
    project_id: str, repo: SceneRepository = RepositoryDep
) -> List[CheckCandidate]:
    _ensure_project(repo, project_id)
    return [_build_check_candidate(repo, batch) for batch in repo.list_batches(project_id)]


@router.post(
    "/batches/{batch_id}/comparison-runs",
    response_model=IntegrationRunResult,
    status_code=201,
)
async def launch_batch_comparison_run(
    batch_id: str,
    payload: BatchComparisonRunCreate,
    request: Request,
    repo: SceneRepository = RepositoryDep,
    _auth: None = AgentAuthDep,
) -> IntegrationRunResult:
    batch = repo.get_batch(batch_id)
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found")
    _ensure_project(repo, str(batch["project_id"]))

    baseline = None
    if payload.baseline_id:
        baseline = repo.get_baseline(payload.baseline_id)
        if not baseline or baseline.get("batch_id") != batch_id:
            raise HTTPException(status_code=404, detail="Baseline not found for batch")
        if baseline.get("status") != BaselineStatus.completed.value:
            raise HTTPException(status_code=400, detail="Baseline is not completed")
    else:
        baseline = _latest_completed_baseline(repo, batch_id)
        if not baseline:
            raise HTTPException(
                status_code=400,
                detail="Batch has no completed baseline for comparison",
            )
    coverage_gaps = _baseline_coverage_gaps(repo, batch, baseline)
    if coverage_gaps:
        raise HTTPException(
            status_code=400,
            detail=(
                "Baseline does not cover all batch task/browser/viewport combinations: "
                + "; ".join(coverage_gaps)
            ),
        )

    record = repo.create_run(
        {
            "project_id": batch["project_id"],
            "batch_id": batch_id,
            "baseline_id": baseline["id"],
            "purpose": RunPurpose.comparison.value,
            "requested_by": payload.requested_by,
            "note": payload.note,
            "spm_ticket": payload.spm_ticket,
            "timeout_seconds": payload.timeout_seconds,
        }
    )
    orchestrator = get_orchestrator()
    orchestrator.enqueue(record["id"])
    return _build_integration_run_result(repo, request, record)


# Runs ----------------------------------------------------------------------------
@router.get("/runs", response_model=List[Run])
async def list_runs(
    project_id: Optional[str] = None,
    batch_id: Optional[str] = None,
    repo: SceneRepository = RepositoryDep,
) -> List[Run]:
    return repo.list_runs(project_id=project_id, batch_id=batch_id)


@router.post("/runs", response_model=Run, status_code=201)
async def create_run(
    payload: RunCreate,
    repo: SceneRepository = RepositoryDep,
    _auth: None = AgentAuthDep,
) -> Run:
    _ensure_project(repo, payload.project_id)
    if not repo.get_batch(payload.batch_id):
        raise HTTPException(status_code=404, detail="Batch not found")
    record = repo.create_run(payload.model_dump())
    orchestrator = get_orchestrator()
    orchestrator.enqueue(record["id"])
    return record


@router.get("/runs/{run_id}", response_model=Run)
async def get_run(run_id: str, repo: SceneRepository = RepositoryDep) -> Run:
    record = repo.get_run(run_id)
    if not record:
        raise HTTPException(status_code=404, detail="Run not found")
    return record


@router.get(
    "/runs/{run_id}/detail",
    response_model=RunDetail,
    operation_id="get_run_detail",
)
async def get_run_detail(run_id: str, repo: SceneRepository = RepositoryDep) -> RunDetail:
    return _build_run_detail(repo, run_id)


@router.get("/runs/{run_id}/result", response_model=IntegrationRunResult)
async def get_run_result(
    run_id: str, request: Request, repo: SceneRepository = RepositoryDep
) -> IntegrationRunResult:
    record = repo.get_run(run_id)
    if not record:
        raise HTTPException(status_code=404, detail="Run not found")
    return _build_integration_run_result(repo, request, record)


@router.get(
    "/runs/{run_id}/artifacts",
    response_model=RunArtifacts,
    operation_id="list_run_artifacts",
)
async def list_run_artifacts(
    run_id: str,
    request: Request,
    repo: SceneRepository = RepositoryDep,
) -> RunArtifacts:
    if not repo.get_run(run_id):
        raise HTTPException(status_code=404, detail="Run not found")
    executions: List[ExecutionArtifactSet] = []
    for execution in repo.list_executions(run_id=run_id):
        artifacts = execution.get("artifacts") or {}
        public_artifacts = {
            key: _public_artifact_info(request, value)
            for key, value in artifacts.items()
            if isinstance(value, dict)
        }
        executions.append(
            ExecutionArtifactSet(
                execution_id=str(execution["id"]),
                task_id=str(execution.get("task_id") or ""),
                task_name=str(execution.get("task_name") or ""),
                browser=str(execution.get("browser") or ""),
                viewport=execution.get("viewport") or {},
                status=execution.get("status", ExecutionStatus.queued.value),
                artifacts=public_artifacts,
                viewer_url=_absolute_url(
                    request,
                    f"/runs/{run_id}/executions/{execution['id']}/viewer",
                )
                if artifacts
                else None,
                log_url=_absolute_url(
                    request,
                    f"/api/runs/{run_id}/executions/{execution['id']}/log",
                ),
            )
        )
    return RunArtifacts(run_id=run_id, executions=executions)


@router.patch("/runs/{run_id}", response_model=Run)
async def update_run(
    run_id: str,
    payload: RunUpdate,
    repo: SceneRepository = RepositoryDep,
    _auth: None = AgentAuthDep,
) -> Run:
    record = repo.update_run(run_id, payload.model_dump(exclude_unset=True))
    if not record:
        raise HTTPException(status_code=404, detail="Run not found")
    return record


@router.delete("/runs/{run_id}", status_code=204)
async def delete_run(
    run_id: str,
    repo: SceneRepository = RepositoryDep,
    _auth: None = AgentAuthDep,
) -> None:
    if repo.get_run(run_id):
        orchestrator = get_orchestrator()
        if hasattr(orchestrator, "cancel_run"):
            orchestrator.cancel_run(run_id)
    repo.delete_run(run_id)


@router.post("/runs/{run_id}/cancel", response_model=Run)
async def cancel_run(
    run_id: str,
    repo: SceneRepository = RepositoryDep,
    _auth: None = AgentAuthDep,
) -> Run:
    record = repo.get_run(run_id)
    if not record:
        raise HTTPException(status_code=404, detail="Run not found")
    if record.get("status") == RunStatus.finished.value:
        raise HTTPException(status_code=400, detail="Finished runs cannot be cancelled.")
    orchestrator = get_orchestrator()
    orchestrator.cancel_run(run_id)
    updated = repo.get_run(run_id)
    assert updated is not None
    return updated


@router.post("/executions/{execution_id}/cancel", response_model=TaskExecution)
async def cancel_execution(
    execution_id: str,
    repo: SceneRepository = RepositoryDep,
    _auth: None = AgentAuthDep,
) -> TaskExecution:
    execution = repo.get_execution(execution_id)
    if not execution:
        raise HTTPException(status_code=404, detail="Execution not found")
    run = repo.get_run(execution["run_id"])
    if run and run.get("status") == RunStatus.finished.value:
        raise HTTPException(status_code=400, detail="Finished runs cannot be modified.")
    orchestrator = get_orchestrator()
    orchestrator.cancel_execution(execution_id)
    updated = repo.get_execution(execution_id)
    assert updated is not None
    return updated


@router.get("/runs/{run_id}/executions", response_model=List[TaskExecution])
async def list_run_executions(run_id: str, repo: SceneRepository = RepositoryDep) -> List[TaskExecution]:
    if not repo.get_run(run_id):
        raise HTTPException(status_code=404, detail="Run not found")
    return repo.list_executions(run_id=run_id)


@router.get("/executions/{execution_id}", response_model=TaskExecution)
async def get_execution(execution_id: str, repo: SceneRepository = RepositoryDep) -> TaskExecution:
    execution = repo.get_execution(execution_id)
    if not execution:
        raise HTTPException(status_code=404, detail="Execution not found")
    return execution


@router.get(
    "/runs/{run_id}/executions/{execution_id}/log",
    response_model=ExecutionLog,
    operation_id="get_execution_log",
)
async def get_execution_log(
    run_id: str,
    execution_id: str,
    request: Request,
    repo: SceneRepository = RepositoryDep,
) -> ExecutionLog:
    return _log_payload(repo, request, run_id=run_id, execution_id=execution_id)


@router.post(
    "/executions/{execution_id}/retry",
    response_model=TaskExecution,
    operation_id="retry_execution",
)
async def retry_execution(
    execution_id: str,
    repo: SceneRepository = RepositoryDep,
    _auth: None = AgentAuthDep,
) -> TaskExecution:
    execution = repo.get_execution(execution_id)
    if not execution:
        raise HTTPException(status_code=404, detail="Execution not found")
    run = repo.get_run(execution["run_id"])
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    if run.get("status") == RunStatus.finished.value:
        raise HTTPException(status_code=400, detail="Finished runs cannot be retried.")
    try:
        return get_orchestrator().retry_execution(execution_id)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/orchestrator/ping")
async def orchestrator_ping() -> Dict[str, str]:
    return {"status": "ok"}


@router.get("/orchestrator/readiness")
async def orchestrator_readiness() -> Dict[str, object]:
    return get_orchestrator().deployment_readiness().as_dict()


def _format_baseline_option_payload(baseline: Dict[str, object]) -> BaselineOption:
    created_raw = str(baseline.get("created_at") or "Unknown")
    status_raw = str(baseline.get("status") or "pending")
    status_label = status_raw.replace("_", " ").title()
    baseline_id = str(baseline.get("id") or "")
    short_id = baseline_id[:8] if baseline_id else ""
    parts = [created_raw, status_label]
    if short_id:
        parts.append(short_id)
    label = " · ".join(parts)
    return BaselineOption(
        id=baseline_id,
        label=label,
        status=status_raw,
        created_at=created_raw,
    )


@router.post("/executions/{execution_id}/complete")
async def complete_execution_callback(
    execution_id: str,
    payload: ExecutionCallbackRequest,
    repo: SceneRepository = RepositoryDep,
):
    execution = repo.get_execution(execution_id)
    if not execution:
        raise HTTPException(status_code=404, detail="Execution not found")
    orchestrator = get_orchestrator()
    if not orchestrator.handle_execution_callback(execution_id, payload.model_dump()):
        raise HTTPException(status_code=403, detail="Invalid completion token or execution not pending")
    return {"status": "ok"}


@router.get("/baselines", response_model=List[Baseline], operation_id="list_baselines")
async def list_baselines(
    project_id: Optional[str] = None,
    batch_id: Optional[str] = None,
    repo: SceneRepository = RepositoryDep,
) -> List[Baseline]:
    if project_id:
        _ensure_project(repo, project_id)
    if batch_id and not repo.get_batch(batch_id):
        raise HTTPException(status_code=404, detail="Batch not found")
    return repo.list_baselines(project_id=project_id, batch_id=batch_id)


@router.get(
    "/baselines/{baseline_id}",
    response_model=Baseline,
    operation_id="get_baseline",
)
async def get_baseline(
    baseline_id: str, repo: SceneRepository = RepositoryDep
) -> Baseline:
    baseline = repo.get_baseline(baseline_id)
    if not baseline:
        raise HTTPException(status_code=404, detail="Baseline not found")
    return baseline


@router.delete(
    "/baselines/{baseline_id}",
    status_code=204,
    operation_id="delete_baseline",
)
async def delete_baseline(
    baseline_id: str,
    repo: SceneRepository = RepositoryDep,
    _auth: None = AgentAuthDep,
) -> None:
    if not repo.get_baseline(baseline_id):
        raise HTTPException(status_code=404, detail="Baseline not found")
    repo.delete_baseline(baseline_id)


@router.get("/batches/{batch_id}/baselines", response_model=List[BaselineOption])
async def list_batch_baselines(
    batch_id: str, repo: SceneRepository = RepositoryDep
) -> List[BaselineOption]:
    if not repo.get_batch(batch_id):
        raise HTTPException(status_code=404, detail="Batch not found")
    baselines = repo.list_baselines(batch_id=batch_id)
    return [
        _format_baseline_option_payload(item)
        for item in baselines
        if item.get("status") == BaselineStatus.completed.value
    ]
