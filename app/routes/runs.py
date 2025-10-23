from __future__ import annotations

import random
from typing import Dict, List, Optional, Tuple

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse

from app.schemas import RunPurpose, RunStatus
from app.services.storage import RepositoryDep, SceneRepository
from app.templating import templates

router = APIRouter(tags=["runs"])


def _status_cycle() -> List[RunStatus]:
    return [
        RunStatus.finished,
        RunStatus.finished,
        RunStatus.executing,
        RunStatus.failed,
    ]


def _mock_executions(
    repo: SceneRepository,
    run: Dict[str, any],
    batch: Optional[Dict[str, any]],
) -> List[Dict[str, any]]:
    summary = run.get("summary") or {}
    if summary.get("executions"):
        return summary["executions"]

    combos: List[Tuple[str, str, str]] = []
    tasks: List[Dict[str, any]] = []
    if batch:
        for task_id in batch.get("task_ids", []):
            task = repo.get_task(task_id)
            if task:
                tasks.append(task)
    if not tasks:
        tasks = repo.list_tasks(run["project_id"])

    for task in tasks:
        browsers = task.get("browsers") or ["chromium"]
        viewports = task.get("viewports") or [{"width": 1280, "height": 720}]
        for browser in browsers:
            for viewport in viewports:
                combos.append(
                    (
                        task["name"],
                        browser,
                        f"{viewport['width']}×{viewport['height']}",
                    )
                )

    if not combos:
        combos = [
            ("Homepage", "chromium", "1280×720"),
            ("Homepage", "firefox", "1280×720"),
            ("Login", "chromium", "1440×900"),
        ]

    statuses = _status_cycle()
    executions: List[Dict[str, any]] = []
    for idx, (task_name, browser, viewport_label) in enumerate(combos[:6]):
        status = statuses[idx % len(statuses)]
        diff_pct = round(random.uniform(0, 2.5), 2) if status == RunStatus.finished else None
        executions.append(
            {
                "id": f"{run['id']}-exec-{idx}",
                "task_name": task_name,
                "browser": browser,
                "viewport": viewport_label,
                "status": status.value,
                "diff_pct": diff_pct,
                "thumbnail": None,
            }
        )
    return executions


def _build_run_context(repo: SceneRepository, run_id: str) -> Dict[str, any]:
    run = repo.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    project = repo.get_project(run["project_id"])
    batch = repo.get_batch(run["batch_id"]) if run.get("batch_id") else None
    executions = _mock_executions(repo, run, batch)

    return {
        "run": run,
        "project": project,
        "batch": batch,
        "executions": executions,
    }


def _build_runs_dashboard_context(
    repo: SceneRepository,
    *,
    filter_project_id: Optional[str] = None,
    filter_status: Optional[str] = None,
    run_id: Optional[str] = None,
) -> Dict[str, any]:
    projects = repo.list_projects()
    project_lookup = {proj["id"]: proj for proj in projects}
    project_batches: Dict[str, List[Dict[str, any]]] = {
        proj["id"]: repo.list_batches(proj["id"]) for proj in projects
    }

    runs = repo.list_runs(project_id=filter_project_id) if filter_project_id else repo.list_runs()
    if filter_status:
        runs = [run for run in runs if run.get("status") == filter_status]

    for run in runs:
        project = project_lookup.get(run["project_id"])
        run["project_name"] = project["name"] if project else run["project_id"]

    selected_id = run_id or (runs[0]["id"] if runs else None)
    detail_context = None
    if selected_id:
        try:
            detail_context = _build_run_context(repo, selected_id)
        except HTTPException:
            selected_id = None
            detail_context = None

    context: Dict[str, any] = {
        "runs": runs,
        "selected_run_id": selected_id,
        "projects": projects,
        "project_batches": project_batches,
        "purposes": list(RunPurpose),
        "statuses": list(RunStatus),
        "filter_project_id": filter_project_id,
        "filter_status": filter_status,
    }
    if detail_context:
        context.update(detail_context)
    else:
        context["run"] = None
        context["project"] = None
        context["batch"] = None
        context["executions"] = []
    return context


@router.get("/runs", response_class=HTMLResponse)
async def runs_home(
    request: Request,
    run_id: Optional[str] = None,
    filter_project_id: Optional[str] = None,
    filter_status: Optional[RunStatus] = None,
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    context = _build_runs_dashboard_context(
        repo,
        filter_project_id=filter_project_id,
        filter_status=filter_status.value if filter_status else None,
        run_id=run_id,
    )
    context["request"] = request
    return templates.TemplateResponse("runs/index.html", context)


@router.get("/runs/fragment", response_class=HTMLResponse)
async def runs_fragment(
    request: Request,
    filter_project_id: Optional[str] = None,
    filter_status: Optional[str] = None,
    run_id: Optional[str] = None,
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    context = _build_runs_dashboard_context(
        repo,
        filter_project_id=filter_project_id,
        filter_status=filter_status,
        run_id=run_id,
    )
    context["request"] = request
    return templates.TemplateResponse("runs/_dashboard.html", context)


@router.get("/runs/{run_id}", response_class=HTMLResponse)
async def run_detail(
    run_id: str,
    request: Request,
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    context = _build_run_context(repo, run_id)
    context["request"] = request
    return templates.TemplateResponse("runs/_run_detail.html", context)


@router.post("/runs/mock", response_class=HTMLResponse)
async def create_mock_run(
    request: Request,
    project_id: str = Form(...),
    batch_id: str = Form(...),
    purpose: RunPurpose = Form(RunPurpose.comparison.value),
    status: RunStatus = Form(RunStatus.queued.value),
    jira_issue: Optional[str] = Form(None),
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    if not repo.get_project(project_id):
        raise HTTPException(status_code=404, detail="Project not found")
    if not repo.get_batch(batch_id):
        raise HTTPException(status_code=404, detail="Batch not found")

    summary = {
        "executions_total": random.randint(3, 8),
        "executions_finished": random.randint(1, 6),
        "executions_failed": random.randint(0, 2),
    }
    new_run = repo.create_run(
        {
            "project_id": project_id,
            "batch_id": batch_id,
            "purpose": purpose.value if isinstance(purpose, RunPurpose) else purpose,
            "status": status.value if isinstance(status, RunStatus) else status,
            "requested_by": "mock_user",
            "jira_issue": jira_issue,
            "summary": summary,
        }
    )

    context = _build_runs_dashboard_context(repo, run_id=new_run["id"])
    context["request"] = request
    return templates.TemplateResponse("runs/_dashboard.html", context)
