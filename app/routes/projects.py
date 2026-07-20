from __future__ import annotations

from typing import List, Optional, Tuple

import json
from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse

from app.constants import DEFAULT_BROWSERS, DEFAULT_VIEWPORTS
from app.pagination import DEFAULT_PAGE_SIZE
from app.schemas import RunStatus
from app.services.orchestrator import get_orchestrator
from app.services.storage import RepositoryDep, SceneRepository
from app.templating import templates

router = APIRouter(tags=["projects"])


class ProjectFormError(ValueError):
    pass


def _request_active_run_cancellation(
    repo: SceneRepository,
    *,
    project_id: Optional[str] = None,
    batch_id: Optional[str] = None,
) -> None:
    active_runs = repo.list_active_runs(project_id=project_id, batch_id=batch_id)
    if not active_runs:
        return
    orchestrator = get_orchestrator()
    for run in active_runs:
        orchestrator.cancel_run(str(run["id"]))
    raise HTTPException(
        status_code=409,
        detail=(
            "Active run cancellation was requested; retry deletion after run cleanup "
            "completes."
        ),
    )


def _parse_viewport_tokens(tokens: List[str]) -> Tuple[List[dict], List[str]]:
    viewports: List[dict] = []
    normalized: List[str] = []
    for token in tokens:
        token = token.strip().lower()
        if not token:
            continue
        if "x" not in token:
            raise HTTPException(status_code=400, detail=f"Invalid viewport '{token}'")
        width_str, height_str = token.split("x", 1)
        try:
            width = int(width_str)
            height = int(height_str)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid viewport '{token}'") from exc
        viewports.append({"width": width, "height": height})
        normalized.append(f"{width}x{height}")
    return viewports, normalized


def _parse_optional_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_actions_payload(raw: Optional[str], *, label: str) -> List[dict]:
    if raw is None:
        return []
    if isinstance(raw, str):
        if not raw.strip():
            return []
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ProjectFormError(f"{label} must be valid JSON.") from exc
    else:
        data = raw
    if not isinstance(data, list):
        raise ProjectFormError(f"{label} must be a JSON array of action objects.")
    cleaned: List[dict] = []
    for index, item in enumerate(data, 1):
        if not isinstance(item, dict):
            raise ProjectFormError(f"{label} item {index} must be an object.")
        action_type = item.get("type")
        if not isinstance(action_type, str) or not action_type.strip():
            raise ProjectFormError(f"{label} item {index} is missing a 'type'.")
        cleaned.append(dict(item))
    return cleaned


def _build_project_context(
    repo: SceneRepository,
    project_id: str,
    active_tab: str = "pages",
    *,
    item_page: int = 1,
    editing_page: Optional[dict] = None,
    editing_task: Optional[dict] = None,
    editing_batch: Optional[dict] = None,
) -> dict:
    project = repo.get_project(project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    page_count = repo.count("pages", key="project_id", value=project_id)
    task_count = repo.count("tasks", key="project_id", value=project_id)
    batch_count = repo.count("batches", key="project_id", value=project_id)
    all_pages: List[dict] = []
    all_tasks: List[dict] = []
    if active_tab == "tasks":
        all_pages = repo.list_pages(project_id)
    elif active_tab == "batches":
        all_tasks = repo.list_tasks(project_id)

    if editing_batch:
        for key in ("run_diff_threshold", "execution_diff_threshold"):
            raw_value = editing_batch.get(key)
            if raw_value in (None, "", "None"):
                editing_batch[key] = None
            else:
                try:
                    editing_batch[key] = float(raw_value)
                except (TypeError, ValueError):
                    editing_batch[key] = None

    if active_tab == "tasks":
        tasks, item_pagination = repo.numbered_page(
            "tasks",
            key="project_id",
            value=project_id,
            page=item_page,
            page_size=DEFAULT_PAGE_SIZE,
        )
        pages = all_pages
        batches = []
    elif active_tab == "batches":
        batches, item_pagination = repo.numbered_page(
            "batches",
            key="project_id",
            value=project_id,
            page=item_page,
            page_size=DEFAULT_PAGE_SIZE,
        )
        pages = []
        tasks = all_tasks
    else:
        pages, item_pagination = repo.numbered_page(
            "pages",
            key="project_id",
            value=project_id,
            page=item_page,
            page_size=DEFAULT_PAGE_SIZE,
        )
        tasks = []
        batches = []

    for batch in batches:
        for key in ("run_diff_threshold", "execution_diff_threshold"):
            raw_value = batch.get(key)
            if raw_value in (None, "", "None"):
                batch[key] = None
            else:
                try:
                    batch[key] = float(raw_value)
                except (TypeError, ValueError):
                    batch[key] = None

    config = repo.get_config()
    available_browsers = config.get("browsers") or DEFAULT_BROWSERS
    available_viewports = config.get("viewports") or DEFAULT_VIEWPORTS

    return {
        "project": project,
        "pages": pages,
        "tasks": tasks,
        "batches": batches,
        "page_lookup": {page["id"]: page for page in all_pages},
        "page_count": page_count,
        "task_count": task_count,
        "batch_count": batch_count,
        "item_pagination": item_pagination,
        "status_choices": list(RunStatus),
        "available_browsers": available_browsers,
        "available_viewports": available_viewports,
        "active_tab": active_tab,
        "editing_page": editing_page,
        "editing_task": editing_task,
        "editing_batch": editing_batch,
    }


def _render_project_detail(
    request: Request,
    repo: SceneRepository,
    project_id: str,
    active_tab: str = "pages",
    *,
    item_page: int = 1,
    editing_page: Optional[dict] = None,
    editing_task: Optional[dict] = None,
    editing_batch: Optional[dict] = None,
    flash_message: Optional[Tuple[str, str]] = None,
    error_message: Optional[str] = None,
) -> HTMLResponse:
    context = _build_project_context(
        repo,
        project_id,
        active_tab=active_tab,
        item_page=item_page,
        editing_page=editing_page,
        editing_task=editing_task,
        editing_batch=editing_batch,
    )
    context["request"] = request
    context["flash_message"] = flash_message or (("danger", error_message) if error_message else None)
    return templates.TemplateResponse(
        request=request,
        name="projects/_project_detail.html",
        context=context,
    )


@router.get("/projects", response_class=HTMLResponse)
async def projects_home(
    request: Request,
    project_id: Optional[str] = None,
    tab: Optional[str] = None,
    item_page: int = 1,
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    projects = repo.list_projects()
    selected_id = project_id or (projects[0]["id"] if projects else None)
    detail_context = None
    if selected_id:
        active_tab = tab if tab in {"pages", "tasks", "batches"} else "pages"
        detail_context = _build_project_context(
            repo,
            selected_id,
            active_tab=active_tab,
            item_page=item_page,
        )

    context = {
        "request": request,
        "projects": projects,
        "selected_project_id": selected_id,
    }
    if detail_context:
        context.update(detail_context)
    return templates.TemplateResponse(
        request=request,
        name="projects/index.html",
        context=context,
    )


@router.post("/projects")
async def create_project(
    request: Request,
    name: str = Form(...),
    slug: str = Form(...),
    description: Optional[str] = Form(None),
    repo: SceneRepository = RepositoryDep,
):
    project = repo.create_project({"name": name, "slug": slug, "description": description})
    response = HTMLResponse("")
    response.headers["HX-Redirect"] = f"/projects?project_id={project['id']}"
    response.status_code = 204
    return response


@router.delete("/projects/{project_id}")
async def delete_project(
    project_id: str, request: Request, repo: SceneRepository = RepositoryDep
):
    _request_active_run_cancellation(repo, project_id=project_id)
    try:
        repo.delete_project(project_id)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    response = HTMLResponse("")
    response.headers["HX-Redirect"] = "/projects"
    response.status_code = 204
    return response


@router.get("/projects/{project_id}/detail", response_class=HTMLResponse)
async def project_detail(
    project_id: str,
    request: Request,
    tab: Optional[str] = None,
    item_page: int = 1,
    edit_page_id: Optional[str] = None,
    edit_task_id: Optional[str] = None,
    edit_batch_id: Optional[str] = None,
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    active_tab = tab if tab in {"pages", "tasks", "batches"} else "pages"
    editing_page = repo.get_page(edit_page_id) if edit_page_id else None
    if editing_page and editing_page.get("project_id") != project_id:
        editing_page = None
    editing_task = repo.get_task(edit_task_id) if edit_task_id else None
    if editing_task and editing_task.get("project_id") != project_id:
        editing_task = None
    editing_batch = repo.get_batch(edit_batch_id) if edit_batch_id else None
    if editing_batch and editing_batch.get("project_id") != project_id:
        editing_batch = None
    return _render_project_detail(
        request,
        repo,
        project_id,
        active_tab=active_tab,
        item_page=item_page,
        editing_page=editing_page,
        editing_task=editing_task,
        editing_batch=editing_batch,
    )


@router.post("/projects/{project_id}/edit", response_class=HTMLResponse)
async def edit_project(
    project_id: str,
    request: Request,
    name: str = Form(...),
    slug: str = Form(...),
    description: Optional[str] = Form(None),
    active_tab: str = Form("pages"),
    item_page: int = Form(1),
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    updated = repo.update_project(
        project_id,
        {
            "name": name,
            "slug": slug,
            "description": description or None,
        },
    )
    if not updated:
        raise HTTPException(status_code=404, detail="Project not found")
    return _render_project_detail(
        request,
        repo,
        project_id,
        active_tab=active_tab,
        item_page=item_page,
    )


@router.post("/projects/{project_id}/pages", response_class=HTMLResponse)
async def create_page(
    project_id: str,
    request: Request,
    name: str = Form(...),
    url: str = Form(...),
    reference_url: Optional[str] = Form(None),
    preparatory_js: Optional[str] = Form(None),
    preparatory_actions: Optional[str] = Form(None),
    basic_auth_username: Optional[str] = Form(None),
    basic_auth_password: Optional[str] = Form(None),
    active_tab: str = Form("pages"),
    item_page: int = Form(1),
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    try:
        actions_payload = _parse_actions_payload(preparatory_actions, label="Preparatory actions")
    except ProjectFormError as exc:
        return _render_project_detail(
            request,
            repo,
            project_id,
            active_tab=active_tab,
            item_page=item_page,
            error_message=str(exc),
        )

    repo.create_page(
        {
            "project_id": project_id,
            "name": name,
            "url": url,
            "reference_url": reference_url or None,
            "preparatory_js": preparatory_js or None,
            "preparatory_actions": actions_payload,
            "basic_auth_username": basic_auth_username or None,
            "basic_auth_password": basic_auth_password or None,
        }
    )
    return _render_project_detail(
        request,
        repo,
        project_id,
        active_tab=active_tab,
        item_page=item_page,
        flash_message=("success", "Page saved"),
    )


@router.post("/pages/{page_id}/edit", response_class=HTMLResponse)
async def edit_page(
    page_id: str,
    request: Request,
    name: str = Form(...),
    url: str = Form(...),
    reference_url: Optional[str] = Form(None),
    preparatory_js: Optional[str] = Form(None),
    preparatory_actions: Optional[str] = Form(None),
    basic_auth_username: Optional[str] = Form(None),
    basic_auth_password: Optional[str] = Form(None),
    active_tab: str = Form("pages"),
    item_page: int = Form(1),
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    existing = repo.get_page(page_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Page not found")
    try:
        actions_payload = _parse_actions_payload(preparatory_actions, label="Preparatory actions")
    except ProjectFormError as exc:
        return _render_project_detail(
            request,
            repo,
            existing["project_id"],
            active_tab=active_tab,
            item_page=item_page,
            editing_page=existing,
            error_message=str(exc),
        )

    page = repo.update_page(
        page_id,
        {
            "name": name,
            "url": url,
            "reference_url": (reference_url or "").strip() or None,
            "preparatory_js": preparatory_js or "",
            "preparatory_actions": actions_payload,
            "basic_auth_username": (basic_auth_username or "").strip() or None,
            "basic_auth_password": basic_auth_password or None,
        },
    )
    if not page:
        raise HTTPException(status_code=404, detail="Page not found")
    return _render_project_detail(
        request,
        repo,
        page["project_id"],
        active_tab=active_tab,
        item_page=item_page,
        flash_message=("success", "Page updated"),
    )


@router.delete("/pages/{page_id}", response_class=HTMLResponse)
async def remove_page(
    page_id: str,
    request: Request,
    item_page: int = Form(1),
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    page = repo.get_page(page_id)
    if not page:
        raise HTTPException(status_code=404, detail="Page not found")
    repo.delete_page(page_id)
    return _render_project_detail(
        request,
        repo,
        page["project_id"],
        active_tab="pages",
        item_page=item_page,
    )


@router.post("/projects/{project_id}/tasks", response_class=HTMLResponse)
async def create_task(
    project_id: str,
    request: Request,
    name: str = Form(...),
    page_id: str = Form(...),
    browsers: Optional[List[str]] = Form(default=None),
    viewports: Optional[List[str]] = Form(default=None),
    task_js: Optional[str] = Form(None),
    task_actions: Optional[str] = Form(None),
    active_tab: str = Form("tasks"),
    item_page: int = Form(1),
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    config = repo.get_config()
    available_browsers = set(config.get("browsers") or DEFAULT_BROWSERS)
    selected_browsers = [b for b in (browsers or []) if b in available_browsers]

    viewport_dicts, viewport_tokens = _parse_viewport_tokens(viewports or [])
    available_viewports = set(config.get("viewports") or DEFAULT_VIEWPORTS)
    invalid_viewports = set(viewport_tokens) - available_viewports
    if invalid_viewports:
        raise HTTPException(
            status_code=400,
            detail="Unsupported viewports: " + ", ".join(sorted(invalid_viewports)),
        )
    try:
        actions_payload = _parse_actions_payload(task_actions, label="Task actions")
    except ProjectFormError as exc:
        return _render_project_detail(
            request,
            repo,
            project_id,
            active_tab=active_tab,
            item_page=item_page,
            error_message=str(exc),
        )

    repo.create_task(
        {
            "project_id": project_id,
            "page_id": page_id,
            "name": name,
            "task_js": task_js or None,
            "task_actions": actions_payload,
            "browsers": selected_browsers,
            "viewports": viewport_dicts,
        }
    )
    return _render_project_detail(
        request,
        repo,
        project_id,
        active_tab=active_tab,
        item_page=item_page,
        flash_message=("success", "Task saved"),
    )


@router.post("/tasks/{task_id}/edit", response_class=HTMLResponse)
async def edit_task(
    task_id: str,
    request: Request,
    name: str = Form(...),
    page_id: str = Form(...),
    browsers: Optional[List[str]] = Form(default=None),
    viewports: Optional[List[str]] = Form(default=None),
    task_js: Optional[str] = Form(None),
    task_actions: Optional[str] = Form(None),
    active_tab: str = Form("tasks"),
    item_page: int = Form(1),
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    task = repo.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    config = repo.get_config()
    available_browsers = set(config.get("browsers") or DEFAULT_BROWSERS)
    selected_browsers = [b for b in (browsers or []) if b in available_browsers]

    viewport_dicts, viewport_tokens = _parse_viewport_tokens(viewports or [])
    available_viewports = set(config.get("viewports") or DEFAULT_VIEWPORTS)
    invalid_viewports = set(viewport_tokens) - available_viewports
    if invalid_viewports:
        raise HTTPException(
            status_code=400,
            detail="Unsupported viewports: " + ", ".join(sorted(invalid_viewports)),
        )
    try:
        actions_payload = _parse_actions_payload(task_actions, label="Task actions")
    except ProjectFormError as exc:
        return _render_project_detail(
            request,
            repo,
            task["project_id"],
            active_tab=active_tab,
            item_page=item_page,
            editing_task=task,
            error_message=str(exc),
        )

    repo.update_task(
        task_id,
        {
            "name": name,
            "page_id": page_id,
            "browsers": selected_browsers,
            "viewports": viewport_dicts,
            "task_js": task_js or "",
            "task_actions": actions_payload,
        },
    )
    return _render_project_detail(
        request,
        repo,
        task["project_id"],
        active_tab=active_tab,
        item_page=item_page,
        flash_message=("success", "Task updated"),
    )


@router.delete("/tasks/{task_id}", response_class=HTMLResponse)
async def remove_task(
    task_id: str,
    request: Request,
    item_page: int = Form(1),
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    task = repo.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    repo.delete_task(task_id)
    return _render_project_detail(
        request,
        repo,
        task["project_id"],
        active_tab="tasks",
        item_page=item_page,
    )


@router.post("/projects/{project_id}/batches", response_class=HTMLResponse)
async def create_batch(
    project_id: str,
    request: Request,
    name: str = Form(...),
    description: Optional[str] = Form(None),
    task_ids: Optional[List[str]] = Form(default=None),
    spm_ticket: Optional[str] = Form(None),
    run_diff_threshold: Optional[str] = Form(None),
    execution_diff_threshold: Optional[str] = Form(None),
    active_tab: str = Form("batches"),
    item_page: int = Form(1),
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    selected_tasks = task_ids or []
    batch_payload = {
        "project_id": project_id,
        "name": name,
        "description": description or None,
        "task_ids": selected_tasks,
        "spm_ticket": spm_ticket or None,
        "run_diff_threshold": _parse_optional_float(run_diff_threshold),
        "execution_diff_threshold": _parse_optional_float(execution_diff_threshold),
    }
    repo.create_batch(
        batch_payload
    )
    return _render_project_detail(
        request,
        repo,
        project_id,
        active_tab=active_tab,
        item_page=item_page,
        flash_message=("success", "Batch saved"),
    )


@router.post("/batches/{batch_id}/edit", response_class=HTMLResponse)
async def edit_batch(
    batch_id: str,
    request: Request,
    name: str = Form(...),
    description: Optional[str] = Form(None),
    task_ids: Optional[List[str]] = Form(default=None),
    spm_ticket: Optional[str] = Form(None),
    run_diff_threshold: Optional[str] = Form(None),
    execution_diff_threshold: Optional[str] = Form(None),
    active_tab: str = Form("batches"),
    item_page: int = Form(1),
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    batch = repo.get_batch(batch_id)
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found")
    payload = {
        "name": name,
        "description": description or None,
        "task_ids": task_ids or [],
        "spm_ticket": spm_ticket or None,
        "run_diff_threshold": _parse_optional_float(run_diff_threshold),
        "execution_diff_threshold": _parse_optional_float(execution_diff_threshold),
    }
    repo.update_batch(
        batch_id,
        payload,
    )
    return _render_project_detail(
        request,
        repo,
        batch["project_id"],
        active_tab=active_tab,
        item_page=item_page,
        flash_message=("success", "Batch updated"),
    )


@router.delete("/batches/{batch_id}", response_class=HTMLResponse)
async def remove_batch(
    batch_id: str,
    request: Request,
    item_page: int = Form(1),
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    batch = repo.get_batch(batch_id)
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found")
    _request_active_run_cancellation(repo, batch_id=batch_id)
    try:
        repo.delete_batch(batch_id)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return _render_project_detail(
        request,
        repo,
        batch["project_id"],
        active_tab="batches",
        item_page=item_page,
    )
