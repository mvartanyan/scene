from __future__ import annotations

from typing import List, Optional, Tuple

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse

from app.constants import DEFAULT_BROWSERS, DEFAULT_VIEWPORTS
from app.schemas import RunStatus
from app.services.storage import RepositoryDep, SceneRepository
from app.templating import templates

router = APIRouter(tags=["projects"])


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


def _build_project_context(
    repo: SceneRepository, project_id: str, active_tab: str = "pages"
) -> dict:
    project = repo.get_project(project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    pages = repo.list_pages(project_id)
    tasks = repo.list_tasks(project_id)
    batches = repo.list_batches(project_id)

    config = repo.get_config()
    available_browsers = config.get("browsers") or DEFAULT_BROWSERS
    available_viewports = config.get("viewports") or DEFAULT_VIEWPORTS

    return {
        "project": project,
        "pages": pages,
        "tasks": tasks,
        "batches": batches,
        "page_lookup": {page["id"]: page for page in pages},
        "status_choices": list(RunStatus),
        "available_browsers": available_browsers,
        "available_viewports": available_viewports,
        "active_tab": active_tab,
    }


def _render_project_detail(
    request: Request, repo: SceneRepository, project_id: str, active_tab: str = "pages"
) -> HTMLResponse:
    context = _build_project_context(repo, project_id, active_tab=active_tab)
    context["request"] = request
    return templates.TemplateResponse("projects/_project_detail.html", context)


@router.get("/projects", response_class=HTMLResponse)
async def projects_home(
    request: Request,
    project_id: Optional[str] = None,
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    projects = repo.list_projects()
    selected_id = project_id or (projects[0]["id"] if projects else None)
    detail_context = None
    if selected_id:
        detail_context = _build_project_context(repo, selected_id)

    context = {
        "request": request,
        "projects": projects,
        "selected_project_id": selected_id,
    }
    if detail_context:
        context.update(detail_context)
    return templates.TemplateResponse("projects/index.html", context)


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
    repo.delete_project(project_id)
    response = HTMLResponse("")
    response.headers["HX-Redirect"] = "/projects"
    response.status_code = 204
    return response


@router.get("/projects/{project_id}/detail", response_class=HTMLResponse)
async def project_detail(
    project_id: str,
    request: Request,
    tab: Optional[str] = None,
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    active_tab = tab if tab in {"pages", "tasks", "batches"} else "pages"
    return _render_project_detail(request, repo, project_id, active_tab=active_tab)


@router.post("/projects/{project_id}/edit", response_class=HTMLResponse)
async def edit_project(
    project_id: str,
    request: Request,
    name: str = Form(...),
    slug: str = Form(...),
    description: Optional[str] = Form(None),
    active_tab: str = Form("pages"),
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
    return _render_project_detail(request, repo, project_id, active_tab=active_tab)


@router.post("/projects/{project_id}/pages", response_class=HTMLResponse)
async def create_page(
    project_id: str,
    request: Request,
    name: str = Form(...),
    url: str = Form(...),
    reference_url: Optional[str] = Form(None),
    preparatory_js: Optional[str] = Form(None),
    active_tab: str = Form("pages"),
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    repo.create_page(
        {
            "project_id": project_id,
            "name": name,
            "url": url,
            "reference_url": reference_url or None,
            "preparatory_js": preparatory_js or None,
        }
    )
    return _render_project_detail(request, repo, project_id, active_tab=active_tab)


@router.post("/pages/{page_id}/edit", response_class=HTMLResponse)
async def edit_page(
    page_id: str,
    request: Request,
    name: str = Form(...),
    url: str = Form(...),
    reference_url: Optional[str] = Form(None),
    preparatory_js: Optional[str] = Form(None),
    active_tab: str = Form("pages"),
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    page = repo.update_page(
        page_id,
        {
            "name": name,
            "url": url,
            "reference_url": reference_url or None,
            "preparatory_js": preparatory_js or None,
        },
    )
    if not page:
        raise HTTPException(status_code=404, detail="Page not found")
    return _render_project_detail(request, repo, page["project_id"], active_tab=active_tab)


@router.delete("/pages/{page_id}", response_class=HTMLResponse)
async def remove_page(
    page_id: str,
    request: Request,
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    page = repo.get_page(page_id)
    if not page:
        raise HTTPException(status_code=404, detail="Page not found")
    repo.delete_page(page_id)
    return _render_project_detail(request, repo, page["project_id"], active_tab="pages")


@router.post("/projects/{project_id}/tasks", response_class=HTMLResponse)
async def create_task(
    project_id: str,
    request: Request,
    name: str = Form(...),
    page_id: str = Form(...),
    browsers: Optional[List[str]] = Form(default=None),
    viewports: Optional[List[str]] = Form(default=None),
    task_js: Optional[str] = Form(None),
    active_tab: str = Form("tasks"),
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

    repo.create_task(
        {
            "project_id": project_id,
            "page_id": page_id,
            "name": name,
            "task_js": task_js or None,
            "browsers": selected_browsers,
            "viewports": viewport_dicts,
        }
    )
    return _render_project_detail(request, repo, project_id, active_tab=active_tab)


@router.post("/tasks/{task_id}/edit", response_class=HTMLResponse)
async def edit_task(
    task_id: str,
    request: Request,
    name: str = Form(...),
    page_id: str = Form(...),
    browsers: Optional[List[str]] = Form(default=None),
    viewports: Optional[List[str]] = Form(default=None),
    task_js: Optional[str] = Form(None),
    active_tab: str = Form("tasks"),
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

    repo.update_task(
        task_id,
        {
            "name": name,
            "page_id": page_id,
            "browsers": selected_browsers,
            "viewports": viewport_dicts,
            "task_js": task_js or None,
        },
    )
    return _render_project_detail(request, repo, task["project_id"], active_tab=active_tab)


@router.delete("/tasks/{task_id}", response_class=HTMLResponse)
async def remove_task(
    task_id: str,
    request: Request,
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    task = repo.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    repo.delete_task(task_id)
    return _render_project_detail(request, repo, task["project_id"], active_tab="tasks")


@router.post("/projects/{project_id}/batches", response_class=HTMLResponse)
async def create_batch(
    project_id: str,
    request: Request,
    name: str = Form(...),
    description: Optional[str] = Form(None),
    task_ids: Optional[List[str]] = Form(default=None),
    jira_issue: Optional[str] = Form(None),
    active_tab: str = Form("batches"),
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    selected_tasks = task_ids or []
    repo.create_batch(
        {
            "project_id": project_id,
            "name": name,
            "description": description or None,
            "task_ids": selected_tasks,
            "jira_issue": jira_issue or None,
        }
    )
    return _render_project_detail(request, repo, project_id, active_tab=active_tab)


@router.post("/batches/{batch_id}/edit", response_class=HTMLResponse)
async def edit_batch(
    batch_id: str,
    request: Request,
    name: str = Form(...),
    description: Optional[str] = Form(None),
    task_ids: Optional[List[str]] = Form(default=None),
    jira_issue: Optional[str] = Form(None),
    active_tab: str = Form("batches"),
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    batch = repo.get_batch(batch_id)
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found")
    repo.update_batch(
        batch_id,
        {
            "name": name,
            "description": description or None,
            "task_ids": task_ids or [],
            "jira_issue": jira_issue or None,
        },
    )
    return _render_project_detail(request, repo, batch["project_id"], active_tab=active_tab)


@router.delete("/batches/{batch_id}", response_class=HTMLResponse)
async def remove_batch(
    batch_id: str,
    request: Request,
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    batch = repo.get_batch(batch_id)
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found")
    repo.delete_batch(batch_id)
    return _render_project_detail(request, repo, batch["project_id"], active_tab="batches")
