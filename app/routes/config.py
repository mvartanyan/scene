from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse

from app.constants import DEFAULT_BROWSERS, DEFAULT_VIEWPORTS
from app.services.storage import RepositoryDep, SceneRepository
from app.templating import templates

router = APIRouter(tags=["config"])

def _collect_usage(repo: SceneRepository) -> tuple[set[str], set[str]]:
    used_browsers: set[str] = set()
    used_viewports: set[str] = set()
    for project in repo.list_projects():
        for task in repo.list_tasks(project["id"]):
            used_browsers.update(task.get("browsers", []))
            for viewport in task.get("viewports", []):
                try:
                    width = int(viewport.get("width"))
                    height = int(viewport.get("height"))
                    used_viewports.add(f"{width}x{height}")
                except (TypeError, ValueError, AttributeError):
                    continue
    return used_browsers, used_viewports


def _build_config_context(
    request: Request,
    repo: SceneRepository,
    *,
    message: Optional[str] = None,
    error: Optional[str] = None,
) -> dict:
    config = repo.get_config()
    selected_browsers = config.get("browsers") or DEFAULT_BROWSERS
    selected_viewports = config.get("viewports") or DEFAULT_VIEWPORTS
    used_browsers, used_viewports = _collect_usage(repo)

    browser_options = sorted(set(DEFAULT_BROWSERS) | set(selected_browsers) | used_browsers)
    available_viewports = sorted(
        set(selected_viewports),
        key=lambda item: tuple(int(x) for x in item.split("x")),
    )

    return {
        "request": request,
        "browser_options": browser_options,
        "selected_browsers": set(selected_browsers),
        "locked_browsers": used_browsers,
        "available_viewports": available_viewports,
        "locked_viewports": used_viewports,
        "message": message,
        "error": error,
    }


@router.get("/config", response_class=HTMLResponse)
async def config_modal(
    request: Request,
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    context = _build_config_context(request, repo)
    return templates.TemplateResponse("config/modal.html", context)


@router.post("/config/browsers", response_class=HTMLResponse)
async def update_browsers(
    request: Request,
    browsers: Optional[List[str]] = Form(default=None),
    new_browser: Optional[str] = Form(default=None),
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    try:
        selected = browsers or []
        if new_browser and new_browser.strip():
            selected.append(new_browser.strip().lower())
        repo.set_available_browsers(selected)
        context = _build_config_context(request, repo, message="Browser availability updated.")
    except ValueError as exc:
        context = _build_config_context(request, repo, error=str(exc))
    return templates.TemplateResponse("config/modal.html", context)


@router.post("/config/viewports/add", response_class=HTMLResponse)
async def add_viewport(
    request: Request,
    width: int = Form(...),
    height: int = Form(...),
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    if width <= 0 or height <= 0:
        context = _build_config_context(request, repo, error="Viewport dimensions must be positive integers.")
        return templates.TemplateResponse("config/modal.html", context)

    token = f"{width}x{height}"
    config = repo.get_config()
    current = config.get("viewports") or []
    if token not in current:
        current.append(token)
    try:
        repo.set_available_viewports(current)
        context = _build_config_context(request, repo, message="Viewport added.")
    except ValueError as exc:
        context = _build_config_context(request, repo, error=str(exc))
    return templates.TemplateResponse("config/modal.html", context)


@router.post("/config/viewports/remove", response_class=HTMLResponse)
async def remove_viewport(
    request: Request,
    viewport: str = Form(...),
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    config = repo.get_config()
    current = [vp for vp in config.get("viewports") or [] if vp != viewport]
    try:
        repo.set_available_viewports(current)
        context = _build_config_context(request, repo, message="Viewport removed.")
    except ValueError as exc:
        context = _build_config_context(request, repo, error=str(exc))
    return templates.TemplateResponse("config/modal.html", context)
