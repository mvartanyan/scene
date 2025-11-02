from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Form, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import HTMLResponse

from app.constants import DEFAULT_BROWSERS, DEFAULT_VIEWPORTS
from app.services.orchestrator import get_orchestrator
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
    display_timezone = config.get("display_timezone", "utc")
    run_timeout_seconds = int(config.get("run_timeout_seconds", 600))
    max_concurrent = int(config.get("max_concurrent_executions", 4))
    scene_host_url = config.get("scene_host_url", "http://host.docker.internal:8000")
    capture_post_wait_ms = int(config.get("capture_post_wait_ms", 7000))

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
        "display_timezone": display_timezone,
        "run_timeout_seconds": run_timeout_seconds,
        "max_concurrent_executions": max_concurrent,
        "scene_host_url": scene_host_url,
        "capture_post_wait_ms": capture_post_wait_ms,
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


@router.post("/config/run-concurrency", response_class=HTMLResponse)
async def update_run_concurrency(
    request: Request,
    max_concurrent_executions: int = Form(...),
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    try:
        repo.set_max_concurrent_executions(max_concurrent_executions)
        get_orchestrator().update_concurrency(max_concurrent_executions)
        context = _build_config_context(request, repo, message="Run concurrency updated.")
    except ValueError as exc:
        context = _build_config_context(request, repo, error=str(exc))
    return templates.TemplateResponse("config/modal.html", context)


@router.post("/config/run-timeout", response_class=HTMLResponse)
async def update_run_timeout(
    request: Request,
    run_timeout_seconds: int = Form(...),
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    try:
        repo.set_run_timeout_seconds(run_timeout_seconds)
        context = _build_config_context(request, repo, message="Run timeout updated.")
    except ValueError as exc:
        context = _build_config_context(request, repo, error=str(exc))
    return templates.TemplateResponse("config/modal.html", context)


@router.post("/config/capture-delay", response_class=HTMLResponse)
async def update_capture_delay(
    request: Request,
    capture_post_wait_ms: int = Form(...),
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    try:
        repo.set_capture_post_wait_ms(capture_post_wait_ms)
        orchestrator = get_orchestrator()
        orchestrator.update_capture_delay(capture_post_wait_ms)
        context = _build_config_context(request, repo, message="Capture stabilization delay updated.")
    except ValueError as exc:
        context = _build_config_context(request, repo, error=str(exc))
    return templates.TemplateResponse("config/modal.html", context)


@router.post("/config/host-url", response_class=HTMLResponse)
async def update_scene_host_url(
    request: Request,
    scene_host_url: str = Form(...),
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    try:
        repo.set_scene_host_url(scene_host_url)
        orchestrator = get_orchestrator()
        orchestrator.update_scene_host(scene_host_url)
        success, probe_message = await run_in_threadpool(orchestrator.validate_callback_host)
        if success:
            note = f"Scene host URL updated. {probe_message}"
            context = _build_config_context(request, repo, message=note)
        else:
            error_message = f"Scene host URL updated but callback probe failed: {probe_message}"
            context = _build_config_context(request, repo, error=error_message)
    except ValueError as exc:
        context = _build_config_context(request, repo, error=str(exc))
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


@router.post("/config/display-timezone", response_class=HTMLResponse)
async def update_display_timezone(
    request: Request,
    display_timezone: str = Form(...),
    repo: SceneRepository = RepositoryDep,
) -> HTMLResponse:
    try:
        repo.set_display_timezone(display_timezone)
        context = _build_config_context(request, repo, message="Timestamp display updated.")
    except ValueError as exc:
        context = _build_config_context(request, repo, error=str(exc))
    return templates.TemplateResponse("config/modal.html", context)
