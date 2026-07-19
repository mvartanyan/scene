import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from app.routes import api
from app.routes import artifacts
from app.routes import config as config_ui
from app.routes import projects as projects_ui
from app.routes import runs as runs_ui
from app.services.artifacts import get_artifact_store
from app.services.storage import SCENE_STATE_BACKEND_ENV, get_repository
from app.services.storage_types import InvalidStorageCursorError, StorageConflictError


@asynccontextmanager
async def lifespan(_app: FastAPI):
    if os.environ.get(SCENE_STATE_BACKEND_ENV, "json").strip().lower() == "dynamodb":
        get_repository().probe()
    if os.environ.get("SCENE_ARTIFACT_STORAGE", "filesystem").strip().lower() in {"s3", "object"}:
        get_artifact_store().probe()
    yield


app = FastAPI(title="Scene Visual Testing Dashboard", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.include_router(api.router)
app.include_router(artifacts.router)
app.include_router(config_ui.router)
app.include_router(projects_ui.router)
app.include_router(runs_ui.router)


@app.exception_handler(InvalidStorageCursorError)
async def invalid_storage_cursor(
    _request: Request,
    exc: InvalidStorageCursorError,
) -> JSONResponse:
    return JSONResponse(status_code=400, content={"detail": str(exc)})


@app.exception_handler(StorageConflictError)
async def storage_conflict(
    _request: Request,
    _exc: StorageConflictError,
) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={"detail": "The record changed concurrently; retry the request."},
    )


@app.get("/")
async def root() -> RedirectResponse:
    """Redirect visitors to the projects list as the primary entry point."""
    return RedirectResponse(url="/projects", status_code=303)
