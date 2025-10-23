from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from app.routes import api
from app.routes import config as config_ui
from app.routes import projects as projects_ui
from app.routes import runs as runs_ui

app = FastAPI(title="Scene Visual Testing Dashboard")
app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.include_router(api.router)
app.include_router(config_ui.router)
app.include_router(projects_ui.router)
app.include_router(runs_ui.router)


@app.get("/")
async def root() -> RedirectResponse:
    """Redirect visitors to the projects list as the primary entry point."""
    return RedirectResponse(url="/projects", status_code=303)
