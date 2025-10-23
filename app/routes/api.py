from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, HTTPException

from app.schemas import (
    Batch,
    BatchCreate,
    BatchUpdate,
    Page,
    PageCreate,
    PageUpdate,
    Project,
    ProjectCreate,
    ProjectUpdate,
    Run,
    RunCreate,
    RunUpdate,
    Task,
    TaskCreate,
    TaskUpdate,
)
from app.services.storage import RepositoryDep, SceneRepository

router = APIRouter(prefix="/api", tags=["api"])


def _ensure_project(repo: SceneRepository, project_id: str) -> None:
    if not repo.get_project(project_id):
        raise HTTPException(status_code=404, detail="Project not found")


@router.get("/projects", response_model=List[Project])
async def list_projects(repo: SceneRepository = RepositoryDep) -> List[Project]:
    return repo.list_projects()


@router.post("/projects", response_model=Project, status_code=201)
async def create_project(
    payload: ProjectCreate, repo: SceneRepository = RepositoryDep
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
    project_id: str, payload: ProjectUpdate, repo: SceneRepository = RepositoryDep
) -> Project:
    record = repo.update_project(project_id, payload.model_dump(exclude_unset=True))
    if not record:
        raise HTTPException(status_code=404, detail="Project not found")
    return record


@router.delete("/projects/{project_id}", status_code=204)
async def delete_project(project_id: str, repo: SceneRepository = RepositoryDep) -> None:
    repo.delete_project(project_id)


# Pages ---------------------------------------------------------------------------
@router.get("/projects/{project_id}/pages", response_model=List[Page])
async def list_pages(project_id: str, repo: SceneRepository = RepositoryDep) -> List[Page]:
    _ensure_project(repo, project_id)
    return repo.list_pages(project_id)


@router.post("/pages", response_model=Page, status_code=201)
async def create_page(payload: PageCreate, repo: SceneRepository = RepositoryDep) -> Page:
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
    page_id: str, payload: PageUpdate, repo: SceneRepository = RepositoryDep
) -> Page:
    record = repo.update_page(page_id, payload.model_dump(exclude_unset=True))
    if not record:
        raise HTTPException(status_code=404, detail="Page not found")
    return record


@router.delete("/pages/{page_id}", status_code=204)
async def delete_page(page_id: str, repo: SceneRepository = RepositoryDep) -> None:
    repo.delete_page(page_id)


# Tasks ---------------------------------------------------------------------------
@router.get("/projects/{project_id}/tasks", response_model=List[Task])
async def list_tasks(project_id: str, repo: SceneRepository = RepositoryDep) -> List[Task]:
    _ensure_project(repo, project_id)
    return repo.list_tasks(project_id)


@router.post("/tasks", response_model=Task, status_code=201)
async def create_task(payload: TaskCreate, repo: SceneRepository = RepositoryDep) -> Task:
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
    task_id: str, payload: TaskUpdate, repo: SceneRepository = RepositoryDep
) -> Task:
    record = repo.update_task(task_id, payload.model_dump(exclude_unset=True))
    if not record:
        raise HTTPException(status_code=404, detail="Task not found")
    return record


@router.delete("/tasks/{task_id}", status_code=204)
async def delete_task(task_id: str, repo: SceneRepository = RepositoryDep) -> None:
    repo.delete_task(task_id)


# Batches -------------------------------------------------------------------------
@router.get("/projects/{project_id}/batches", response_model=List[Batch])
async def list_batches(project_id: str, repo: SceneRepository = RepositoryDep) -> List[Batch]:
    _ensure_project(repo, project_id)
    return repo.list_batches(project_id)


@router.post("/batches", response_model=Batch, status_code=201)
async def create_batch(
    payload: BatchCreate, repo: SceneRepository = RepositoryDep
) -> Batch:
    _ensure_project(repo, payload.project_id)
    return repo.create_batch(payload.model_dump())


@router.get("/batches/{batch_id}", response_model=Batch)
async def get_batch(batch_id: str, repo: SceneRepository = RepositoryDep) -> Batch:
    batch = repo.get_batch(batch_id)
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found")
    return batch


@router.patch("/batches/{batch_id}", response_model=Batch)
async def update_batch(
    batch_id: str, payload: BatchUpdate, repo: SceneRepository = RepositoryDep
) -> Batch:
    record = repo.update_batch(batch_id, payload.model_dump(exclude_unset=True))
    if not record:
        raise HTTPException(status_code=404, detail="Batch not found")
    return record


@router.delete("/batches/{batch_id}", status_code=204)
async def delete_batch(batch_id: str, repo: SceneRepository = RepositoryDep) -> None:
    repo.delete_batch(batch_id)


# Runs ----------------------------------------------------------------------------
@router.get("/runs", response_model=List[Run])
async def list_runs(
    project_id: Optional[str] = None,
    batch_id: Optional[str] = None,
    repo: SceneRepository = RepositoryDep,
) -> List[Run]:
    return repo.list_runs(project_id=project_id, batch_id=batch_id)


@router.post("/runs", response_model=Run, status_code=201)
async def create_run(payload: RunCreate, repo: SceneRepository = RepositoryDep) -> Run:
    _ensure_project(repo, payload.project_id)
    if not repo.get_batch(payload.batch_id):
        raise HTTPException(status_code=404, detail="Batch not found")
    return repo.create_run(payload.model_dump())


@router.get("/runs/{run_id}", response_model=Run)
async def get_run(run_id: str, repo: SceneRepository = RepositoryDep) -> Run:
    record = repo.get_run(run_id)
    if not record:
        raise HTTPException(status_code=404, detail="Run not found")
    return record


@router.patch("/runs/{run_id}", response_model=Run)
async def update_run(
    run_id: str, payload: RunUpdate, repo: SceneRepository = RepositoryDep
) -> Run:
    record = repo.update_run(run_id, payload.model_dump(exclude_unset=True))
    if not record:
        raise HTTPException(status_code=404, detail="Run not found")
    return record


@router.delete("/runs/{run_id}", status_code=204)
async def delete_run(run_id: str, repo: SceneRepository = RepositoryDep) -> None:
    repo.delete_run(run_id)
