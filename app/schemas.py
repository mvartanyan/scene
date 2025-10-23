from __future__ import annotations

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, HttpUrl, field_validator


class ProjectBase(BaseModel):
    name: str
    slug: str
    description: Optional[str] = None


class ProjectCreate(ProjectBase):
    pass


class ProjectUpdate(BaseModel):
    name: Optional[str] = None
    slug: Optional[str] = None
    description: Optional[str] = None


class Project(ProjectBase):
    id: str
    created_at: str
    updated_at: str

    model_config = {"from_attributes": True}


class PageBase(BaseModel):
    name: str
    url: HttpUrl
    reference_url: Optional[HttpUrl] = None
    preparatory_js: Optional[str] = None


class PageCreate(PageBase):
    project_id: str


class PageUpdate(BaseModel):
    name: Optional[str] = None
    url: Optional[HttpUrl] = None
    reference_url: Optional[HttpUrl] = None
    preparatory_js: Optional[str] = None


class Page(PageBase):
    id: str
    project_id: str
    created_at: str
    updated_at: str

    model_config = {"from_attributes": True}


class Viewport(BaseModel):
    width: int
    height: int

    @field_validator("width", "height")
    @classmethod
    def validate_positive(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("Viewport dimensions must be positive integers.")
        return value


class TaskBase(BaseModel):
    name: str
    task_js: Optional[str] = None
    browsers: List[str]
    viewports: List[Viewport]


class TaskCreate(TaskBase):
    project_id: str
    page_id: str


class TaskUpdate(BaseModel):
    name: Optional[str] = None
    task_js: Optional[str] = None
    browsers: Optional[List[str]] = None
    viewports: Optional[List[Viewport]] = None


class Task(TaskBase):
    id: str
    project_id: str
    page_id: str
    created_at: str
    updated_at: str

    model_config = {"from_attributes": True}


class BatchBase(BaseModel):
    name: str
    description: Optional[str] = None
    task_ids: List[str] = []
    jira_issue: Optional[str] = None


class BatchCreate(BatchBase):
    project_id: str


class BatchUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    task_ids: Optional[List[str]] = None
    jira_issue: Optional[str] = None


class Batch(BatchBase):
    id: str
    project_id: str
    created_at: str
    updated_at: str

    model_config = {"from_attributes": True}


class RunPurpose(str, Enum):
    baseline_recording = "baseline_recording"
    comparison = "comparison"


class RunStatus(str, Enum):
    queued = "queued"
    executing = "executing"
    finished = "finished"
    failed = "failed"


class RunBase(BaseModel):
    project_id: str
    batch_id: str
    baseline_id: Optional[str] = None
    purpose: RunPurpose
    status: RunStatus = RunStatus.queued
    requested_by: Optional[str] = None
    note: Optional[str] = None
    jira_issue: Optional[str] = None
    summary: dict = {}


class RunCreate(RunBase):
    pass


class RunUpdate(BaseModel):
    status: Optional[RunStatus] = None
    note: Optional[str] = None
    jira_issue: Optional[str] = None
    summary: Optional[dict] = None


class Run(RunBase):
    id: str
    created_at: str
    updated_at: str

    model_config = {"from_attributes": True}
