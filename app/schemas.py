from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, HttpUrl, field_validator

class ActionDefinition(BaseModel):
    type: str = Field(..., description="Action identifier, e.g. click, wait_for_selector.")
    selector: Optional[str] = None
    value: Optional[str] = None
    key: Optional[str] = None
    button: Optional[str] = None
    state: Optional[str] = None
    script: Optional[str] = None
    args: Optional[Dict[str, Any]] = None
    timeout_ms: Optional[int] = Field(default=None, ge=0)
    wait_ms: Optional[int] = Field(default=None, ge=0)
    delay_ms: Optional[int] = Field(default=None, ge=0)

    model_config = {"extra": "allow"}


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
    preparatory_actions: Optional[List[ActionDefinition]] = None
    basic_auth_username: Optional[str] = None
    basic_auth_password: Optional[str] = None


class PageCreate(PageBase):
    project_id: str


class PageUpdate(BaseModel):
    name: Optional[str] = None
    url: Optional[HttpUrl] = None
    reference_url: Optional[HttpUrl] = None
    preparatory_js: Optional[str] = None
    preparatory_actions: Optional[List[ActionDefinition]] = None
    basic_auth_username: Optional[str] = None
    basic_auth_password: Optional[str] = None


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
    task_actions: Optional[List[ActionDefinition]] = None


class TaskCreate(TaskBase):
    project_id: str
    page_id: str


class TaskUpdate(BaseModel):
    name: Optional[str] = None
    task_js: Optional[str] = None
    browsers: Optional[List[str]] = None
    viewports: Optional[List[Viewport]] = None
    task_actions: Optional[List[ActionDefinition]] = None


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
    run_diff_threshold: Optional[float] = Field(default=None, ge=0)
    execution_diff_threshold: Optional[float] = Field(default=None, ge=0)


class BatchCreate(BatchBase):
    project_id: str


class BatchUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    task_ids: Optional[List[str]] = None
    jira_issue: Optional[str] = None
    run_diff_threshold: Optional[float] = Field(default=None, ge=0)
    execution_diff_threshold: Optional[float] = Field(default=None, ge=0)


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
    cancelled = "cancelled"


class ExecutionStatus(str, Enum):
    queued = "queued"
    executing = "executing"
    finished = "finished"
    failed = "failed"
    cancelled = "cancelled"


class BaselineStatus(str, Enum):
    pending = "pending"
    completed = "completed"
    failed = "failed"


class ArtifactKind(str, Enum):
    observed = "observed"
    baseline = "baseline"
    reference = "reference"
    diff = "diff"
    heatmap = "heatmap"
    trace = "trace"
    video = "video"
    log = "log"


class RunSummary(BaseModel):
    executions_total: int = 0
    executions_finished: int = 0
    executions_failed: int = 0
    executions_cancelled: int = 0
    diff_average: float = 0.0
    diff_maximum: float = 0.0
    diff_samples: int = 0


class ArtifactInfo(BaseModel):
    kind: ArtifactKind
    path: str
    url: Optional[str] = None
    content_type: Optional[str] = None
    label: Optional[str] = None
    size_bytes: Optional[int] = None


class ExecutionDiff(BaseModel):
    pixel_count: Optional[int] = None
    percentage: Optional[float] = None
    diff_level: Optional[float] = None


class TaskExecution(BaseModel):
    id: str
    run_id: str
    project_id: str
    batch_id: str
    task_id: str
    task_name: str
    browser: str
    viewport: Dict[str, int]
    status: ExecutionStatus
    artifacts: Dict[str, ArtifactInfo] = Field(default_factory=dict)
    diff: Optional[ExecutionDiff] = None
    message: Optional[str] = None
    sequence: Optional[int] = None
    created_at: str
    updated_at: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    diff_level: Optional[float] = None

    model_config = {"from_attributes": True}


class ExecutionCallbackRequest(BaseModel):
    token: str
    result: Dict[str, object]


class BaselineItem(BaseModel):
    task_id: str
    task_name: str
    browser: str
    viewport: Dict[str, int]
    artifacts: Dict[str, ArtifactInfo]
    captured_at: str
    execution_id: Optional[str] = None


class Baseline(BaseModel):
    id: str
    project_id: str
    batch_id: str
    run_id: Optional[str] = None
    status: BaselineStatus
    note: Optional[str] = None
    items: List[BaselineItem] = Field(default_factory=list)
    created_at: str
    updated_at: str

    model_config = {"from_attributes": True}


class RunBase(BaseModel):
    project_id: str
    batch_id: str
    baseline_id: Optional[str] = None
    purpose: RunPurpose
    status: RunStatus = RunStatus.queued
    requested_by: Optional[str] = None
    note: Optional[str] = None
    jira_issue: Optional[str] = None
    summary: RunSummary = Field(default_factory=RunSummary)
    timeout_seconds: Optional[int] = Field(default=None, ge=1)


class RunCreate(RunBase):
    pass


class RunUpdate(BaseModel):
    status: Optional[RunStatus] = None
    note: Optional[str] = None
    jira_issue: Optional[str] = None
    summary: Optional[RunSummary] = None
    timeout_seconds: Optional[int] = Field(default=None, ge=1)


class Run(RunBase):
    id: str
    created_at: str
    updated_at: str

    model_config = {"from_attributes": True}
