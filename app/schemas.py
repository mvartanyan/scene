from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, HttpUrl, field_validator, model_validator


def spm_ticket_field() -> Field:
    return Field(
        default=None,
        description="SPM ticket key or reference associated with this object.",
    )


class SpmTicketAliasModel(BaseModel):
    @model_validator(mode="before")
    @classmethod
    def accept_legacy_jira_issue(cls, data):
        if isinstance(data, dict) and "spm_ticket" not in data and "jira_issue" in data:
            normalized = dict(data)
            normalized["spm_ticket"] = normalized["jira_issue"]
            return normalized
        return data

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


class ProjectCursorPage(BaseModel):
    items: List[Project]
    next_cursor: Optional[str] = None


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


class PageCursorPage(BaseModel):
    items: List[Page]
    next_cursor: Optional[str] = None


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


class TaskCursorPage(BaseModel):
    items: List[Task]
    next_cursor: Optional[str] = None


class BatchBase(SpmTicketAliasModel):
    name: str
    description: Optional[str] = None
    task_ids: List[str] = []
    spm_ticket: Optional[str] = spm_ticket_field()
    run_diff_threshold: Optional[float] = Field(default=None, ge=0)
    execution_diff_threshold: Optional[float] = Field(default=None, ge=0)


class BatchCreate(BatchBase):
    project_id: str


class BatchUpdate(SpmTicketAliasModel):
    name: Optional[str] = None
    description: Optional[str] = None
    task_ids: Optional[List[str]] = None
    spm_ticket: Optional[str] = spm_ticket_field()
    run_diff_threshold: Optional[float] = Field(default=None, ge=0)
    execution_diff_threshold: Optional[float] = Field(default=None, ge=0)


class Batch(BatchBase):
    id: str
    project_id: str
    created_at: str
    updated_at: str

    model_config = {"from_attributes": True}


class BatchCursorPage(BaseModel):
    items: List[Batch]
    next_cursor: Optional[str] = None


class SceneConfig(BaseModel):
    browsers: List[str]
    viewports: List[str]
    display_timezone: str
    run_timeout_seconds: int = Field(ge=1)
    max_concurrent_executions: int = Field(ge=1, le=32)
    scene_host_url: str
    capture_post_wait_ms: int = Field(ge=0)
    diff_pixel_tolerance: int = Field(ge=0, le=255)


class SceneConfigUpdate(BaseModel):
    browsers: Optional[List[str]] = None
    viewports: Optional[List[str]] = None
    display_timezone: Optional[str] = None
    run_timeout_seconds: Optional[int] = Field(default=None, ge=1)
    max_concurrent_executions: Optional[int] = Field(default=None, ge=1, le=32)
    scene_host_url: Optional[str] = None
    capture_post_wait_ms: Optional[int] = Field(default=None, ge=0)
    diff_pixel_tolerance: Optional[int] = Field(default=None, ge=0, le=255)


class AgentManifestEndpoint(BaseModel):
    method: str
    path: str
    description: str
    auth_required: bool = False


class AgentManifest(BaseModel):
    name: str
    version: str
    openapi_url: str
    docs_url: str
    auth: Dict[str, object]
    capabilities: List[str]
    endpoints: List[AgentManifestEndpoint]
    mcp_server: Dict[str, object]


class AgentSetupPage(PageBase):
    pass


class AgentSetupTask(TaskBase):
    page_name: str = Field(..., description="Name of the setup page this task belongs to.")


class AgentSetupBatch(SpmTicketAliasModel):
    name: str
    description: Optional[str] = None
    task_names: List[str] = Field(default_factory=list)
    spm_ticket: Optional[str] = spm_ticket_field()
    run_diff_threshold: Optional[float] = Field(default=None, ge=0)
    execution_diff_threshold: Optional[float] = Field(default=None, ge=0)


class AgentSetupRequest(BaseModel):
    project: ProjectCreate
    config: Optional[SceneConfigUpdate] = None
    pages: List[AgentSetupPage] = Field(default_factory=list)
    tasks: List[AgentSetupTask] = Field(default_factory=list)
    batches: List[AgentSetupBatch] = Field(default_factory=list)


class AgentSetupEntityResult(BaseModel):
    id: str
    name: str
    action: str


class AgentSetupResponse(BaseModel):
    project: AgentSetupEntityResult
    config: Optional[SceneConfig] = None
    pages: List[AgentSetupEntityResult] = Field(default_factory=list)
    tasks: List[AgentSetupEntityResult] = Field(default_factory=list)
    batches: List[AgentSetupEntityResult] = Field(default_factory=list)


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
    storage: Optional[str] = None
    bucket: Optional[str] = None
    key: Optional[str] = None
    sha256: Optional[str] = None
    etag: Optional[str] = None
    version_id: Optional[str] = None


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


class ExecutionCursorPage(BaseModel):
    items: List[TaskExecution]
    next_cursor: Optional[str] = None


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


class BaselineOption(BaseModel):
    id: str
    label: str
    status: Optional[str] = None
    created_at: Optional[str] = None


class CheckCandidate(BaseModel):
    project_id: str
    project_name: str
    batch_id: str
    batch_name: str
    task_count: int
    latest_baseline_id: Optional[str] = None
    completed_baseline_count: int = 0
    run_diff_threshold: Optional[float] = None
    execution_diff_threshold: Optional[float] = None
    can_compare: bool
    unavailable_reasons: List[str] = Field(default_factory=list)


class BatchComparisonRunCreate(SpmTicketAliasModel):
    baseline_id: Optional[str] = None
    requested_by: Optional[str] = None
    note: Optional[str] = None
    spm_ticket: Optional[str] = spm_ticket_field()
    timeout_seconds: Optional[int] = Field(default=None, ge=1)
    task_ids: Optional[List[str]] = Field(
        default=None,
        description="Optional subset of task ids from the batch. Omit to run the full batch.",
    )
    idempotency_key: Optional[str] = Field(
        default=None,
        min_length=1,
        max_length=200,
        description="Caller-stable key used to return the same run after a retried launch.",
    )


class RunFailureStatus(BaseModel):
    scope: str
    status: str
    message: Optional[str] = None
    execution_id: Optional[str] = None
    task_id: Optional[str] = None
    task_name: Optional[str] = None
    browser: Optional[str] = None
    viewport: Optional[Dict[str, int]] = None


class IntegrationRunResult(BaseModel):
    run_id: str
    status: RunStatus
    batch_id: str
    baseline_id: Optional[str] = None
    spm_ticket: Optional[str] = None
    executions_total: int
    executions_finished: int
    executions_failed: int
    executions_cancelled: int
    diff_average: float
    diff_maximum: float
    run_diff_threshold: Optional[float] = None
    execution_diff_threshold: Optional[float] = None
    threshold_passed: Optional[bool] = None
    threshold_failures: List[str] = Field(default_factory=list)
    artifact_url: Optional[str] = None
    viewer_url: Optional[str] = None
    failure_statuses: List[RunFailureStatus] = Field(default_factory=list)


class RunBase(SpmTicketAliasModel):
    project_id: str
    batch_id: str
    baseline_id: Optional[str] = None
    purpose: RunPurpose
    status: RunStatus = RunStatus.queued
    requested_by: Optional[str] = None
    note: Optional[str] = None
    spm_ticket: Optional[str] = spm_ticket_field()
    summary: RunSummary = Field(default_factory=RunSummary)
    timeout_seconds: Optional[int] = Field(default=None, ge=1)
    task_ids: Optional[List[str]] = Field(
        default=None,
        description="Optional subset of task ids from the batch. Omit to run the full batch.",
    )
    idempotency_key: Optional[str] = Field(default=None, min_length=1, max_length=200)


class RunCreate(RunBase):
    pass


class RunUpdate(SpmTicketAliasModel):
    status: Optional[RunStatus] = None
    note: Optional[str] = None
    spm_ticket: Optional[str] = spm_ticket_field()
    summary: Optional[RunSummary] = None
    timeout_seconds: Optional[int] = Field(default=None, ge=1)


class Run(RunBase):
    id: str
    created_at: str
    updated_at: str

    model_config = {"from_attributes": True}


class RunCursorPage(BaseModel):
    items: List[Run]
    next_cursor: Optional[str] = None


class RunDetail(BaseModel):
    run: Run
    project: Optional[Project] = None
    batch: Optional[Batch] = None
    baseline: Optional[Baseline] = None
    executions: List[TaskExecution] = Field(default_factory=list)
    counts: Dict[str, int] = Field(default_factory=dict)


class ExecutionArtifactSet(BaseModel):
    execution_id: str
    task_id: str
    task_name: str
    browser: str
    viewport: Dict[str, int]
    status: ExecutionStatus
    artifacts: Dict[str, ArtifactInfo] = Field(default_factory=dict)
    viewer_url: Optional[str] = None
    log_url: Optional[str] = None


class RunArtifacts(BaseModel):
    run_id: str
    executions: List[ExecutionArtifactSet] = Field(default_factory=list)


class ExecutionLog(BaseModel):
    run_id: str
    execution_id: str
    exists: bool
    length: int
    text: str
    artifact_path: Optional[str] = None
    artifact_url: Optional[str] = None
