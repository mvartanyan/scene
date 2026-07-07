from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

import pytest
from PIL import Image

from app.schemas import ExecutionStatus, RunPurpose, RunStatus
from app.services.artifacts import ArtifactStore
from app.services.orchestrator import RunOrchestrator
from app.services.storage import LocalDynamoStorage, SceneRepository


class ExitedContainerHandle:
    def __init__(self, exit_code: int = 0) -> None:
        self.id = "exited-container"
        self._exit_code = exit_code
        self.status_calls = 0
        self.removed = False

    def logs(self, stream: bool = True, follow: bool = True):
        if stream:
            return iter(())
        return b""

    def kill(self) -> None:
        pass

    def remove(self, force: bool = False) -> None:
        self.removed = True

    def status(self) -> str:
        self.status_calls += 1
        return "exited"

    def exit_code(self) -> int:
        return self._exit_code


class ExitedBackend:
    def __init__(self, handle: ExitedContainerHandle) -> None:
        self.handle = handle
        self.run_calls: List[Dict[str, object]] = []

    def run_container(
        self,
        image: str,
        command: List[str],
        *,
        environment: Dict[str, str],
        volumes: Dict[str, Dict[str, str]],
        working_dir: str | None,
        shm_size: str | None,
        name: str | None,
        auto_remove: bool,
        extra_hosts: Dict[str, str] | None,
    ) -> ExitedContainerHandle:
        self.run_calls.append(
            {
                "image": image,
                "command": command,
                "environment": environment,
                "volumes": volumes,
                "working_dir": working_dir,
                "shm_size": shm_size,
                "name": name,
                "auto_remove": auto_remove,
                "extra_hosts": extra_hosts,
            }
        )
        return self.handle

    def get_container(self, container_id: str) -> ExitedContainerHandle:
        return self.handle


class WorkspaceWritingRunner:
    image = "scene-playwright-runner:latest"

    def __init__(self, *, result_payload: Dict[str, object] | None, write_observed: bool) -> None:
        self.result_payload = result_payload
        self.write_observed = write_observed

    @property
    def timeout(self) -> int:
        return 180

    def prepare_workspace(self, config: Dict[str, object], workdir: Path) -> None:
        workdir.mkdir(parents=True, exist_ok=True)
        if self.write_observed:
            Image.new("RGBA", (4, 4), (20, 40, 60, 255)).save(workdir / "observed.png")
        if self.result_payload is not None:
            (workdir / "result.json").write_text(json.dumps(self.result_payload), encoding="utf-8")


def _create_single_execution_run(repo: SceneRepository) -> Dict[str, object]:
    project = repo.create_project({"name": "Watchdog", "slug": "watchdog"})
    page = repo.create_page(
        {
            "project_id": project["id"],
            "name": "Page",
            "url": "https://example.com",
        }
    )
    task = repo.create_task(
        {
            "project_id": project["id"],
            "page_id": page["id"],
            "name": "Task",
            "browsers": ["chromium"],
            "viewports": [{"width": 800, "height": 600}],
        }
    )
    batch = repo.create_batch(
        {
            "project_id": project["id"],
            "name": "Batch",
            "task_ids": [task["id"]],
        }
    )
    return repo.create_run(
        {
            "project_id": project["id"],
            "batch_id": batch["id"],
            "purpose": RunPurpose.baseline_recording.value,
            "requested_by": "unit-test",
        }
    )


def _make_orchestrator(
    tmp_path: Path,
    *,
    handle: ExitedContainerHandle,
    runner: WorkspaceWritingRunner,
) -> tuple[RunOrchestrator, SceneRepository]:
    storage = LocalDynamoStorage(tmp_path / "db.json")
    repo = SceneRepository(storage)
    artifacts = ArtifactStore(root=tmp_path / "artifacts")
    orchestrator = RunOrchestrator(
        repo=repo,
        artifacts=artifacts,
        auto_start=False,
        docker_backend=ExitedBackend(handle),
    )
    orchestrator._runner = runner
    orchestrator._runner_image_verified = True
    orchestrator._watchdog_interval = 0.01
    return orchestrator, repo


def _stop_watchdog(orchestrator: RunOrchestrator) -> None:
    orchestrator._watchdog_stop.set()
    if orchestrator._watchdog:
        orchestrator._watchdog.join(timeout=2)


@pytest.mark.unit
def test_watchdog_reconciles_exited_container_from_result_file(tmp_path: Path) -> None:
    handle = ExitedContainerHandle(exit_code=0)
    runner = WorkspaceWritingRunner(
        result_payload={"status": "ok", "screenshot": "observed.png"},
        write_observed=True,
    )
    orchestrator, repo = _make_orchestrator(tmp_path, handle=handle, runner=runner)
    run = _create_single_execution_run(repo)

    try:
        orchestrator._ensure_watchdog()
        orchestrator.execute_now(run["id"])
    finally:
        _stop_watchdog(orchestrator)

    refreshed_run = repo.get_run(run["id"])
    execution = repo.list_executions(run_id=run["id"])[0]
    assert refreshed_run["status"] == RunStatus.finished.value
    assert execution["status"] == ExecutionStatus.finished.value
    assert execution["artifacts"]["observed"]["path"].endswith("observed.png")
    assert handle.status_calls >= 1
    assert handle.removed
    assert orchestrator._execution_contexts == {}


@pytest.mark.unit
def test_watchdog_finalizes_exited_container_without_callback_as_failed(tmp_path: Path) -> None:
    handle = ExitedContainerHandle(exit_code=1)
    runner = WorkspaceWritingRunner(result_payload=None, write_observed=False)
    orchestrator, repo = _make_orchestrator(tmp_path, handle=handle, runner=runner)
    run = _create_single_execution_run(repo)

    try:
        orchestrator._ensure_watchdog()
        orchestrator.execute_now(run["id"])
    finally:
        _stop_watchdog(orchestrator)

    refreshed_run = repo.get_run(run["id"])
    execution = repo.list_executions(run_id=run["id"])[0]
    assert refreshed_run["status"] == RunStatus.failed.value
    assert execution["status"] == ExecutionStatus.failed.value
    assert "exit_code=1" in execution["message"]
    assert handle.status_calls >= 1
    assert handle.removed
    assert orchestrator._execution_contexts == {}


@pytest.mark.unit
def test_watchdog_does_not_infer_success_from_observed_only(tmp_path: Path) -> None:
    handle = ExitedContainerHandle(exit_code=0)
    runner = WorkspaceWritingRunner(result_payload=None, write_observed=True)
    orchestrator, repo = _make_orchestrator(tmp_path, handle=handle, runner=runner)
    run = _create_single_execution_run(repo)

    try:
        orchestrator._ensure_watchdog()
        orchestrator.execute_now(run["id"])
    finally:
        _stop_watchdog(orchestrator)

    refreshed_run = repo.get_run(run["id"])
    execution = repo.list_executions(run_id=run["id"])[0]
    assert refreshed_run["status"] == RunStatus.failed.value
    assert execution["status"] == ExecutionStatus.failed.value
    assert "exit_code=0" in execution["message"]
    assert handle.status_calls >= 1
    assert handle.removed
    assert orchestrator._execution_contexts == {}
