from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import pytest
import urllib.request

from app.schemas import RunPurpose
from app.services.artifacts import ArtifactStore
from app.services.orchestrator import RunnerResult, RunOrchestrator
from app.services.storage import LocalDynamoStorage, SceneRepository


class StubRunner:
    def __init__(self) -> None:
        self.configs: List[Dict[str, object]] = []

    def prepare_workspace(self, config: Dict[str, object], workdir: Path) -> None:
        self.configs.append(config)
        workdir.mkdir(parents=True, exist_ok=True)


class StubContainerHandle:
    def __init__(self, status: str = "running", exit_code: int | None = None) -> None:
        self.id = "stub"
        self._status = status
        self._exit_code = exit_code

    def logs(self, stream: bool = True, follow: bool = True):
        return iter(())

    def kill(self) -> None:
        pass

    def remove(self, force: bool = False) -> None:
        pass

    def status(self) -> str:
        return self._status

    def exit_code(self) -> int | None:
        return self._exit_code


class StubBackend:
    def __init__(self) -> None:
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
    ) -> StubContainerHandle:
        is_probe = any(isinstance(part, str) and "orchestrator/ping" in part for part in command)
        handle = StubContainerHandle(status="exited" if is_probe else "running", exit_code=0 if is_probe else None)
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
                "handle": handle,
            }
        )
        return handle

    def get_container(self, container_id: str) -> StubContainerHandle:
        return StubContainerHandle()


@pytest.mark.unit
def test_runner_receives_basic_auth_credentials(tmp_path: Path) -> None:
    storage = LocalDynamoStorage(tmp_path / "db.json")
    repo = SceneRepository(storage)
    artifacts = ArtifactStore(root=tmp_path / "artifacts")
    stub_backend = StubBackend()
    orchestrator = RunOrchestrator(
        repo=repo,
        artifacts=artifacts,
        auto_start=False,
        docker_backend=stub_backend,
    )

    stub_runner = StubRunner()
    orchestrator._runner = stub_runner

    project = repo.create_project({"name": "Secure", "slug": "secure"})
    page = repo.create_page(
        {
            "project_id": project["id"],
            "name": "Auth Page",
            "url": "https://example.com/protected",
            "basic_auth_username": "alice",
            "basic_auth_password": "wonderland",
        }
    )
    task = repo.create_task(
        {
            "project_id": project["id"],
            "page_id": page["id"],
            "name": "Smoke",
            "browsers": ["chromium"],
            "viewports": [{"width": 800, "height": 600}],
        }
    )
    batch = repo.create_batch(
        {
            "project_id": project["id"],
            "name": "Baseline Batch",
            "task_ids": [task["id"]],
        }
    )

    run = repo.create_run(
        {
            "project_id": project["id"],
            "batch_id": batch["id"],
            "purpose": RunPurpose.baseline_recording.value,
            "requested_by": "unit-test",
        }
    )

    executions = orchestrator._create_execution_matrix(run, [(task, page)])
    execution = executions[0]

    orchestrator._launch_execution(
        run=run,
        execution=execution,
        task=task,
        page=page,
        baseline=None,
        deadline=None,
        execution_timeout=None,
    )

    assert stub_runner.configs, "runner.prepare_workspace was not invoked"
    config = stub_runner.configs[0]
    assert config.get("http_credentials"), "http_credentials missing from runner config"
    creds = config["http_credentials"]
    assert creds["username"] == "alice"
    assert creds["password"] == "wonderland"

    orchestrator._cleanup_execution_context(execution["id"])


@pytest.mark.unit
def test_validate_callback_host_uses_probe(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    storage = LocalDynamoStorage(tmp_path / "db.json")
    repo = SceneRepository(storage)
    artifacts = ArtifactStore(root=tmp_path / "artifacts")
    stub_backend = StubBackend()
    orchestrator = RunOrchestrator(
        repo=repo,
        artifacts=artifacts,
        auto_start=False,
        docker_backend=stub_backend,
    )

    def _fake_urlopen(url: str, timeout: int = 15):
        class _Response:
            def read(self) -> bytes:
                return b"ok"

        return _Response()

    monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen)

    success, message = orchestrator.validate_callback_host()
    assert success
    assert "reachable" in message
    assert stub_backend.run_calls, "Probe container was not launched"
