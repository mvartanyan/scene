from __future__ import annotations

from pathlib import Path
from typing import Generator, Tuple

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routes import api as api_routes
from app.routes import runs as runs_routes
from app.schemas import ExecutionStatus, RunPurpose, RunStatus
from app.services.agent_auth import SCENE_API_TOKEN_HEADER
from app.services import artifacts as artifact_module
from app.services.storage import LocalDynamoStorage, SceneRepository, get_repository


class _FakeOrchestrator:
    def __init__(self, repo: SceneRepository) -> None:
        self.repo = repo
        self.enqueued: list[str] = []
        self.cancelled: list[str] = []
        self.retried: list[str] = []
        self.concurrency_updates: list[int] = []

    def enqueue(self, run_id: str) -> None:
        self.enqueued.append(run_id)

    def cancel_run(self, run_id: str) -> None:
        self.cancelled.append(run_id)
        self.repo.update_run(run_id, {"status": RunStatus.cancelled.value})

    def cancel_execution(self, execution_id: str) -> None:
        self.repo.update_execution(
            execution_id,
            {
                "status": ExecutionStatus.cancelled.value,
                "message": "cancelled by test",
            },
        )

    def retry_execution(self, execution_id: str):
        self.retried.append(execution_id)
        execution = self.repo.get_execution(execution_id)
        assert execution is not None
        return self.repo.create_execution(
            {
                "run_id": execution["run_id"],
                "project_id": execution["project_id"],
                "batch_id": execution["batch_id"],
                "task_id": execution["task_id"],
                "task_name": execution["task_name"],
                "page_id": execution.get("page_id"),
                "browser": execution["browser"],
                "viewport": execution["viewport"],
                "status": ExecutionStatus.queued.value,
            }
        )

    def update_concurrency(self, max_workers: int) -> None:
        self.concurrency_updates.append(max_workers)

    def update_scene_host(self, host_url: str) -> None:
        return None

    def update_capture_delay(self, milliseconds: int) -> None:
        return None

    def update_diff_pixel_tolerance(self, tolerance: int) -> None:
        return None

    def deployment_readiness(self):
        class _Readiness:
            def as_dict(self) -> dict:
                return {"ok": True, "issues": []}

        return _Readiness()


@pytest.fixture
def client(monkeypatch, tmp_path: Path) -> Generator[Tuple[TestClient, SceneRepository, _FakeOrchestrator], None, None]:
    monkeypatch.delenv("SCENE_API_TOKEN", raising=False)
    monkeypatch.setenv("SCENE_ARTIFACT_ROOT", str(tmp_path / "artifacts"))
    monkeypatch.setattr(artifact_module, "_artifact_store", None)

    repo = SceneRepository(LocalDynamoStorage(tmp_path / "state.json"))
    orchestrator = _FakeOrchestrator(repo)

    original_api_orchestrator = api_routes.get_orchestrator
    original_runs_orchestrator = runs_routes.get_orchestrator
    api_routes.get_orchestrator = lambda: orchestrator
    runs_routes.get_orchestrator = lambda: orchestrator

    app.dependency_overrides[get_repository] = lambda: repo
    with TestClient(app) as test_client:
        yield test_client, repo, orchestrator
    app.dependency_overrides.clear()
    api_routes.get_orchestrator = original_api_orchestrator
    runs_routes.get_orchestrator = original_runs_orchestrator
    monkeypatch.setattr(artifact_module, "_artifact_store", None)


def _auth_headers() -> dict[str, str]:
    return {"Authorization": "Bearer secret"}


def test_agent_manifest_docs_and_openapi(client: Tuple[TestClient, SceneRepository, _FakeOrchestrator]) -> None:
    api, _repo, _orch = client

    manifest_resp = api.get("/api/agent/manifest")
    assert manifest_resp.status_code == 200
    manifest = manifest_resp.json()
    assert manifest["openapi_url"] == "/openapi.json"
    assert manifest["docs_url"] == "/api/agent/docs"
    assert manifest["auth"]["alternate_header"] == SCENE_API_TOKEN_HEADER
    assert "SCENE_INGRESS_BASIC_AUTH_USERNAME" in manifest["mcp_server"]["env"]
    assert "SCENE_INGRESS_BASIC_AUTH_PASSWORD" in manifest["mcp_server"]["env"]
    assert "scene_apply_setup" in manifest["mcp_server"]["tools"]

    docs_resp = api.get("/api/agent/docs")
    assert docs_resp.status_code == 200
    assert "SCENE Agent API" in docs_resp.text

    openapi = api.get("/openapi.json").json()
    assert "/api/agent/setup" in openapi["paths"]
    assert openapi["paths"]["/api/agent/setup"]["post"]["operationId"] == "apply_agent_setup"
    assert "HTTPBearer" in openapi["components"]["securitySchemes"]
    setup_parameters = openapi["paths"]["/api/agent/setup"]["post"]["parameters"]
    assert any(
        parameter["in"] == "header" and parameter["name"] == SCENE_API_TOKEN_HEADER
        for parameter in setup_parameters
    )

    readiness = api.get("/api/orchestrator/readiness")
    assert readiness.status_code == 200
    assert readiness.json()["state"]["ok"] is True
    assert readiness.json()["state"]["backend"] == "json"


def test_agent_token_auth_guards_mutation_routes(
    monkeypatch,
    client: Tuple[TestClient, SceneRepository, _FakeOrchestrator],
) -> None:
    api, _repo, _orch = client
    monkeypatch.setenv("SCENE_API_TOKEN", "secret")

    payload = {"name": "Acme", "slug": "acme"}
    assert api.get("/api/projects").status_code == 200
    assert api.post("/api/projects", json=payload).status_code == 401
    assert (
        api.post(
            "/api/projects",
            json=payload,
            headers={"Authorization": "Bearer wrong"},
        ).status_code
        == 403
    )

    created = api.post("/api/projects", json=payload, headers=_auth_headers())
    assert created.status_code == 201
    assert created.json()["slug"] == "acme"

    custom_header_created = api.post(
        "/api/projects",
        json={"name": "Staging", "slug": "staging"},
        headers={
            "Authorization": "Basic c2NlbmUtcmV2aWV3ZXI6c2VjcmV0",
            SCENE_API_TOKEN_HEADER: "secret",
        },
    )
    assert custom_header_created.status_code == 201
    assert custom_header_created.json()["slug"] == "staging"

    invalid_custom_header = api.post(
        "/api/projects",
        json={"name": "Denied", "slug": "denied"},
        headers={SCENE_API_TOKEN_HEADER: "wrong"},
    )
    assert invalid_custom_header.status_code == 403


def test_agent_setup_is_idempotent_and_updates_config(
    monkeypatch,
    client: Tuple[TestClient, SceneRepository, _FakeOrchestrator],
) -> None:
    api, repo, orchestrator = client
    monkeypatch.setenv("SCENE_API_TOKEN", "secret")
    setup = {
        "config": {
            "browsers": ["chromium"],
            "viewports": ["1280x720"],
            "max_concurrent_executions": 2,
            "run_timeout_seconds": 900,
            "capture_post_wait_ms": 1000,
            "diff_pixel_tolerance": 1,
            "scene_host_url": "http://host.docker.internal:8010",
        },
        "project": {"name": "Agent Demo", "slug": "agent-demo"},
        "pages": [{"name": "Home", "url": "https://example.com/"}],
        "tasks": [
            {
                "name": "Home visual",
                "page_name": "Home",
                "browsers": ["chromium"],
                "viewports": [{"width": 1280, "height": 720}],
            }
        ],
        "batches": [
            {
                "name": "Smoke",
                "description": "first",
                "spm_ticket": "SCENE-12",
                "task_names": ["Home visual"],
                "run_diff_threshold": 2.0,
                "execution_diff_threshold": 3.0,
            }
        ],
    }

    first = api.post("/api/agent/setup", json=setup, headers=_auth_headers())
    assert first.status_code == 200
    first_body = first.json()
    assert first_body["project"]["action"] == "created"
    assert first_body["pages"][0]["action"] == "created"
    assert first_body["tasks"][0]["action"] == "created"
    assert first_body["batches"][0]["action"] == "created"
    assert first_body["config"]["run_timeout_seconds"] == 900

    setup["batches"][0]["description"] = "updated"
    second = api.post("/api/agent/setup", json=setup, headers=_auth_headers())
    assert second.status_code == 200
    second_body = second.json()
    assert second_body["project"]["id"] == first_body["project"]["id"]
    assert second_body["project"]["action"] == "updated"
    assert second_body["batches"][0]["id"] == first_body["batches"][0]["id"]
    assert second_body["batches"][0]["action"] == "updated"

    project_id = first_body["project"]["id"]
    assert len(repo.list_pages(project_id)) == 1
    assert len(repo.list_tasks(project_id)) == 1
    assert len(repo.list_batches(project_id)) == 1
    assert repo.list_batches(project_id)[0]["description"] == "updated"
    assert repo.list_batches(project_id)[0]["spm_ticket"] == "SCENE-12"
    assert orchestrator.concurrency_updates == [2, 2]


def test_agent_setup_rolls_back_invalid_project_graph(
    monkeypatch,
    client: Tuple[TestClient, SceneRepository, _FakeOrchestrator],
) -> None:
    api, repo, orchestrator = client
    monkeypatch.setenv("SCENE_API_TOKEN", "secret")

    response = api.post(
        "/api/agent/setup",
        headers=_auth_headers(),
        json={
            "config": {"max_concurrent_executions": 7},
            "project": {"name": "Invalid graph", "slug": "invalid-graph"},
            "pages": [{"name": "Home", "url": "https://example.com/"}],
            "tasks": [
                {
                    "name": "Missing page",
                    "page_name": "Unknown",
                    "browsers": ["chromium"],
                    "viewports": [{"width": 1280, "height": 720}],
                }
            ],
        },
    )

    assert response.status_code == 400
    assert repo.list_projects() == []
    assert repo._storage.list("pages") == []
    assert repo._storage.list("tasks") == []
    assert repo.get_config()["max_concurrent_executions"] == 4
    assert orchestrator.concurrency_updates == []


def test_run_detail_artifacts_log_and_retry_json(
    monkeypatch,
    client: Tuple[TestClient, SceneRepository, _FakeOrchestrator],
    tmp_path: Path,
) -> None:
    api, repo, _orch = client
    monkeypatch.setenv("SCENE_API_TOKEN", "secret")
    store = artifact_module.get_artifact_store()

    project = repo.create_project({"name": "Acme", "slug": "acme"})
    page = repo.create_page(
        {"project_id": project["id"], "name": "Home", "url": "https://example.com/"}
    )
    task = repo.create_task(
        {
            "project_id": project["id"],
            "page_id": page["id"],
            "name": "Home visual",
            "browsers": ["chromium"],
            "viewports": [{"width": 1280, "height": 720}],
        }
    )
    batch = repo.create_batch(
        {"project_id": project["id"], "name": "Smoke", "task_ids": [task["id"]]}
    )
    baseline = repo.create_baseline(
        {
            "project_id": project["id"],
            "batch_id": batch["id"],
            "status": "completed",
            "items": [],
        }
    )
    run = repo.create_run(
        {
            "project_id": project["id"],
            "batch_id": batch["id"],
            "baseline_id": baseline["id"],
            "purpose": RunPurpose.comparison.value,
            "status": RunStatus.executing.value,
        }
    )
    execution = repo.create_execution(
        {
            "run_id": run["id"],
            "project_id": project["id"],
            "batch_id": batch["id"],
            "task_id": task["id"],
            "task_name": task["name"],
            "browser": "chromium",
            "viewport": {"width": 1280, "height": 720},
            "status": ExecutionStatus.failed.value,
            "artifacts": {
                "log": {
                    "kind": "log",
                    "path": f"runs/{run['id']}/{task['id']}/runner.log",
                    "content_type": "text/plain",
                },
                "diff": {
                    "kind": "diff",
                    "path": f"runs/{run['id']}/{task['id']}/diff.png",
                    "content_type": "image/png",
                },
            },
        }
    )
    log_path = store.root / f"runs/{run['id']}/{task['id']}/runner.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text("runner output", encoding="utf-8")

    detail = api.get(f"/api/runs/{run['id']}/detail")
    assert detail.status_code == 200
    assert detail.json()["counts"]["failed"] == 1

    artifacts = api.get(f"/api/runs/{run['id']}/artifacts")
    assert artifacts.status_code == 200
    artifact_payload = artifacts.json()["executions"][0]
    assert artifact_payload["viewer_url"].endswith(f"/runs/{run['id']}/executions/{execution['id']}/viewer")
    assert artifact_payload["artifacts"]["diff"]["url"].endswith(f"/artifacts/runs/{run['id']}/{task['id']}/diff.png")

    log = api.get(f"/api/runs/{run['id']}/executions/{execution['id']}/log")
    assert log.status_code == 200
    assert log.json()["text"] == "runner output"

    retry = api.post(f"/api/executions/{execution['id']}/retry", headers=_auth_headers())
    assert retry.status_code == 200
    assert retry.json()["status"] == ExecutionStatus.queued.value
