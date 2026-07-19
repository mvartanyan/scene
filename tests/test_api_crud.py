from __future__ import annotations

from typing import Generator, Tuple

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routes import api as api_routes
from app.routes import runs as runs_routes
from app.services.storage import LocalDynamoStorage, SceneRepository, get_repository


@pytest.fixture
def client(tmp_path) -> Generator[Tuple[TestClient, SceneRepository], None, None]:
    """Provide an isolated TestClient with a fresh repository per test."""
    storage = LocalDynamoStorage(tmp_path / "db.json")
    repo = SceneRepository(storage)

    class _NoopOrchestrator:
        def enqueue(self, run_id: str) -> None:
            return None

    original_api_orchestrator = api_routes.get_orchestrator
    original_runs_orchestrator = runs_routes.get_orchestrator

    def override_orchestrator() -> _NoopOrchestrator:
        return _NoopOrchestrator()

    api_routes.get_orchestrator = override_orchestrator
    runs_routes.get_orchestrator = override_orchestrator

    def override_repo() -> SceneRepository:
        return repo

    app.dependency_overrides[get_repository] = override_repo
    with TestClient(app) as test_client:
        yield test_client, repo
    app.dependency_overrides.clear()
    api_routes.get_orchestrator = original_api_orchestrator
    runs_routes.get_orchestrator = original_runs_orchestrator


def test_project_crud(client: Tuple[TestClient, SceneRepository]) -> None:
    api, _repo = client

    resp = api.post(
        "/api/projects",
        json={"name": "Acme", "slug": "acme", "description": "Visual regression suite"},
    )
    assert resp.status_code == 201
    project = resp.json()
    project_id = project["id"]

    resp = api.get("/api/projects")
    assert resp.status_code == 200
    projects = resp.json()
    assert len(projects) == 1
    assert projects[0]["name"] == "Acme"

    resp = api.patch(f"/api/projects/{project_id}", json={"name": "Acme Corp"})
    assert resp.status_code == 200
    assert resp.json()["name"] == "Acme Corp"

    resp = api.get(f"/api/projects/{project_id}")
    assert resp.status_code == 200
    assert resp.json()["slug"] == "acme"

    resp = api.delete(f"/api/projects/{project_id}")
    assert resp.status_code == 204

    resp = api.get("/api/projects")
    assert resp.status_code == 200
    assert resp.json() == []


def test_page_task_batch_crud_flow(client: Tuple[TestClient, SceneRepository]) -> None:
    api, _repo = client

    project = api.post("/api/projects", json={"name": "Demo", "slug": "demo"}).json()
    project_id = project["id"]

    page_payload = {
        "project_id": project_id,
        "name": "Landing",
        "url": "https://example.org/",
        "reference_url": "https://prod.example.org/",
        "preparatory_js": "console.log('setup')",
    }
    page = api.post("/api/pages", json=page_payload).json()
    assert page["name"] == "Landing"
    page_id = page["id"]

    resp = api.get(f"/api/projects/{project_id}/pages")
    assert resp.status_code == 200
    assert resp.json()[0]["url"] == "https://example.org/"

    task_payload = {
        "project_id": project_id,
        "page_id": page_id,
        "name": "Landing comparison",
        "task_js": "await page.click('#cta')",
        "browsers": ["chromium", "firefox"],
        "viewports": [{"width": 1280, "height": 720}, {"width": 800, "height": 600}],
    }
    task = api.post("/api/tasks", json=task_payload).json()
    task_id = task["id"]

    resp = api.get(f"/api/projects/{project_id}/tasks")
    tasks = resp.json()
    assert len(tasks) == 1
    assert tasks[0]["browsers"] == ["chromium", "firefox"]

    batch_payload = {
        "project_id": project_id,
        "name": "Smoke",
        "description": "Key surfaces",
        "task_ids": [task_id],
        "spm_ticket": "SCENE-101",
    }
    batch = api.post("/api/batches", json=batch_payload).json()
    batch_id = batch["id"]

    resp = api.get(f"/api/projects/{project_id}/batches")
    batches = resp.json()
    assert len(batches) == 1
    assert batches[0]["task_ids"] == [task_id]
    assert batches[0]["spm_ticket"] == "SCENE-101"
    assert "jira_issue" not in batches[0]

    resp = api.patch(
        f"/api/batches/{batch_id}",
        json={"description": "Updated desc", "jira_issue": "SCENE-102"},
    )
    assert resp.status_code == 200
    batch_updated = resp.json()
    assert batch_updated["description"] == "Updated desc"
    assert batch_updated["spm_ticket"] == "SCENE-102"

    resp = api.delete(f"/api/tasks/{task_id}")
    assert resp.status_code == 204
    resp = api.get(f"/api/projects/{project_id}/tasks")
    assert resp.json() == []

    resp = api.delete(f"/api/batches/{batch_id}")
    assert resp.status_code == 204
    resp = api.get(f"/api/projects/{project_id}/batches")
    assert resp.json() == []


def test_run_crud(client: Tuple[TestClient, SceneRepository]) -> None:
    api, _repo = client

    project = api.post("/api/projects", json={"name": "Foo", "slug": "foo"}).json()
    project_id = project["id"]
    page = api.post(
        "/api/pages",
        json={
            "project_id": project_id,
            "name": "Home",
            "url": "https://example.com/",
        },
    ).json()
    task = api.post(
        "/api/tasks",
        json={
            "project_id": project_id,
            "page_id": page["id"],
            "name": "Home baseline",
            "browsers": ["chromium"],
            "viewports": [{"width": 1440, "height": 900}],
        },
    ).json()
    batch = api.post(
        "/api/batches",
        json={
            "project_id": project_id,
            "name": "Primary",
            "task_ids": [task["id"]],
            "spm_ticket": "SCENE-202",
        },
    ).json()

    run_payload = {
        "project_id": project_id,
        "batch_id": batch["id"],
        "purpose": "comparison",
        "requested_by": "qa@example.com",
        "summary": {"executions_total": 1, "executions_finished": 1},
    }
    run = api.post("/api/runs", json=run_payload).json()
    run_id = run["id"]
    assert run["status"] == "queued"
    assert run["spm_ticket"] == "SCENE-202"
    assert "jira_issue" not in run

    resp = api.patch(f"/api/runs/{run_id}", json={"spm_ticket": "SCENE-303"})
    assert resp.status_code == 200
    assert resp.json()["spm_ticket"] == "SCENE-303"

    resp = api.get("/api/runs", params={"project_id": project_id})
    assert resp.status_code == 200
    assert len(resp.json()) == 1

    resp = api.patch(f"/api/runs/{run_id}", json={"status": "executing"})
    assert resp.status_code == 200
    assert resp.json()["status"] == "executing"

    resp = api.get(f"/api/runs/{run_id}")
    assert resp.status_code == 200
    run_data = resp.json()
    assert run_data["purpose"] == "comparison"
    assert run_data["spm_ticket"] == "SCENE-303"

    second_run = api.post(
        "/api/runs",
        json={
            "project_id": project_id,
            "batch_id": batch["id"],
            "purpose": "baseline_recording",
            "status": "finished",
            "jira_issue": "SCENE-999",
        },
    ).json()
    assert second_run["spm_ticket"] == "SCENE-999"

    resp = api.delete(f"/api/runs/{run_id}")
    assert resp.status_code == 204

    resp = api.get("/api/runs", params={"project_id": project_id})
    runs_for_project = resp.json()
    assert len(runs_for_project) == 1
    assert runs_for_project[0]["spm_ticket"] == "SCENE-999"


def test_run_task_subset_is_validated_and_persisted(
    client: Tuple[TestClient, SceneRepository],
) -> None:
    api, _repo = client
    project = api.post("/api/projects", json={"name": "Subset", "slug": "subset"}).json()
    page = api.post(
        "/api/pages",
        json={
            "project_id": project["id"],
            "name": "Home",
            "url": "https://example.com/",
        },
    ).json()
    tasks = [
        api.post(
            "/api/tasks",
            json={
                "project_id": project["id"],
                "page_id": page["id"],
                "name": f"Task {index}",
                "browsers": ["chromium"],
                "viewports": [{"width": 800, "height": 600}],
            },
        ).json()
        for index in range(3)
    ]
    batch = api.post(
        "/api/batches",
        json={
            "project_id": project["id"],
            "name": "Matrix",
            "task_ids": [task["id"] for task in tasks],
        },
    ).json()

    response = api.post(
        "/api/runs",
        json={
            "project_id": project["id"],
            "batch_id": batch["id"],
            "purpose": "baseline_recording",
            "task_ids": [tasks[2]["id"], tasks[0]["id"]],
        },
    )
    assert response.status_code == 201
    assert response.json()["task_ids"] == [tasks[0]["id"], tasks[2]["id"]]

    invalid = api.post(
        "/api/runs",
        json={
            "project_id": project["id"],
            "batch_id": batch["id"],
            "purpose": "baseline_recording",
            "task_ids": ["not-in-batch"],
        },
    )
    assert invalid.status_code == 400
    assert "not part of this batch" in invalid.json()["detail"]
