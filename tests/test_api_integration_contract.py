from __future__ import annotations

from typing import Generator, Tuple

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routes import api as api_routes
from app.routes import runs as runs_routes
from app.schemas import BaselineStatus, ExecutionStatus, RunStatus
from app.services.storage import LocalDynamoStorage, SceneRepository, get_repository


@pytest.fixture
def client(tmp_path) -> Generator[Tuple[TestClient, SceneRepository], None, None]:
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


def _seed_batch(repo: SceneRepository):
    project = repo.create_project({"name": "Acme", "slug": "acme"})
    page = repo.create_page(
        {
            "project_id": project["id"],
            "name": "Home",
            "url": "https://example.com/",
        }
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
        {
            "project_id": project["id"],
            "name": "Success criteria",
            "task_ids": [task["id"]],
            "run_diff_threshold": 2.0,
            "execution_diff_threshold": 4.0,
        }
    )
    baseline = repo.create_baseline(
        {
            "project_id": project["id"],
            "batch_id": batch["id"],
            "status": BaselineStatus.completed.value,
            "items": [
                {
                    "task_id": task["id"],
                    "task_name": task["name"],
                    "browser": "chromium",
                    "viewport": {"width": 1280, "height": 720},
                    "artifacts": {
                        "baseline": {
                            "kind": "baseline",
                            "path": f"baselines/{batch['id']}/chromium.png",
                            "url": f"/artifacts/baselines/{batch['id']}/chromium.png",
                        }
                    },
                }
            ],
        }
    )
    return project, task, batch, baseline


def test_success_criteria_integration_contract(
    client: Tuple[TestClient, SceneRepository],
) -> None:
    api, repo = client
    project, task, batch, baseline = _seed_batch(repo)

    projects_resp = api.get("/api/projects")
    assert projects_resp.status_code == 200
    assert projects_resp.json()[0]["id"] == project["id"]

    batches_resp = api.get(f"/api/projects/{project['id']}/batches")
    assert batches_resp.status_code == 200
    assert batches_resp.json()[0]["id"] == batch["id"]

    all_batches_resp = api.get("/api/batches", params={"project_id": project["id"]})
    assert all_batches_resp.status_code == 200
    assert all_batches_resp.json()[0]["id"] == batch["id"]

    candidates_resp = api.get(
        "/api/check-candidates", params={"project_id": project["id"]}
    )
    assert candidates_resp.status_code == 200
    candidates = candidates_resp.json()
    assert candidates == [
        {
            "project_id": project["id"],
            "project_name": "Acme",
            "batch_id": batch["id"],
            "batch_name": "Success criteria",
            "task_count": 1,
            "latest_baseline_id": baseline["id"],
            "completed_baseline_count": 1,
            "run_diff_threshold": 2.0,
            "execution_diff_threshold": 4.0,
            "can_compare": True,
            "unavailable_reasons": [],
        }
    ]

    launch_resp = api.post(
        f"/api/batches/{batch['id']}/comparison-runs",
        json={"requested_by": "spm", "note": "SCENE-6 contract smoke"},
    )
    assert launch_resp.status_code == 201
    launched = launch_resp.json()
    run_id = launched["run_id"]
    assert launched["status"] == RunStatus.queued.value
    assert launched["batch_id"] == batch["id"]
    assert launched["baseline_id"] == baseline["id"]
    assert launched["run_diff_threshold"] == 2.0
    assert launched["execution_diff_threshold"] == 4.0
    assert launched["threshold_passed"] is None

    finished_execution = repo.create_execution(
        {
            "run_id": run_id,
            "project_id": project["id"],
            "batch_id": batch["id"],
            "task_id": task["id"],
            "task_name": task["name"],
            "browser": "chromium",
            "viewport": {"width": 1280, "height": 720},
            "status": ExecutionStatus.finished.value,
            "artifacts": {
                "diff": {
                    "kind": "diff",
                    "path": f"runs/{run_id}/finished/diff.png",
                    "url": f"/artifacts/runs/{run_id}/finished/diff.png",
                }
            },
            "diff": {"percentage": 1.25},
        }
    )
    failed_execution = repo.create_execution(
        {
            "run_id": run_id,
            "project_id": project["id"],
            "batch_id": batch["id"],
            "task_id": task["id"],
            "task_name": task["name"],
            "browser": "firefox",
            "viewport": {"width": 1280, "height": 720},
            "status": ExecutionStatus.failed.value,
            "message": "Playwright timeout waiting for selector",
            "diff": {"percentage": 5.0},
        }
    )
    assert finished_execution["id"]
    assert failed_execution["id"]
    repo.update_run(
        run_id,
        {
            "status": RunStatus.finished.value,
            "summary": {
                "executions_total": 2,
                "executions_finished": 1,
                "executions_failed": 1,
                "executions_cancelled": 0,
                "diff_average": 3.125,
                "diff_maximum": 5.0,
                "diff_samples": 2,
            },
        },
    )

    result_resp = api.get(f"/api/runs/{run_id}/result")
    assert result_resp.status_code == 200
    result = result_resp.json()
    assert result["run_id"] == run_id
    assert result["status"] == RunStatus.finished.value
    assert result["executions_total"] == 2
    assert result["executions_finished"] == 1
    assert result["executions_failed"] == 1
    assert result["executions_cancelled"] == 0
    assert result["diff_average"] == 3.125
    assert result["diff_maximum"] == 5.0
    assert result["threshold_passed"] is False
    assert result["threshold_failures"] == [
        "run_diff_average 3.1250 exceeds threshold 2.0000",
        f"execution {failed_execution['id']} ended with status failed",
        f"execution {failed_execution['id']} diff 5.0000 exceeds threshold 4.0000",
    ]
    assert result["artifact_url"] == (
        f"http://testserver/artifacts/runs/{run_id}/finished/diff.png"
    )
    assert result["viewer_url"] == (
        f"http://testserver/runs/{run_id}/executions/"
        f"{finished_execution['id']}/viewer"
    )
    assert result["failure_statuses"] == [
        {
            "scope": "execution",
            "status": ExecutionStatus.failed.value,
            "message": "Playwright timeout waiting for selector",
            "execution_id": failed_execution["id"],
            "task_id": task["id"],
            "task_name": task["name"],
            "browser": "firefox",
            "viewport": {"width": 1280, "height": 720},
        }
    ]


def test_batch_comparison_run_requires_completed_baseline(
    client: Tuple[TestClient, SceneRepository],
) -> None:
    api, repo = client
    project = repo.create_project({"name": "Acme", "slug": "acme"})
    batch = repo.create_batch(
        {
            "project_id": project["id"],
            "name": "No baseline",
            "task_ids": [],
        }
    )

    resp = api.post(f"/api/batches/{batch['id']}/comparison-runs", json={})

    assert resp.status_code == 400
    assert resp.json()["detail"] == "Batch has no completed baseline for comparison"


def test_batch_comparison_run_rejects_completed_baseline_missing_coverage(
    client: Tuple[TestClient, SceneRepository],
) -> None:
    api, repo = client
    project, task, batch, baseline = _seed_batch(repo)
    repo.update_baseline(baseline["id"], {"items": []})

    candidates_resp = api.get(
        "/api/check-candidates", params={"project_id": project["id"]}
    )
    assert candidates_resp.status_code == 200
    candidate = candidates_resp.json()[0]
    assert candidate["can_compare"] is False
    assert candidate["unavailable_reasons"] == ["latest_baseline_missing_coverage"]

    resp = api.post(f"/api/batches/{batch['id']}/comparison-runs", json={})

    assert resp.status_code == 400
    assert resp.json()["detail"] == (
        "Baseline does not cover all batch task/browser/viewport combinations: "
        f"{task['name']} / chromium / 1280x720"
    )
