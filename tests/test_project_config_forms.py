from __future__ import annotations

from typing import Generator, Tuple

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services.storage import LocalDynamoStorage, SceneRepository, get_repository


@pytest.fixture
def client(tmp_path) -> Generator[Tuple[TestClient, SceneRepository], None, None]:
    repo = SceneRepository(LocalDynamoStorage(tmp_path / "db.json"))

    def override_repo() -> SceneRepository:
        return repo

    app.dependency_overrides[get_repository] = override_repo
    with TestClient(app) as test_client:
        yield test_client, repo
    app.dependency_overrides.clear()


def _seed_project(repo: SceneRepository) -> dict:
    project = repo.create_project({"name": "Config", "slug": "config"})
    page = repo.create_page(
        {
            "project_id": project["id"],
            "name": "Home",
            "url": "https://example.com/",
            "reference_url": "https://example.org/",
            "preparatory_js": "window.before = true;",
            "preparatory_actions": [{"type": "wait", "wait_ms": 10}],
            "basic_auth_username": "scene",
            "basic_auth_password": "secret",
        }
    )
    task = repo.create_task(
        {
            "project_id": project["id"],
            "page_id": page["id"],
            "name": "Screenshot",
            "task_js": "window.after = true;",
            "task_actions": [{"type": "wait", "wait_ms": 10}],
            "browsers": ["chromium"],
            "viewports": [{"width": 1280, "height": 720}],
        }
    )
    batch = repo.create_batch(
        {
            "project_id": project["id"],
            "name": "Batch",
            "description": "Old description",
            "task_ids": [task["id"]],
            "jira_issue": "SCENE-5",
            "run_diff_threshold": 2.0,
            "execution_diff_threshold": 1.0,
        }
    )
    return {"project": project, "page": page, "task": task, "batch": batch}


def test_project_form_edits_can_clear_optional_fields(
    client: Tuple[TestClient, SceneRepository],
) -> None:
    api, repo = client
    seeded = _seed_project(repo)
    project = seeded["project"]
    page = seeded["page"]
    task = seeded["task"]
    batch = seeded["batch"]

    page_resp = api.post(
        f"/pages/{page['id']}/edit",
        data={
            "name": page["name"],
            "url": page["url"],
            "reference_url": "",
            "preparatory_js": "",
            "preparatory_actions": "",
            "basic_auth_username": "",
            "basic_auth_password": "",
            "active_tab": "pages",
        },
    )
    assert page_resp.status_code == 200
    updated_page = repo.get_page(page["id"])
    assert updated_page["reference_url"] is None
    assert updated_page["preparatory_js"] is None
    assert updated_page["preparatory_actions"] == []
    assert updated_page["basic_auth_username"] is None
    assert updated_page["basic_auth_password"] is None

    task_resp = api.post(
        f"/tasks/{task['id']}/edit",
        data={
            "name": task["name"],
            "page_id": page["id"],
            "browsers": ["chromium"],
            "viewports": ["1280x720"],
            "task_js": "",
            "task_actions": "",
            "active_tab": "tasks",
        },
    )
    assert task_resp.status_code == 200
    updated_task = repo.get_task(task["id"])
    assert updated_task["task_js"] is None
    assert updated_task["task_actions"] == []

    batch_resp = api.post(
        f"/batches/{batch['id']}/edit",
        data={
            "name": batch["name"],
            "description": "",
            "task_ids": [task["id"]],
            "jira_issue": "",
            "run_diff_threshold": "",
            "execution_diff_threshold": "",
            "active_tab": "batches",
        },
    )
    assert batch_resp.status_code == 200
    updated_batch = repo.get_batch(batch["id"])
    assert updated_batch["description"] is None
    assert updated_batch["jira_issue"] is None
    assert updated_batch["run_diff_threshold"] is None
    assert updated_batch["execution_diff_threshold"] is None
    assert repo.get_project(project["id"])


def test_invalid_action_json_returns_project_partial_error(
    client: Tuple[TestClient, SceneRepository],
) -> None:
    api, repo = client
    seeded = _seed_project(repo)
    page = seeded["page"]

    resp = api.post(
        f"/pages/{page['id']}/edit",
        data={
            "name": page["name"],
            "url": page["url"],
            "preparatory_actions": "[",
            "active_tab": "pages",
        },
    )

    assert resp.status_code == 200
    assert "Preparatory actions must be valid JSON." in resp.text
    assert repo.get_page(page["id"])["preparatory_actions"] == [
        {"type": "wait", "wait_ms": 10}
    ]
