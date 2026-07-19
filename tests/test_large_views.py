from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Generator, Tuple

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services.storage import LocalDynamoStorage, SceneRepository, get_repository


LARGE_ITEM_COUNT = 205
RUN_COUNT = 70
EXECUTION_COUNT = 125


def _timestamp(index: int) -> str:
    return (datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=index)).isoformat()


def _write_large_state(path: Path) -> None:
    project_id = "large-project"
    batch_id = "large-batch"
    pages = {}
    tasks = {}
    task_ids = []
    for index in range(LARGE_ITEM_COUNT):
        page_id = f"page-{index:03d}"
        task_id = f"task-{index:03d}"
        created_at = _timestamp(index)
        pages[page_id] = {
            "id": page_id,
            "project_id": project_id,
            "name": f"Page {index:03d}",
            "url": f"https://example.com/{index}",
            "reference_url": None,
            "preparatory_js": None,
            "preparatory_actions": [],
            "basic_auth_username": None,
            "basic_auth_password": None,
            "created_at": created_at,
            "updated_at": created_at,
        }
        tasks[task_id] = {
            "id": task_id,
            "project_id": project_id,
            "page_id": page_id,
            "name": f"Task {index:03d}",
            "task_js": None,
            "task_actions": [],
            "browsers": ["chromium", "firefox"],
            "viewports": [
                {"width": 1280, "height": 720},
                {"width": 390, "height": 844},
            ],
            "created_at": created_at,
            "updated_at": created_at,
        }
        task_ids.append(task_id)

    runs = {}
    for index in range(RUN_COUNT):
        run_id = f"run-{index:03d}"
        created_at = _timestamp(1000 + index)
        runs[run_id] = {
            "id": run_id,
            "project_id": project_id,
            "batch_id": batch_id,
            "baseline_id": None,
            "purpose": "baseline_recording",
            "status": "executing" if index == 0 else "finished",
            "requested_by": "large-view-test",
            "note": None,
            "spm_ticket": "SCENE-17",
            "created_at": created_at,
            "updated_at": created_at,
            "summary": {
                "executions_total": EXECUTION_COUNT if index == 0 else 0,
                "executions_finished": 0,
                "executions_failed": 0,
                "executions_cancelled": 0,
                "diff_average": 0.0,
                "diff_maximum": 0.0,
                "diff_samples": 0,
            },
            "timeout_seconds": 600,
            "task_ids": None,
        }

    executions = {}
    for index in range(EXECUTION_COUNT):
        execution_id = f"execution-{index:03d}"
        created_at = _timestamp(2000 + index)
        executions[execution_id] = {
            "id": execution_id,
            "run_id": "run-000",
            "project_id": project_id,
            "batch_id": batch_id,
            "task_id": task_ids[index % LARGE_ITEM_COUNT],
            "task_name": f"Task {index % LARGE_ITEM_COUNT:03d}",
            "page_id": f"page-{index % LARGE_ITEM_COUNT:03d}",
            "browser": "chromium",
            "viewport": {"width": 1280, "height": 720},
            "status": "queued",
            "sequence": index,
            "artifacts": {},
            "diff": None,
            "message": None,
            "created_at": created_at,
            "updated_at": created_at,
        }

    state = {
        "version": 1,
        "projects": {
            project_id: {
                "id": project_id,
                "name": "Large Project",
                "slug": "large-project",
                "description": "Synthetic SCENE-17 fixture",
                "created_at": _timestamp(0),
                "updated_at": _timestamp(0),
            }
        },
        "pages": pages,
        "tasks": tasks,
        "batches": {
            batch_id: {
                "id": batch_id,
                "project_id": project_id,
                "name": "Large Batch",
                "description": "All synthetic tasks",
                "task_ids": task_ids,
                "spm_ticket": "SCENE-17",
                "run_diff_threshold": 1.0,
                "execution_diff_threshold": 1.0,
                "created_at": _timestamp(500),
                "updated_at": _timestamp(500),
            }
        },
        "runs": runs,
        "executions": executions,
        "baselines": {},
        "config": {},
    }
    path.write_text(json.dumps(state), encoding="utf-8")


@pytest.fixture
def large_client(tmp_path: Path) -> Generator[Tuple[TestClient, SceneRepository], None, None]:
    state_path = tmp_path / "large.json"
    _write_large_state(state_path)
    repo = SceneRepository(LocalDynamoStorage(state_path))

    def override_repo() -> SceneRepository:
        return repo

    app.dependency_overrides[get_repository] = override_repo
    with TestClient(app) as client:
        yield client, repo
    app.dependency_overrides.clear()


def test_large_project_tabs_are_lazy_paginated_and_fast(
    large_client: Tuple[TestClient, SceneRepository],
) -> None:
    client, _repo = large_client

    started = time.perf_counter()
    pages = client.get("/projects/large-project/detail", params={"tab": "pages"})
    elapsed = time.perf_counter() - started
    assert pages.status_code == 200
    assert elapsed < 2.0
    assert pages.text.count('data-testid="page-list-item"') == 25
    assert 'data-testid="task-list-item"' not in pages.text
    assert 'data-testid="pages-pagination"' in pages.text
    assert "1-25 of 205" in pages.text
    assert len(pages.content) < 150_000

    tasks = client.get(
        "/projects/large-project/detail",
        params={"tab": "tasks", "item_page": 9},
    )
    assert tasks.status_code == 200
    assert tasks.text.count('data-testid="task-list-item"') == 5
    assert 'data-testid="page-list-item"' not in tasks.text
    assert "201-205 of 205" in tasks.text
    assert len(tasks.content) < 250_000

    batches = client.get("/projects/large-project/detail", params={"tab": "batches"})
    assert batches.status_code == 200
    assert batches.text.count('data-testid="batch-list-item"') == 1
    assert batches.text.count('name="task_ids"') == LARGE_ITEM_COUNT
    assert len(batches.content) < 250_000


def test_run_and_execution_views_have_bounded_payloads(
    large_client: Tuple[TestClient, SceneRepository],
) -> None:
    client, _repo = large_client

    started = time.perf_counter()
    dashboard = client.get("/runs")
    elapsed = time.perf_counter() - started
    assert dashboard.status_code == 200
    assert elapsed < 2.0
    assert dashboard.text.count('data-testid="run-list-item"') == 25
    assert 'data-testid="run-pagination"' in dashboard.text
    assert len(dashboard.content) < 500_000

    second_page = client.get("/runs", params={"run_page": 2})
    assert second_page.status_code == 200
    assert second_page.text.count('data-testid="run-list-item"') == 25
    assert "26-50 of 70" in second_page.text

    overlay = client.get("/runs/run-000/overlay")
    assert overlay.status_code == 200
    assert overlay.text.count('data-testid="execution-list-item"') == 50
    assert 'data-testid="execution-pagination"' in overlay.text
    assert 'data-execution-page="1"' in overlay.text
    assert len(overlay.content) < 200_000

    final_page = client.get("/runs/run-000/overlay", params={"execution_page": 3})
    assert final_page.status_code == 200
    assert final_page.text.count('data-testid="execution-list-item"') == 25
    assert 'data-execution-page="3"' in final_page.text
    assert "101-125 of 125" in final_page.text

    viewer = client.get("/runs/run-000/executions/execution-124/viewer")
    assert viewer.status_code == 200
    assert "Task 124" in viewer.text
