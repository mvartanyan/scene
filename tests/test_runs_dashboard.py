from __future__ import annotations

from typing import Generator, Tuple

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services.storage import LocalDynamoStorage, SceneRepository, get_repository


@pytest.fixture
def client(tmp_path) -> Generator[Tuple[TestClient, SceneRepository], None, None]:
    storage = LocalDynamoStorage(tmp_path / "db.json")
    repo = SceneRepository(storage)

    def override_repo() -> SceneRepository:
        return repo

    app.dependency_overrides[get_repository] = override_repo
    with TestClient(app) as test_client:
        yield test_client, repo
    app.dependency_overrides.clear()


def test_runs_dashboard_renders_without_runs(client: Tuple[TestClient, SceneRepository]) -> None:
    api, _repo = client
    resp = api.get("/runs")
    assert resp.status_code == 200
    assert "Select a run from the log to inspect details" in resp.text
