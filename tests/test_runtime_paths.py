from __future__ import annotations

from pathlib import Path

from app.services import artifacts as artifact_service
from app.services import storage as storage_service


def test_default_runtime_paths_stay_under_local_scene_dir(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv(storage_service.SCENE_STATE_PATH_ENV, raising=False)
    monkeypatch.delenv(artifact_service.SCENE_ARTIFACT_ROOT_ENV, raising=False)
    monkeypatch.setattr(storage_service, "_repository", None)
    monkeypatch.setattr(artifact_service, "_artifact_store", None)

    repo = storage_service.get_repository()
    repo.create_project({"name": "Local", "slug": "local"})
    artifact_store = artifact_service.get_artifact_store()

    assert (tmp_path / ".scene" / "dev.dynamodb.json").exists()
    assert artifact_store.root == (tmp_path / ".scene" / "artifacts").resolve()
    assert not (tmp_path / "dev.dynamodb.json").exists()


def test_runtime_paths_can_be_configured_with_environment(
    monkeypatch,
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state" / "scene.json"
    artifact_root = tmp_path / "runtime-artifacts"
    monkeypatch.setenv(storage_service.SCENE_STATE_PATH_ENV, str(state_path))
    monkeypatch.setenv(artifact_service.SCENE_ARTIFACT_ROOT_ENV, str(artifact_root))
    monkeypatch.setattr(storage_service, "_repository", None)
    monkeypatch.setattr(artifact_service, "_artifact_store", None)

    repo = storage_service.get_repository()
    repo.create_project({"name": "Configured", "slug": "configured"})
    artifact_store = artifact_service.get_artifact_store()

    assert state_path.exists()
    assert artifact_store.root == artifact_root.resolve()
