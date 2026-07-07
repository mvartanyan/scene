from __future__ import annotations

from pathlib import Path

from app.services import artifacts as artifact_module
from app.services.artifacts import get_artifact_store
from app.services.orchestrator import DockerPlaywrightRunner
from app.services.storage import LocalDynamoStorage, SceneRepository


def test_artifact_store_uses_staging_env(monkeypatch, tmp_path: Path) -> None:
    artifact_root = tmp_path / "staging-artifacts"
    monkeypatch.setenv("SCENE_ARTIFACT_ROOT", str(artifact_root))
    monkeypatch.setenv("SCENE_ARTIFACT_BASE_URL", "https://scene.example.test/artifacts")
    monkeypatch.setattr(artifact_module, "_artifact_store", None)

    store = get_artifact_store()

    assert store.root == artifact_root.resolve()
    assert store.url(store.root / "runs" / "r1" / "observed.png") == (
        "https://scene.example.test/artifacts/runs/r1/observed.png"
    )


def test_repository_config_applies_staging_env_overrides(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SCENE_HOST_URL", "https://scene.example.test")
    monkeypatch.setenv("SCENE_MAX_CONCURRENT_EXECUTIONS", "2")
    monkeypatch.setenv("SCENE_RUN_TIMEOUT_SECONDS", "900")
    monkeypatch.setenv("SCENE_CAPTURE_DELAY_MS", "7500")
    monkeypatch.setenv("SCENE_DIFF_PIXEL_TOLERANCE", "3")

    repo = SceneRepository(LocalDynamoStorage(tmp_path / "state.json"))
    config = repo.get_config()

    assert config["scene_host_url"] == "https://scene.example.test"
    assert config["max_concurrent_executions"] == 2
    assert config["run_timeout_seconds"] == 900
    assert config["capture_post_wait_ms"] == 7500
    assert config["diff_pixel_tolerance"] == 3


def test_runner_image_uses_staging_env(monkeypatch) -> None:
    monkeypatch.setenv("SCENE_RUNNER_IMAGE", "scene-playwright-runner:1.47.0-jammy")

    runner = DockerPlaywrightRunner()

    assert runner.image == "scene-playwright-runner:1.47.0-jammy"
