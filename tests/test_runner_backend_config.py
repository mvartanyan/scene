from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from app.services.runner_backend import (
    load_runner_runtime_config,
    validate_runner_runtime_config,
)


@pytest.mark.unit
def test_docker_backend_defaults_to_local_inline_launch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("SCENE_RUNNER_BACKEND", raising=False)
    monkeypatch.delenv("SCENE_RUNNER_IMAGE_AUTOBUILD", raising=False)
    monkeypatch.delenv("SCENE_ENV", raising=False)

    config = load_runner_runtime_config(
        {"scene_host_url": "http://host.docker.internal:8000", "max_concurrent_executions": 4},
        artifact_root=tmp_path / "artifacts",
    )

    assert config.backend == "docker"
    assert config.supports_inline_launch
    assert config.allow_image_build
    assert config.add_host_gateway
    assert config.callback_base_url == "http://host.docker.internal:8000"


@pytest.mark.unit
def test_k3s_backend_uses_cluster_service_url_and_pvc(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SCENE_RUNNER_BACKEND", "k3s")
    monkeypatch.setenv("SCENE_RUNNER_IMAGE", "registry.example.com/scene-runner:1.47.0-20260708")
    monkeypatch.setenv("SCENE_RUNNER_IMAGE_AUTOBUILD", "false")
    monkeypatch.setenv("SCENE_K3S_SERVICE_URL", "http://scene.scene.svc.cluster.local:8000")
    monkeypatch.setenv("SCENE_ARTIFACT_STORAGE", "pvc")
    monkeypatch.setenv("SCENE_ARTIFACT_PVC_CLAIM", "scene-artifacts")

    config = load_runner_runtime_config(
        {"scene_host_url": "http://host.docker.internal:8000", "max_concurrent_executions": 2},
        artifact_root=tmp_path / "artifacts",
    )
    report = validate_runner_runtime_config(config)

    assert config.backend == "k3s"
    assert not config.supports_inline_launch
    assert config.callback_base_url == "http://scene.scene.svc.cluster.local:8000"
    assert report.ok


@pytest.mark.unit
def test_k3s_backend_accepts_private_s3_artifacts(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SCENE_RUNNER_BACKEND", "k3s")
    monkeypatch.setenv("SCENE_RUNNER_IMAGE", "registry.example.com/scene-runner:1.47.0-20260719")
    monkeypatch.setenv("SCENE_RUNNER_IMAGE_AUTOBUILD", "false")
    monkeypatch.setenv("SCENE_K3S_SERVICE_URL", "http://scene.scene.svc.cluster.local:8000")
    monkeypatch.setenv("SCENE_ARTIFACT_STORAGE", "s3")
    monkeypatch.setenv("SCENE_S3_BUCKET", "scene-private")

    config = load_runner_runtime_config(
        {"max_concurrent_executions": 2},
        artifact_root=tmp_path / "artifacts",
    )
    report = validate_runner_runtime_config(config)

    assert config.artifact_storage == "s3"
    assert config.s3_bucket == "scene-private"
    assert report.ok


@pytest.mark.unit
def test_k3s_backend_rejects_host_docker_and_local_artifacts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("SCENE_RUNNER_BACKEND", "k3s")
    monkeypatch.setenv("SCENE_RUNNER_IMAGE", "scene-playwright-runner:latest")
    monkeypatch.setenv("SCENE_RUNNER_IMAGE_AUTOBUILD", "true")
    monkeypatch.delenv("SCENE_K3S_SERVICE_URL", raising=False)
    monkeypatch.setenv("SCENE_ARTIFACT_STORAGE", "filesystem")

    config = load_runner_runtime_config(
        {"scene_host_url": "http://host.docker.internal:8000", "max_concurrent_executions": 3},
        artifact_root=tmp_path / "artifacts",
    )
    report = validate_runner_runtime_config(config)
    codes = {issue.code for issue in report.issues if issue.level == "error"}

    assert not report.ok
    assert "host_docker_internal_for_cluster" in codes
    assert "image_autobuild_enabled" in codes
    assert "unpinned_runner_image" in codes
    assert "local_artifact_storage_for_cluster" in codes
    assert "missing_k3s_service_url" in codes


@pytest.mark.unit
def test_runner_readiness_script_rejects_host_docker_for_k3s(tmp_path: Path) -> None:
    proc = subprocess.run(
        [
            sys.executable,
            "scripts/runner_readiness.py",
            "--callback-url",
            "http://host.docker.internal:8000",
            "--artifact-dir",
            str(tmp_path),
            "--expected-storage",
            "pvc",
            "--json",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 1
    assert "host.docker.internal is not valid for k3s runner pods" in proc.stdout


@pytest.mark.unit
def test_runner_readiness_s3_uses_only_ephemeral_workspace(tmp_path: Path) -> None:
    proc = subprocess.run(
        [
            sys.executable,
            "scripts/runner_readiness.py",
            "--callback-url",
            "http://127.0.0.1:9",
            "--artifact-dir",
            str(tmp_path),
            "--expected-storage",
            "s3",
            "--json",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    payload = json.loads(proc.stdout)
    artifact_check = next(check for check in payload["checks"] if check["name"] == "artifact_write")
    assert artifact_check["ok"]
    assert "ephemeral workspace" in artifact_check["message"]
