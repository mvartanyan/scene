from __future__ import annotations

import asyncio
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Generator, Optional, Tuple

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routes import operations


class FakeRepository:
    def __init__(self) -> None:
        self.probe_error: Optional[Exception] = None
        self.probe_result: Dict[str, object] = {"ok": True, "backend": "json"}
        self.dispatcher: Optional[Dict[str, object]] = None
        self.probe_calls = 0
        self.dispatcher_calls = 0

    def probe(self) -> Dict[str, object]:
        self.probe_calls += 1
        if self.probe_error:
            raise self.probe_error
        return self.probe_result

    def dispatcher_status(self) -> Optional[Dict[str, object]]:
        self.dispatcher_calls += 1
        return self.dispatcher


class FakeArtifactStore:
    def __init__(self) -> None:
        self.probe_error: Optional[Exception] = None
        self.probe_result: Dict[str, object] = {"backend": "filesystem"}
        self.probe_calls = 0

    def probe(self) -> Dict[str, object]:
        self.probe_calls += 1
        if self.probe_error:
            raise self.probe_error
        return self.probe_result


class FakeReadinessReport:
    def __init__(self, *, backend: str = "docker", ok: bool = True) -> None:
        self.backend = backend
        self.ok = ok

    def as_dict(self) -> Dict[str, object]:
        return {
            "ok": self.ok,
            "config": {"backend": self.backend, "signed_url": "must-not-leak"},
            "issues": [] if self.ok else [{"code": "runner_invalid", "message": "secret"}],
        }


class FakeOrchestrator:
    def __init__(self) -> None:
        self.backend = "docker"
        self.ready = True
        self.readiness_error: Optional[Exception] = None
        self.readiness_calls = 0

    def deployment_readiness(self) -> FakeReadinessReport:
        self.readiness_calls += 1
        if self.readiness_error:
            raise self.readiness_error
        return FakeReadinessReport(backend=self.backend, ok=self.ready)


@pytest.fixture
def operations_client(
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[
    Tuple[TestClient, FakeRepository, FakeArtifactStore, FakeOrchestrator],
    None,
    None,
]:
    monkeypatch.setenv("SCENE_RUNNER_BACKEND", "docker")
    monkeypatch.setenv("SCENE_STATE_BACKEND", "json")
    monkeypatch.setenv("SCENE_ARTIFACT_STORAGE", "filesystem")
    repo = FakeRepository()
    artifacts = FakeArtifactStore()
    orchestrator = FakeOrchestrator()

    monkeypatch.setattr(operations, "get_repository", lambda: repo)
    monkeypatch.setattr(operations, "get_artifact_store", lambda: artifacts)
    monkeypatch.setattr(operations, "get_orchestrator", lambda: orchestrator)
    with TestClient(app) as client:
        yield client, repo, artifacts, orchestrator


@pytest.mark.unit
def test_healthz_is_process_only_and_startup_does_not_probe_dependencies(
    operations_client: Tuple[
        TestClient,
        FakeRepository,
        FakeArtifactStore,
        FakeOrchestrator,
    ],
) -> None:
    client, repo, artifacts, orchestrator = operations_client
    repo.probe_error = RuntimeError("state-secret")
    artifacts.probe_error = RuntimeError("artifact-secret")
    orchestrator.readiness_error = RuntimeError("runner-secret")

    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert response.headers["cache-control"] == "no-store"
    assert repo.probe_calls == 0
    assert artifacts.probe_calls == 0
    assert orchestrator.readiness_calls == 0


@pytest.mark.unit
def test_readyz_reports_healthy_docker_without_requiring_dispatcher(
    operations_client: Tuple[
        TestClient,
        FakeRepository,
        FakeArtifactStore,
        FakeOrchestrator,
    ],
) -> None:
    client, repo, _artifacts, _orchestrator = operations_client

    response = client.get("/readyz")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ready"
    assert all(check["ok"] for check in body["checks"].values())
    assert body["checks"]["dispatcher"] == {
        "ok": True,
        "required": False,
        "lease_seconds_remaining": 0.0,
    }
    assert repo.dispatcher_calls == 0


@pytest.mark.unit
@pytest.mark.parametrize("failed_dependency", ["state", "artifacts", "runner"])
def test_readyz_returns_503_for_dependency_outages_without_leaking_errors(
    failed_dependency: str,
    operations_client: Tuple[
        TestClient,
        FakeRepository,
        FakeArtifactStore,
        FakeOrchestrator,
    ],
) -> None:
    client, repo, artifacts, orchestrator = operations_client
    secret = f"{failed_dependency}-credential-value"
    if failed_dependency == "state":
        repo.probe_error = RuntimeError(secret)
    elif failed_dependency == "artifacts":
        artifacts.probe_error = RuntimeError(secret)
    else:
        orchestrator.readiness_error = RuntimeError(secret)

    response = client.get("/readyz")

    assert response.status_code == 503
    assert response.json()["status"] == "not_ready"
    assert response.json()["checks"][failed_dependency]["ok"] is False
    assert secret not in response.text


@pytest.mark.unit
def test_readyz_returns_503_for_invalid_runner_configuration(
    operations_client: Tuple[
        TestClient,
        FakeRepository,
        FakeArtifactStore,
        FakeOrchestrator,
    ],
) -> None:
    client, _repo, _artifacts, orchestrator = operations_client
    orchestrator.ready = False

    response = client.get("/readyz")

    assert response.status_code == 503
    runner = response.json()["checks"]["runner"]
    assert runner == {
        "ok": False,
        "backend": "docker",
        "reason": "configuration_invalid",
        "issues": ["runner_invalid"],
    }
    assert "secret" not in response.text


@pytest.mark.unit
def test_readyz_survives_state_backend_initialization_failure(
    monkeypatch: pytest.MonkeyPatch,
    operations_client: Tuple[
        TestClient,
        FakeRepository,
        FakeArtifactStore,
        FakeOrchestrator,
    ],
) -> None:
    client, _repo, _artifacts, _orchestrator = operations_client

    def unavailable_repository():
        raise RuntimeError("database-credential-must-not-leak")

    monkeypatch.setattr(operations, "get_repository", unavailable_repository)

    response = client.get("/readyz")

    assert response.status_code == 503
    assert response.json()["checks"]["state"] == {
        "ok": False,
        "backend": "json",
        "reason": "probe_failed",
    }
    assert "database-credential-must-not-leak" not in response.text


@pytest.mark.unit
def test_readyz_times_out_a_hung_dependency_as_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    operations_client: Tuple[
        TestClient,
        FakeRepository,
        FakeArtifactStore,
        FakeOrchestrator,
    ],
) -> None:
    client, repo, _artifacts, _orchestrator = operations_client
    monkeypatch.setattr(operations, "_READINESS_CHECK_TIMEOUT_SECONDS", 0.01)

    original_probe = repo.probe

    def slow_probe() -> Dict[str, object]:
        time.sleep(0.05)
        return original_probe()

    repo.probe = slow_probe  # type: ignore[method-assign]

    response = client.get("/readyz")

    assert response.status_code == 503
    assert response.json()["checks"]["state"] == {
        "ok": False,
        "backend": "json",
        "reason": "probe_failed",
    }


@pytest.mark.unit
@pytest.mark.parametrize("dispatcher_state", ["missing", "expired"])
def test_readyz_returns_503_when_k3s_dispatcher_heartbeat_is_unhealthy(
    dispatcher_state: str,
    operations_client: Tuple[
        TestClient,
        FakeRepository,
        FakeArtifactStore,
        FakeOrchestrator,
    ],
) -> None:
    client, repo, _artifacts, orchestrator = operations_client
    orchestrator.backend = "k3s"
    now = datetime.now(timezone.utc)
    if dispatcher_state == "expired":
        repo.dispatcher = {
            "owner": "dispatcher-a",
            "heartbeat_at": (now - timedelta(minutes=2)).isoformat(),
            "expires_at": (now - timedelta(minutes=1)).isoformat(),
        }

    response = client.get("/readyz")

    assert response.status_code == 503
    dispatcher = response.json()["checks"]["dispatcher"]
    assert dispatcher["ok"] is False
    assert dispatcher["required"] is True
    assert dispatcher["lease_seconds_remaining"] == 0.0


@pytest.mark.unit
def test_readyz_accepts_fresh_k3s_dispatcher_heartbeat(
    operations_client: Tuple[
        TestClient,
        FakeRepository,
        FakeArtifactStore,
        FakeOrchestrator,
    ],
) -> None:
    client, repo, _artifacts, orchestrator = operations_client
    orchestrator.backend = "k3s"
    now = datetime.now(timezone.utc)
    repo.dispatcher = {
        "owner": "dispatcher-a",
        "heartbeat_at": (now - timedelta(seconds=1)).isoformat(),
        "expires_at": (now + timedelta(seconds=29)).isoformat(),
        "capabilities_ok": True,
        "capabilities_checked_at": (now - timedelta(seconds=1)).isoformat(),
    }

    response = client.get("/readyz")

    assert response.status_code == 200
    dispatcher = response.json()["checks"]["dispatcher"]
    assert dispatcher["ok"] is True
    assert dispatcher["required"] is True
    assert 0 < dispatcher["lease_seconds_remaining"] <= 29


@pytest.mark.unit
def test_readyz_rejects_dispatcher_without_required_kubernetes_permissions(
    operations_client: Tuple[
        TestClient,
        FakeRepository,
        FakeArtifactStore,
        FakeOrchestrator,
    ],
) -> None:
    client, repo, _artifacts, orchestrator = operations_client
    orchestrator.backend = "k3s"
    now = datetime.now(timezone.utc)
    repo.dispatcher = {
        "owner": "dispatcher-a",
        "heartbeat_at": (now - timedelta(seconds=1)).isoformat(),
        "expires_at": (now + timedelta(seconds=29)).isoformat(),
        "capabilities_ok": False,
        "capabilities_checked_at": (now - timedelta(seconds=1)).isoformat(),
    }

    response = client.get("/readyz")

    assert response.status_code == 503
    dispatcher = response.json()["checks"]["dispatcher"]
    assert dispatcher["reason"] == "permissions_unavailable"


@pytest.mark.unit
@pytest.mark.parametrize("stale_field", ["heartbeat_at", "capabilities_checked_at"])
def test_readyz_rejects_stale_dispatcher_signals(
    stale_field: str,
    operations_client: Tuple[
        TestClient,
        FakeRepository,
        FakeArtifactStore,
        FakeOrchestrator,
    ],
) -> None:
    client, repo, _artifacts, orchestrator = operations_client
    orchestrator.backend = "k3s"
    now = datetime.now(timezone.utc)
    repo.dispatcher = {
        "owner": "dispatcher-a",
        "heartbeat_at": (now - timedelta(seconds=1)).isoformat(),
        "expires_at": (now + timedelta(seconds=29)).isoformat(),
        "capabilities_ok": True,
        "capabilities_checked_at": (now - timedelta(seconds=1)).isoformat(),
    }
    age = 16 if stale_field == "heartbeat_at" else 46
    repo.dispatcher[stale_field] = (now - timedelta(seconds=age)).isoformat()

    response = client.get("/readyz")

    assert response.status_code == 503
    assert response.json()["checks"]["dispatcher"]["ok"] is False


@pytest.mark.unit
def test_version_returns_only_explicit_non_secret_build_identity(
    monkeypatch: pytest.MonkeyPatch,
    operations_client: Tuple[
        TestClient,
        FakeRepository,
        FakeArtifactStore,
        FakeOrchestrator,
    ],
) -> None:
    client, _repo, _artifacts, _orchestrator = operations_client
    monkeypatch.setenv("SCENE_VERSION", "1.2.3")
    monkeypatch.setenv("SCENE_GIT_SHA", "abc1234")
    monkeypatch.setenv("SCENE_BUILD_TIME", "2026-07-20T12:00:00Z")
    monkeypatch.setenv("SCENE_ENV", "staging")
    monkeypatch.setenv("SCENE_API_TOKEN", "must-never-appear")

    response = client.get("/version")

    assert response.status_code == 200
    assert response.json() == {
        "version": "1.2.3",
        "git_sha": "abc1234",
        "build_time": "2026-07-20T12:00:00Z",
        "environment": "staging",
    }
    assert "must-never-appear" not in response.text


@pytest.mark.unit
def test_metrics_is_valid_bounded_prometheus_text_even_when_not_ready(
    operations_client: Tuple[
        TestClient,
        FakeRepository,
        FakeArtifactStore,
        FakeOrchestrator,
    ],
) -> None:
    client, repo, _artifacts, _orchestrator = operations_client
    repo.probe_error = RuntimeError("must-never-appear")

    response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain; version=0.0.4")
    assert "# TYPE scene_ready gauge" in response.text
    assert "scene_ready 0" in response.text
    assert 'scene_dependency_ready{dependency="state"} 0' in response.text
    assert response.text.count("scene_dependency_ready{") == 4
    assert "must-never-appear" not in response.text

    sample_pattern = re.compile(
        r'^[a-zA-Z_:][a-zA-Z0-9_:]*(?:\{dependency="[a-z]+"\})? '
        r"[0-9]+(?:\.[0-9]+)?(?:e[+-]?[0-9]+)?$"
    )
    samples = [line for line in response.text.splitlines() if not line.startswith("#")]
    assert samples
    assert all(sample_pattern.fullmatch(sample) for sample in samples)
    assert repo.probe_calls == 0
    assert _artifacts.probe_calls == 0
    assert _orchestrator.readiness_calls == 0


@pytest.mark.unit
def test_metrics_renders_last_readiness_snapshot_without_new_probes(
    operations_client: Tuple[
        TestClient,
        FakeRepository,
        FakeArtifactStore,
        FakeOrchestrator,
    ],
) -> None:
    client, repo, artifacts, orchestrator = operations_client

    ready_response = client.get("/readyz")
    call_counts = (
        repo.probe_calls,
        artifacts.probe_calls,
        orchestrator.readiness_calls,
    )
    repo.probe_error = RuntimeError("metrics-must-not-probe")
    artifacts.probe_error = RuntimeError("metrics-must-not-probe")
    orchestrator.readiness_error = RuntimeError("metrics-must-not-probe")

    metrics_response = client.get("/metrics")

    assert ready_response.status_code == 200
    assert "scene_ready 1" in metrics_response.text
    assert (
        repo.probe_calls,
        artifacts.probe_calls,
        orchestrator.readiness_calls,
    ) == call_counts


@pytest.mark.unit
@pytest.mark.asyncio
async def test_concurrent_hung_readiness_probes_are_single_flight_and_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = FakeRepository()
    artifacts = FakeArtifactStore()
    orchestrator = FakeOrchestrator()
    probe_started = threading.Event()
    release_probe = threading.Event()
    main_thread = threading.get_ident()
    orchestrator_factory_threads = []

    def hung_probe() -> Dict[str, object]:
        repo.probe_calls += 1
        probe_started.set()
        release_probe.wait(timeout=1.0)
        return repo.probe_result

    def orchestrator_factory() -> FakeOrchestrator:
        orchestrator_factory_threads.append(threading.get_ident())
        return orchestrator

    repo.probe = hung_probe  # type: ignore[method-assign]
    monkeypatch.setattr(operations, "get_repository", lambda: repo)
    monkeypatch.setattr(operations, "get_artifact_store", lambda: artifacts)
    monkeypatch.setattr(operations, "get_orchestrator", orchestrator_factory)
    monkeypatch.setattr(operations, "_READINESS_CHECK_TIMEOUT_SECONDS", 0.02)

    try:
        first, second = await asyncio.gather(
            operations.collect_readiness(),
            operations.collect_readiness(),
        )

        assert probe_started.is_set()
        assert first["status"] == "not_ready"
        assert second["status"] == "not_ready"
        assert repo.probe_calls == 1
        assert artifacts.probe_calls == 1
        assert orchestrator.readiness_calls == 1
        assert orchestrator_factory_threads
        assert all(thread_id != main_thread for thread_id in orchestrator_factory_threads)

        started_at = time.monotonic()
        third = await operations.collect_readiness()
        assert time.monotonic() - started_at < 0.02
        assert third["status"] == "not_ready"
        assert repo.probe_calls == 1
        assert artifacts.probe_calls == 1
        assert orchestrator.readiness_calls == 1

        metrics_snapshot = operations._snapshot_for_metrics()
        assert metrics_snapshot["status"] == "not_ready"
        assert repo.probe_calls == 1
        assert artifacts.probe_calls == 1
        assert orchestrator.readiness_calls == 1
    finally:
        release_probe.set()

    for _attempt in range(100):
        await asyncio.sleep(0.005)
        if operations._readiness_refresh_task is None:
            break

    recovered = await operations.collect_readiness()
    assert recovered["status"] == "ready"
    assert repo.probe_calls == 1
