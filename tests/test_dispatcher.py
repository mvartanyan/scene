from __future__ import annotations

import json
import time
from pathlib import Path
from threading import Barrier, Lock, Thread

import pytest
from PIL import Image

from app.schemas import RunPurpose
from app.services.artifacts import ArtifactStore
from app.services.dispatcher import KubernetesDispatcher
from app.services.kubernetes_runner import KubernetesExecutionStatus
from app.services.orchestrator import RunOrchestrator
from app.services.storage import LocalDynamoStorage, SceneRepository


class FakeKubernetesRunner:
    def __init__(self) -> None:
        self.phases: dict[str, KubernetesExecutionStatus] = {}
        self.created: list[str] = []
        self.deleted: list[str] = []
        self.tokens: dict[str, str] = {}
        self.configs: dict[str, dict] = {}
        self.cleanup_results: list[bool] = []

    def build_secret(self, **kwargs):
        self.tokens[kwargs["name"]] = kwargs["callback_token"]
        self.configs[kwargs["name"]] = kwargs["runner_config"]
        return {"name": kwargs["name"], "digest": kwargs["spec_digest"]}

    def build_job(self, **kwargs):
        return {"name": kwargs["name"], "digest": kwargs["spec_digest"]}

    def create_or_adopt(self, *, secret, job, spec_digest):
        self.created.append(job["name"])
        self.phases[job["name"]] = KubernetesExecutionStatus("active", pod_name="pod-1")

    def delete(self, *, job_name: str, secret_name: str):
        self.deleted.append(job_name)
        confirmed = self.cleanup_results.pop(0) if self.cleanup_results else True
        if confirmed:
            self.phases.pop(job_name, None)
        return confirmed

    def status(self, job_name: str) -> KubernetesExecutionStatus:
        return self.phases.get(job_name, KubernetesExecutionStatus("missing", "JobNotFound"))

    def logs(self, pod_name: str, *, tail_lines: int = 500) -> str:
        return "runner output"

    def probe_permissions(self) -> dict[str, bool]:
        return {
            "create:batch:jobs": True,
            "create:core:secrets": True,
            "get:core:pods/log": True,
        }


def _runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCENE_RUNNER_BACKEND", "k3s")
    monkeypatch.setenv("SCENE_RUNNER_IMAGE", "registry.example/runner@sha256:" + "a" * 64)
    monkeypatch.setenv("SCENE_RUNNER_IMAGE_AUTOBUILD", "false")
    monkeypatch.setenv("SCENE_K3S_SERVICE_URL", "http://scene.scene.svc.cluster.local")
    monkeypatch.setenv("SCENE_ARTIFACT_STORAGE", "s3")
    monkeypatch.setenv("SCENE_S3_BUCKET", "scene-private")
    monkeypatch.setenv("SCENE_MAX_CONCURRENT_EXECUTIONS", "1")


def _system(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    _runtime(monkeypatch)
    repo = SceneRepository(LocalDynamoStorage(tmp_path / "state.json"))
    artifacts = ArtifactStore(root=tmp_path / "artifacts")
    orchestrator = RunOrchestrator(repo=repo, artifacts=artifacts, auto_start=False)
    project = repo.create_project({"name": "Project", "slug": "project"})
    page = repo.create_page(
        {"project_id": project["id"], "name": "Page", "url": "https://example.test"}
    )
    task = repo.create_task(
        {
            "project_id": project["id"],
            "page_id": page["id"],
            "name": "Smoke",
            "browsers": ["chromium"],
            "viewports": [{"width": 800, "height": 600}],
        }
    )
    batch = repo.create_batch(
        {"project_id": project["id"], "name": "Batch", "task_ids": [task["id"]]}
    )
    run = repo.create_run(
        {
            "project_id": project["id"],
            "batch_id": batch["id"],
            "purpose": RunPurpose.baseline_recording.value,
            "requested_by": "test",
        }
    )
    orchestrator.prepare_durable_run(run["id"])
    runner = FakeKubernetesRunner()
    dispatcher = KubernetesDispatcher(
        repo=repo,
        orchestrator=orchestrator,
        runner=runner,
        owner="dispatcher-test",
    )
    return repo, orchestrator, runner, dispatcher, run


@pytest.mark.unit
def test_dispatcher_creates_one_durable_job_and_marks_execution(monkeypatch, tmp_path: Path) -> None:
    repo, _orchestrator, runner, dispatcher, run = _system(monkeypatch, tmp_path)

    stats = dispatcher.run_once()
    execution = repo.list_executions(run_id=run["id"])[0]

    assert stats["leader"]
    assert stats["capabilities_ok"]
    assert stats["dispatched"] == 1
    assert execution["status"] == "executing"
    assert execution["dispatch_generation"] == 1
    assert execution["callback_token_sha256"]
    assert "callback_token" not in execution
    assert runner.created == [execution["kubernetes_job_name"]]


@pytest.mark.unit
def test_dispatcher_deletes_job_before_terminalizing_cancellation(monkeypatch, tmp_path: Path) -> None:
    repo, _orchestrator, runner, dispatcher, run = _system(monkeypatch, tmp_path)
    dispatcher.run_once()
    execution = repo.list_executions(run_id=run["id"])[0]
    repo.request_execution_cancellation(execution["id"], reason="Run cancelled manually")

    dispatcher.run_once()
    updated = repo.get_execution(execution["id"])

    assert runner.deleted == [execution["kubernetes_job_name"]]
    assert updated["status"] == "cancelled"
    assert updated["kubernetes_cleaned_at"]
    assert repo.get_run(run["id"])["status"] == "cancelled"


@pytest.mark.unit
def test_dispatcher_retries_cleanup_until_job_and_pods_are_confirmed_absent(
    monkeypatch,
    tmp_path: Path,
) -> None:
    repo, _orchestrator, runner, dispatcher, run = _system(monkeypatch, tmp_path)
    dispatcher.run_once()
    execution = repo.list_executions(run_id=run["id"])[0]
    runner.cleanup_results = [False, True]
    repo.request_execution_cancellation(execution["id"], reason="Run cancelled manually")

    dispatcher.run_once()
    pending_cleanup = repo.get_execution(execution["id"])

    assert pending_cleanup["status"] == "executing"
    assert not pending_cleanup.get("kubernetes_cleaned_at")
    assert pending_cleanup["kubernetes_cleanup_requested_at"]
    assert repo.get_run(run["id"])["status"] == "executing"
    assert runner.deleted == [execution["kubernetes_job_name"]]

    dispatcher.run_once()
    cleaned = repo.get_execution(execution["id"])

    assert cleaned["status"] == "cancelled"
    assert cleaned["kubernetes_cleaned_at"]
    assert repo.get_run(run["id"])["status"] == "cancelled"
    assert runner.deleted == [execution["kubernetes_job_name"]] * 2


@pytest.mark.unit
def test_secret_only_execution_is_cleaned_before_retry(monkeypatch, tmp_path: Path) -> None:
    repo, orchestrator, runner, dispatcher, run = _system(monkeypatch, tmp_path)
    execution = repo.list_executions(run_id=run["id"])[0]
    repo.update_execution(
        execution["id"],
        {
            "status": "failed",
            "completed_at": "2026-01-01T00:00:00+00:00",
            "kubernetes_secret_name": "scene-exec-secret-only",
        },
    )
    repo.update_run(run["id"], {"status": "failed"})

    with pytest.raises(ValueError, match="cleaned up"):
        orchestrator.retry_execution(execution["id"])
    assert [item["id"] for item in repo.list_cleanup_pending_executions()] == [
        execution["id"]
    ]

    stats = dispatcher.run_once()
    cleaned = repo.get_execution(execution["id"])

    assert stats["cleaned"] == 1
    assert cleaned["kubernetes_cleaned_at"]
    assert orchestrator.retry_execution(execution["id"])["status"] == "queued"


@pytest.mark.unit
def test_terminal_failure_keeps_remembered_status_while_cleanup_is_pending(
    monkeypatch,
    tmp_path: Path,
) -> None:
    repo, _orchestrator, runner, dispatcher, run = _system(monkeypatch, tmp_path)
    dispatcher.run_once()
    execution = repo.list_executions(run_id=run["id"])[0]
    job_name = execution["kubernetes_job_name"]
    runner.phases[job_name] = KubernetesExecutionStatus(
        "failed",
        "Error",
        "capture failed",
        "pod-1",
    )

    dispatcher.run_once()
    repo.update_execution(
        execution["id"],
        {"result_recovery_started_at": "2000-01-01T00:00:00+00:00"},
    )
    runner.cleanup_results = [False, True]
    dispatcher.run_once()

    deleting = repo.get_execution(execution["id"])
    assert deleting["status"] == "executing"
    assert deleting["kubernetes_cleanup_requested_at"]
    assert not deleting.get("kubernetes_cleaned_at")

    runner.phases[job_name] = KubernetesExecutionStatus("pending")
    dispatcher.run_once()

    failed = repo.get_execution(execution["id"])
    assert failed["status"] == "failed"
    assert failed["kubernetes_cleaned_at"]
    assert failed["kubernetes_terminal_phase"] == "failed"
    assert "capture failed" in failed["message"]


@pytest.mark.unit
def test_dispatcher_classifies_runner_failure_and_preserves_log(monkeypatch, tmp_path: Path) -> None:
    repo, _orchestrator, runner, dispatcher, run = _system(monkeypatch, tmp_path)
    dispatcher.run_once()
    execution = repo.list_executions(run_id=run["id"])[0]
    runner.phases[execution["kubernetes_job_name"]] = KubernetesExecutionStatus(
        "image_pull",
        "ImagePullBackOff",
        "manifest unknown",
        "pod-1",
    )

    dispatcher.run_once()
    waiting = repo.get_execution(execution["id"])
    assert waiting["status"] == "executing"
    assert waiting["result_recovery_started_at"]
    repo.update_execution(
        execution["id"],
        {"result_recovery_started_at": "2000-01-01T00:00:00+00:00"},
    )

    dispatcher.run_once()
    updated = repo.get_execution(execution["id"])

    assert updated["status"] == "failed"
    assert "could not be pulled" in updated["message"]
    assert updated["artifacts"]["log"]["path"].endswith("runner.log")
    assert repo.get_run(run["id"])["status"] == "failed"


@pytest.mark.unit
@pytest.mark.parametrize("phase", ["failed", "missing"])
def test_terminal_or_missing_job_recovers_result_before_infrastructure_failure(
    monkeypatch,
    tmp_path: Path,
    phase: str,
) -> None:
    repo, orchestrator, runner, dispatcher, run = _system(monkeypatch, tmp_path)
    dispatcher.run_once()
    execution = repo.list_executions(run_id=run["id"])[0]
    job_name = execution["kubernetes_job_name"]
    runner.phases[job_name] = KubernetesExecutionStatus(
        phase,
        "JobNotFound" if phase == "missing" else "Error",
        "runner terminated",
        None if phase == "missing" else "pod-1",
    )
    recovered: list[str] = []

    def recover(execution_id: str) -> bool:
        recovered.append(execution_id)
        repo.update_execution(
            execution_id,
            {
                "status": "finished",
                "completed_at": "2026-07-20T12:00:00+00:00",
                "message": "Recovered from result.json",
            },
        )
        orchestrator.refresh_durable_run(run["id"])
        return True

    monkeypatch.setattr(orchestrator, "recover_durable_result", recover)

    dispatcher.run_once()
    updated = repo.get_execution(execution["id"])

    assert recovered == [execution["id"]]
    assert updated["status"] == "finished"
    assert updated["message"] == "Recovered from result.json"
    assert "infrastructure" not in updated["message"].lower()


@pytest.mark.unit
def test_missing_job_failure_waits_for_bounded_result_recovery_grace(
    monkeypatch,
    tmp_path: Path,
) -> None:
    repo, _orchestrator, runner, dispatcher, run = _system(monkeypatch, tmp_path)
    dispatcher.run_once()
    execution = repo.list_executions(run_id=run["id"])[0]
    runner.phases.pop(execution["kubernetes_job_name"], None)

    dispatcher.run_once()
    waiting = repo.get_execution(execution["id"])

    assert waiting["status"] == "executing"
    assert waiting["kubernetes_terminal_phase"] == "missing"
    assert waiting["result_recovery_started_at"]

    repo.update_execution(
        execution["id"],
        {"result_recovery_started_at": "2000-01-01T00:00:00+00:00"},
    )
    dispatcher.run_once()
    failed = repo.get_execution(execution["id"])

    assert failed["status"] == "failed"
    assert "disappeared before completion" in failed["message"]


@pytest.mark.unit
def test_durable_callback_validates_identity_and_is_idempotent(monkeypatch, tmp_path: Path) -> None:
    repo, orchestrator, runner, dispatcher, run = _system(monkeypatch, tmp_path)
    dispatcher.run_once()
    execution = repo.list_executions(run_id=run["id"])[0]
    token = runner.tokens[execution["kubernetes_secret_name"]]
    payload = {
        "token": token,
        "run_id": run["id"],
        "execution_id": execution["id"],
        "dispatch_generation": execution["dispatch_generation"],
        "result": {"status": "error", "error": "capture failed"},
    }

    assert orchestrator.handle_durable_execution_callback(execution["id"], payload) == "accepted"
    accepted = repo.get_execution(execution["id"])
    assert accepted["status"] == "executing"
    assert accepted["callback_state"] == "accepted"
    assert orchestrator.handle_durable_execution_callback(execution["id"], payload) == "duplicate"
    assert orchestrator.handle_durable_execution_callback(
        execution["id"],
        {**payload, "result": {"status": "error", "error": "different"}},
    ) == "conflict"
    assert orchestrator.handle_durable_execution_callback(
        execution["id"],
        {**payload, "dispatch_generation": 999},
    ) == "invalid"

    stats = dispatcher.run_once()
    finalized = repo.get_execution(execution["id"])
    assert stats["callbacks_finalized"] == 1
    assert finalized["status"] == "failed"
    assert finalized["callback_state"] == "finalized"


@pytest.mark.unit
def test_infrastructure_failure_does_not_overwrite_accepted_callback(
    monkeypatch,
    tmp_path: Path,
) -> None:
    repo, orchestrator, runner, dispatcher, run = _system(monkeypatch, tmp_path)
    dispatcher.run_once()
    execution = repo.list_executions(run_id=run["id"])[0]
    payload = {
        "token": runner.tokens[execution["kubernetes_secret_name"]],
        "run_id": run["id"],
        "execution_id": execution["id"],
        "dispatch_generation": execution["dispatch_generation"],
        "result": {"status": "ok"},
    }
    assert orchestrator.handle_durable_execution_callback(execution["id"], payload) == "accepted"
    before_failure = repo.get_execution(execution["id"])

    assert not orchestrator.finalize_k3s_failure(
        execution["id"],
        message="Kubernetes Job failed after callback delivery",
        runner_log="late infrastructure failure",
    )

    accepted = repo.get_execution(execution["id"])
    assert accepted["status"] == "executing"
    assert accepted["callback_state"] == "accepted"
    assert accepted.get("message") != "Kubernetes Job failed after callback delivery"
    assert accepted.get("artifacts", {}) == before_failure.get("artifacts", {})


@pytest.mark.unit
def test_dispatcher_recovers_callback_finalization_after_terminal_write(
    monkeypatch,
    tmp_path: Path,
) -> None:
    repo, orchestrator, runner, dispatcher, run = _system(monkeypatch, tmp_path)
    dispatcher.run_once()
    execution = repo.list_executions(run_id=run["id"])[0]
    payload = {
        "token": runner.tokens[execution["kubernetes_secret_name"]],
        "run_id": run["id"],
        "execution_id": execution["id"],
        "dispatch_generation": execution["dispatch_generation"],
        "result": {"status": "error", "error": "capture failed"},
    }
    assert orchestrator.handle_durable_execution_callback(execution["id"], payload) == "accepted"

    # Simulate a dispatcher crash after the terminal execution write but before
    # the run summary and callback state were finalized.
    repo.update_execution(
        execution["id"],
        {"status": "failed", "completed_at": "2026-01-01T00:00:00+00:00"},
    )

    stats = dispatcher.run_once()

    assert stats["callbacks_finalized"] == 1
    assert repo.get_execution(execution["id"])["callback_state"] == "finalized"
    assert repo.get_run(run["id"])["status"] == "failed"


@pytest.mark.unit
def test_callback_finalizer_takeover_recovers_after_crash_at_terminal_write(
    monkeypatch,
    tmp_path: Path,
) -> None:
    repo, orchestrator, runner, dispatcher, run = _system(monkeypatch, tmp_path)
    dispatcher.run_once()
    execution = repo.list_executions(run_id=run["id"])[0]
    payload = {
        "token": runner.tokens[execution["kubernetes_secret_name"]],
        "run_id": run["id"],
        "execution_id": execution["id"],
        "dispatch_generation": execution["dispatch_generation"],
        "result": {"status": "error", "error": "capture failed"},
    }
    assert orchestrator.handle_durable_execution_callback(execution["id"], payload) == "accepted"

    def crash_after_terminal_write(run_record, execution_id, result_data, baseline_record):
        repo.update_execution(
            execution_id,
            {
                "status": "failed",
                "completed_at": "2026-07-20T12:00:00+00:00",
                "message": str(result_data.get("error") or "capture failed"),
            },
        )
        raise RuntimeError("simulated process crash")

    monkeypatch.setattr(orchestrator, "_finalize_from_callback", crash_after_terminal_write)

    with pytest.raises(RuntimeError, match="simulated process crash"):
        orchestrator.finalize_durable_callback(execution["id"], owner="dispatcher-a")

    assert repo.get_execution(execution["id"])["callback_finalizer_owner"] == "dispatcher-a"
    assert not orchestrator.finalize_durable_callback(execution["id"], owner="dispatcher-b")

    repo.update_execution(
        execution["id"],
        {"callback_finalizer_lease_expires_at": "2000-01-01T00:00:00+00:00"},
    )

    assert orchestrator.finalize_durable_callback(execution["id"], owner="dispatcher-b")
    finalized = repo.get_execution(execution["id"])
    assert finalized["callback_state"] == "finalized"
    assert finalized["callback_finalizer_owner"] == "dispatcher-b"
    assert repo.get_run(run["id"])["status"] == "failed"


@pytest.mark.unit
def test_callback_replay_after_run_refresh_does_not_duplicate_baseline_items(
    monkeypatch,
    tmp_path: Path,
) -> None:
    repo, orchestrator, runner, dispatcher, run = _system(monkeypatch, tmp_path)
    dispatcher.run_once()
    execution = repo.list_executions(run_id=run["id"])[0]
    observed = tmp_path / "artifacts" / "runs" / run["id"] / execution["id"] / "observed.png"
    observed.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGBA", (4, 4), (20, 40, 60, 255)).save(observed)
    payload = {
        "token": runner.tokens[execution["kubernetes_secret_name"]],
        "run_id": run["id"],
        "execution_id": execution["id"],
        "dispatch_generation": execution["dispatch_generation"],
        "result": {"status": "ok", "screenshot": "observed.png"},
    }
    assert orchestrator.handle_durable_execution_callback(execution["id"], payload) == "accepted"

    mark_callback_finalized = repo.mark_callback_finalized
    attempts = 0

    def crash_before_marker(execution_id: str, digest: str, *, owner: str) -> bool:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("simulated marker crash")
        return mark_callback_finalized(execution_id, digest, owner=owner)

    monkeypatch.setattr(repo, "mark_callback_finalized", crash_before_marker)

    with pytest.raises(RuntimeError, match="simulated marker crash"):
        orchestrator.finalize_durable_callback(execution["id"], owner="dispatcher-a")

    baseline_id = repo.get_run(run["id"])["baseline_id"]
    assert len(repo.get_baseline(baseline_id)["items"]) == 1
    assert repo.get_execution(execution["id"])["callback_state"] == "accepted"

    assert orchestrator.finalize_durable_callback(execution["id"], owner="dispatcher-a")
    assert len(repo.get_baseline(baseline_id)["items"]) == 1
    assert repo.get_execution(execution["id"])["callback_state"] == "finalized"


@pytest.mark.unit
def test_s3_result_recovery_only_accepts_callback_for_dispatcher_finalization(
    monkeypatch,
    tmp_path: Path,
) -> None:
    repo, orchestrator, _runner, dispatcher, run = _system(monkeypatch, tmp_path)
    dispatcher.run_once()
    execution = repo.list_executions(run_id=run["id"])[0]
    repo.update_execution(
        execution["id"],
        {
            "artifact_transfer": {
                "outputs": {"result": {"key": "runs/result.json"}},
            }
        },
    )
    monkeypatch.setattr(
        type(orchestrator._artifacts),
        "backend",
        property(lambda _store: "s3"),
    )

    def materialize(_artifact, destination: Path) -> Path:
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(
            json.dumps({"status": "error", "error": "capture failed"}),
            encoding="utf-8",
        )
        return destination

    finalized: list[str] = []
    monkeypatch.setattr(orchestrator._artifacts, "materialize", materialize)
    monkeypatch.setattr(
        orchestrator,
        "finalize_durable_callback",
        lambda execution_id, **_kwargs: finalized.append(execution_id) or True,
    )

    assert orchestrator.recover_durable_result(execution["id"])
    recovered = repo.get_execution(execution["id"])
    assert recovered["callback_state"] == "accepted"
    assert recovered["status"] == "executing"
    assert finalized == []


@pytest.mark.unit
def test_dispatcher_cancels_queued_execution_after_run_deadline(monkeypatch, tmp_path: Path) -> None:
    repo, _orchestrator, runner, dispatcher, run = _system(monkeypatch, tmp_path)
    repo.update_run(run["id"], {"timeout_deadline": time.time() - 1})

    dispatcher.run_once()

    execution = repo.list_executions(run_id=run["id"])[0]
    assert execution["status"] == "cancelled"
    assert "timed out" in execution["message"]
    assert runner.created == []
    assert repo.get_run(run["id"])["status"] == "cancelled"


@pytest.mark.unit
def test_failed_durable_run_can_reopen_and_dispatch_retry(monkeypatch, tmp_path: Path) -> None:
    repo, orchestrator, runner, dispatcher, run = _system(monkeypatch, tmp_path)
    dispatcher.run_once()
    failed = repo.list_executions(run_id=run["id"])[0]
    runner.phases[failed["kubernetes_job_name"]] = KubernetesExecutionStatus(
        "failed",
        "Error",
        "capture failed",
        "pod-1",
    )
    dispatcher.run_once()
    repo.update_execution(
        failed["id"],
        {"result_recovery_started_at": "2000-01-01T00:00:00+00:00"},
    )
    dispatcher.run_once()
    assert repo.get_run(run["id"])["status"] == "failed"

    retry = orchestrator.retry_execution(failed["id"])
    reopened = repo.get_run(run["id"])
    assert reopened["status"] == "executing"
    assert reopened["timeout_deadline"] > time.time()

    dispatcher.run_once()

    assert repo.get_execution(retry["id"])["status"] == "executing"
    assert len(runner.created) == 2
    assert repo.get_execution(failed["id"])["superseded_by_execution_id"] == retry["id"]
    assert repo.execution_status_counts(run["id"])["failed"] == 0


@pytest.mark.unit
def test_retry_rejects_active_and_cleanup_pending_executions(monkeypatch, tmp_path: Path) -> None:
    repo, orchestrator, _runner, dispatcher, run = _system(monkeypatch, tmp_path)
    execution = repo.list_executions(run_id=run["id"])[0]

    with pytest.raises(ValueError, match="Only failed or cancelled"):
        orchestrator.retry_execution(execution["id"])

    dispatcher.run_once()
    executing = repo.get_execution(execution["id"])
    repo.update_execution(
        execution["id"],
        {"status": "failed", "completed_at": "2026-01-01T00:00:00+00:00"},
    )
    repo.update_run(run["id"], {"status": "failed"})

    with pytest.raises(ValueError, match="cleaned up"):
        orchestrator.retry_execution(executing["id"])


@pytest.mark.unit
def test_retry_rejects_failed_execution_on_finished_run_before_reservation(
    monkeypatch,
    tmp_path: Path,
) -> None:
    repo, orchestrator, _runner, _dispatcher, run = _system(monkeypatch, tmp_path)
    execution = repo.list_executions(run_id=run["id"])[0]
    repo.update_execution(
        execution["id"],
        {"status": "failed", "completed_at": "2026-07-20T12:00:00+00:00"},
    )
    repo.update_run(run["id"], {"status": "finished"})

    with pytest.raises(ValueError, match="failed or cancelled runs"):
        orchestrator.retry_execution(execution["id"])

    assert not repo.get_execution(execution["id"]).get("superseded_by_execution_id")
    assert len(repo.list_executions(run_id=run["id"])) == 1


@pytest.mark.unit
def test_retry_is_idempotent_and_comparison_baseline_remains_completed(
    monkeypatch,
    tmp_path: Path,
) -> None:
    repo, orchestrator, _runner, _dispatcher, recording_run = _system(monkeypatch, tmp_path)
    recording = repo.get_run(recording_run["id"])
    baseline = repo.get_baseline(recording["baseline_id"])
    repo.update_baseline(baseline["id"], {"status": "completed"})
    comparison = repo.create_run(
        {
            "project_id": recording["project_id"],
            "batch_id": recording["batch_id"],
            "baseline_id": baseline["id"],
            "purpose": RunPurpose.comparison.value,
            "requested_by": "test",
        }
    )
    orchestrator.prepare_durable_run(comparison["id"])
    execution = repo.list_executions(run_id=comparison["id"])[0]
    repo.update_execution(
        execution["id"],
        {"status": "failed", "completed_at": "2026-01-01T00:00:00+00:00"},
    )
    orchestrator.refresh_durable_run(comparison["id"])

    first = orchestrator.retry_execution(execution["id"])
    run_after_first_retry = repo.get_run(comparison["id"])
    second = orchestrator.retry_execution(execution["id"])

    assert first["id"] == second["id"]
    assert repo.get_run(comparison["id"]) == run_after_first_retry
    assert len(
        [
            item
            for item in repo.list_executions(run_id=comparison["id"])
            if item.get("retry_of_execution_id") == execution["id"]
        ]
    ) == 1
    assert repo.get_baseline(baseline["id"])["status"] == "completed"


@pytest.mark.unit
def test_concurrent_retry_duplicates_have_one_run_lifecycle_owner(
    monkeypatch,
    tmp_path: Path,
) -> None:
    repo, orchestrator, _runner, _dispatcher, run = _system(monkeypatch, tmp_path)
    source = repo.list_executions(run_id=run["id"])[0]
    repo.update_execution(
        source["id"],
        {"status": "failed", "completed_at": "2026-07-20T12:00:00+00:00"},
    )
    orchestrator.refresh_durable_run(run["id"])

    reopen_barrier = Barrier(2)
    original_reopen = repo.reopen_run_for_retry
    original_update_baseline = repo.update_baseline
    original_update_run = repo.update_run
    counts = {"baseline": 0, "deadline": 0, "summary": 0}
    counts_lock = Lock()

    def synchronized_reopen(run_id: str):
        reopen_barrier.wait(timeout=3)
        return original_reopen(run_id)

    def counted_update_baseline(baseline_id: str, payload: dict):
        if payload.get("status") == "pending":
            with counts_lock:
                counts["baseline"] += 1
        return original_update_baseline(baseline_id, payload)

    def counted_update_run(run_id: str, payload: dict):
        with counts_lock:
            if "timeout_deadline" in payload:
                counts["deadline"] += 1
            if "summary" in payload:
                counts["summary"] += 1
        return original_update_run(run_id, payload)

    monkeypatch.setattr(repo, "reopen_run_for_retry", synchronized_reopen)
    monkeypatch.setattr(repo, "update_baseline", counted_update_baseline)
    monkeypatch.setattr(repo, "update_run", counted_update_run)
    results: list[dict] = []
    errors: list[Exception] = []

    def retry() -> None:
        try:
            result = orchestrator.retry_execution(source["id"])
            with counts_lock:
                results.append(result)
        except Exception as exc:  # pragma: no cover - asserted below
            with counts_lock:
                errors.append(exc)

    threads = [Thread(target=retry) for _index in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert all(not thread.is_alive() for thread in threads)
    assert errors == []
    assert len(results) == 2
    assert results[0]["id"] == results[1]["id"]
    assert counts == {"baseline": 1, "deadline": 1, "summary": 1}
    assert len(
        [
            execution
            for execution in repo.list_executions(run_id=run["id"])
            if execution.get("retry_of_execution_id") == source["id"]
        ]
    ) == 1
    assert repo.get_run(run["id"])["status"] == "executing"


@pytest.mark.unit
def test_retry_recovers_reserved_queued_child_before_reopening_run(
    monkeypatch,
    tmp_path: Path,
) -> None:
    repo, orchestrator, _runner, _dispatcher, run = _system(monkeypatch, tmp_path)
    source = repo.list_executions(run_id=run["id"])[0]
    repo.update_execution(
        source["id"],
        {"status": "failed", "completed_at": "2026-07-20T12:00:00+00:00"},
    )
    orchestrator.refresh_durable_run(run["id"])
    reservation, reserved_source = repo.reserve_execution_retry(source["id"])
    assert reservation == "reserved"
    retry_id = reserved_source["superseded_by_execution_id"]
    queued_child = repo.create_execution(
        {
            "run_id": run["id"],
            "project_id": run["project_id"],
            "batch_id": run["batch_id"],
            "task_id": source["task_id"],
            "task_name": source["task_name"],
            "page_id": source.get("page_id"),
            "browser": source["browser"],
            "viewport": source["viewport"],
            "status": "queued",
            "sequence": (source.get("sequence") or 0) + 1,
            "idempotency_key": f"retry:{source['id']}",
            "configuration_snapshot": source["configuration_snapshot"],
            "retry_of_execution_id": source["id"],
        }
    )
    assert queued_child["id"] == retry_id
    assert repo.get_run(run["id"])["status"] == "failed"

    recovered = orchestrator.retry_execution(source["id"])

    assert recovered["id"] == queued_child["id"]
    reopened = repo.get_run(run["id"])
    assert reopened["status"] == "executing"
    assert reopened["timeout_deadline"] > time.time()
    assert reopened["summary"]["executions_total"] == 1
    assert len(
        [
            execution
            for execution in repo.list_executions(run_id=run["id"])
            if execution.get("retry_of_execution_id") == source["id"]
        ]
    ) == 1


@pytest.mark.unit
def test_retry_replay_after_child_failure_does_not_reopen_run(
    monkeypatch,
    tmp_path: Path,
) -> None:
    repo, orchestrator, _runner, _dispatcher, run = _system(monkeypatch, tmp_path)
    source = repo.list_executions(run_id=run["id"])[0]
    repo.update_execution(
        source["id"],
        {"status": "failed", "completed_at": "2026-07-20T12:00:00+00:00"},
    )
    orchestrator.refresh_durable_run(run["id"])
    child = orchestrator.retry_execution(source["id"])
    repo.update_execution(
        child["id"],
        {"status": "failed", "completed_at": "2026-07-20T12:01:00+00:00"},
    )
    orchestrator.refresh_durable_run(run["id"])
    terminal_run = repo.get_run(run["id"])
    assert terminal_run["status"] == "failed"

    replay = orchestrator.retry_execution(source["id"])

    assert replay["id"] == child["id"]
    assert repo.get_run(run["id"]) == terminal_run

    next_attempt = orchestrator.retry_execution(child["id"])

    assert next_attempt["id"] != child["id"]
    assert next_attempt["retry_of_execution_id"] == child["id"]
    assert repo.get_run(run["id"])["status"] == "executing"


@pytest.mark.unit
def test_dispatch_uses_launch_snapshot_after_page_and_task_are_deleted(
    monkeypatch,
    tmp_path: Path,
) -> None:
    repo, _orchestrator, runner, dispatcher, run = _system(monkeypatch, tmp_path)
    execution = repo.list_executions(run_id=run["id"])[0]
    snapshot = execution["configuration_snapshot"]
    assert snapshot["page"]["url"] == "https://example.test"

    repo.delete_page(execution["page_id"])
    assert repo.get_page(execution["page_id"]) is None
    assert repo.get_task(execution["task_id"]) is None

    dispatcher.run_once()

    dispatched = repo.get_execution(execution["id"])
    assert dispatched["status"] == "executing"
    config = runner.configs[dispatched["kubernetes_secret_name"]]
    assert config["url"] == "https://example.test"
    assert config["post_wait_ms"] == snapshot["capture_post_wait_ms"]
