from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional

import pytest

from app.services.storage import LocalDynamoStorage, SceneRepository


class _ConflictOnceStorage(LocalDynamoStorage):
    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self._conflict: Optional[tuple[str, str, Callable[[dict], None]]] = None

    def conflict_once(
        self,
        collection: str,
        item_id: str,
        mutate: Callable[[dict], None],
    ) -> None:
        self._conflict = (collection, item_id, mutate)

    def upsert(self, collection: str, item_id: str, payload: dict) -> dict:
        conflict = self._conflict
        if conflict and conflict[:2] == (collection, item_id):
            self._conflict = None
            current = super().get(collection, item_id)
            assert current is not None
            conflict[2](current)
            super().upsert(collection, item_id, current)
        return super().upsert(collection, item_id, payload)


def _repo(tmp_path: Path) -> SceneRepository:
    return SceneRepository(LocalDynamoStorage(tmp_path / "state.json"))


def _execution(repo: SceneRepository) -> dict:
    return repo.create_execution(
        {
            "run_id": "run-1",
            "project_id": "project-1",
            "batch_id": "batch-1",
            "task_id": "task-1",
            "task_name": "Task",
            "page_id": "page-1",
            "browser": "chromium",
            "viewport": {"width": 1280, "height": 720},
            "status": "queued",
            "sequence": 1,
            "idempotency_key": "matrix:1",
        }
    )


def _accepted_callback(repo: SceneRepository, execution: dict) -> dict:
    claim = repo.claim_execution(execution["id"], owner="dispatcher-a")
    assert claim is not None
    token_sha = hashlib.sha256(b"token").hexdigest()
    configured = repo.configure_execution_callback(
        execution["id"],
        owner="dispatcher-a",
        generation=1,
        token_sha256=token_sha,
        expires_at=(datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
    )
    assert configured is not None
    assert repo.mark_execution_dispatched(
        execution["id"],
        owner="dispatcher-a",
        generation=1,
    )
    outcome, accepted = repo.accept_execution_callback(
        execution["id"],
        run_id="run-1",
        generation=1,
        token_sha256=token_sha,
        result={"status": "ok"},
    )
    assert outcome == "accepted"
    assert accepted is not None
    return accepted


@pytest.mark.unit
def test_dispatcher_lease_is_single_owner_and_can_be_released(tmp_path: Path) -> None:
    repo = _repo(tmp_path)

    assert repo.acquire_dispatcher_lease("dispatcher-a")
    assert not repo.acquire_dispatcher_lease("dispatcher-b")
    assert repo.acquire_dispatcher_lease("dispatcher-a")
    assert repo.report_dispatcher_health(
        "dispatcher-a",
        capabilities={"create:batch:jobs": True},
    )
    assert repo.dispatcher_status()["capabilities_ok"] is True
    assert repo.release_dispatcher_lease("dispatcher-a")
    assert repo.acquire_dispatcher_lease("dispatcher-b")
    status = repo.dispatcher_status()
    assert status["owner"] == "dispatcher-b"
    assert "capabilities_ok" not in status


@pytest.mark.unit
def test_execution_claim_is_atomic_and_generation_is_monotonic(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    execution = _execution(repo)

    claim = repo.claim_execution(execution["id"], owner="dispatcher-a", lease_seconds=60)

    assert claim is not None
    assert claim["dispatch_generation"] == 1
    assert claim["kubernetes_job_name"].endswith("-g1")
    assert repo.claim_execution(execution["id"], owner="dispatcher-b") is None
    assert repo.renew_execution_claim(
        execution["id"],
        owner="dispatcher-a",
        generation=1,
    )


@pytest.mark.unit
def test_execution_claim_does_not_report_success_after_losing_cas_retry(
    tmp_path: Path,
) -> None:
    storage = _ConflictOnceStorage(tmp_path / "state.json")
    repo = SceneRepository(storage)
    execution = _execution(repo)

    def competitor_claim(record: dict) -> None:
        record["dispatch_claim_owner"] = "dispatcher-b"
        record["dispatch_lease_expires_at"] = (
            datetime.now(timezone.utc) + timedelta(minutes=5)
        ).isoformat()

    storage.conflict_once("executions", execution["id"], competitor_claim)

    assert repo.claim_execution(execution["id"], owner="dispatcher-a") is None
    assert repo.get_execution(execution["id"])["dispatch_claim_owner"] == "dispatcher-b"


@pytest.mark.unit
def test_dispatcher_release_does_not_report_success_after_owner_changes(
    tmp_path: Path,
) -> None:
    storage = _ConflictOnceStorage(tmp_path / "state.json")
    repo = SceneRepository(storage)
    assert repo.acquire_dispatcher_lease("dispatcher-a")

    def competitor_lease(record: dict) -> None:
        record["owner"] = "dispatcher-b"
        record["expires_at"] = (
            datetime.now(timezone.utc) + timedelta(minutes=5)
        ).isoformat()

    storage.conflict_once("leases", "k3s-dispatcher", competitor_lease)

    assert repo.release_dispatcher_lease("dispatcher-a") is False
    assert repo.dispatcher_status()["owner"] == "dispatcher-b"


@pytest.mark.unit
def test_callback_is_scoped_idempotent_and_conflict_detecting(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    execution = _execution(repo)
    claim = repo.claim_execution(execution["id"], owner="dispatcher-a")
    assert claim is not None
    token = "runner-callback-token"
    token_sha = hashlib.sha256(token.encode("utf-8")).hexdigest()
    expires = (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat()
    configured = repo.configure_execution_callback(
        execution["id"],
        owner="dispatcher-a",
        generation=1,
        token_sha256=token_sha,
        expires_at=expires,
        artifact_transfer={"version": 1, "outputs": {}},
        runner_spec_digest="spec-digest",
    )
    assert configured is not None
    assert repo.mark_execution_dispatched(
        execution["id"],
        owner="dispatcher-a",
        generation=1,
    )

    result = {"status": "ok", "uploads": {}}
    outcome, accepted = repo.accept_execution_callback(
        execution["id"],
        run_id="run-1",
        generation=1,
        token_sha256=token_sha,
        result=result,
    )
    repo.update_execution(
        execution["id"],
        {"callback_expires_at": "2000-01-01T00:00:00+00:00"},
    )
    duplicate, _ = repo.accept_execution_callback(
        execution["id"],
        run_id="run-1",
        generation=1,
        token_sha256=token_sha,
        result=result,
    )
    conflict, _ = repo.accept_execution_callback(
        execution["id"],
        run_id="run-1",
        generation=1,
        token_sha256=token_sha,
        result={"status": "error"},
    )

    assert outcome == "accepted"
    assert accepted["callback_state"] == "accepted"
    assert duplicate == "duplicate"
    assert conflict == "conflict"
    assert "runner-callback-token" not in str(repo.get_execution(execution["id"]))


@pytest.mark.unit
def test_callback_acceptance_blocks_concurrent_cancellation_retry(tmp_path: Path) -> None:
    storage = _ConflictOnceStorage(tmp_path / "state.json")
    repo = SceneRepository(storage)
    execution = _execution(repo)
    claim = repo.claim_execution(execution["id"], owner="dispatcher-a")
    assert claim is not None
    token_sha = hashlib.sha256(b"token").hexdigest()
    configured = repo.configure_execution_callback(
        execution["id"],
        owner="dispatcher-a",
        generation=1,
        token_sha256=token_sha,
        expires_at=(datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
    )
    assert configured is not None

    def concurrent_accept(record: dict) -> None:
        record.update(
            {
                "callback_state": "accepted",
                "callback_result_digest": hashlib.sha256(b"result").hexdigest(),
                "callback_result": {"status": "ok"},
            }
        )

    storage.conflict_once("executions", execution["id"], concurrent_accept)

    cancelled = repo.request_execution_cancellation(
        execution["id"],
        reason="Run cancelled manually",
    )
    persisted = repo.get_execution(execution["id"])

    assert cancelled is None
    assert persisted["callback_state"] == "accepted"
    assert "cancellation_requested_at" not in persisted


@pytest.mark.unit
def test_callback_does_not_report_acceptance_after_cancellation_wins_retry(
    tmp_path: Path,
) -> None:
    storage = _ConflictOnceStorage(tmp_path / "state.json")
    repo = SceneRepository(storage)
    execution = _execution(repo)
    claim = repo.claim_execution(execution["id"], owner="dispatcher-a")
    assert claim is not None
    token_sha = hashlib.sha256(b"token").hexdigest()
    configured = repo.configure_execution_callback(
        execution["id"],
        owner="dispatcher-a",
        generation=1,
        token_sha256=token_sha,
        expires_at=(datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
    )
    assert configured is not None

    def concurrent_cancel(record: dict) -> None:
        record["cancellation_requested_at"] = datetime.now(timezone.utc).isoformat()

    storage.conflict_once("executions", execution["id"], concurrent_cancel)

    outcome, accepted = repo.accept_execution_callback(
        execution["id"],
        run_id="run-1",
        generation=1,
        token_sha256=token_sha,
        result={"status": "ok"},
    )

    assert outcome == "invalid"
    assert accepted is not None
    assert accepted["callback_state"] == "pending"
    assert "callback_result_digest" not in accepted


@pytest.mark.unit
def test_cancellation_intent_blocks_callback_acceptance(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    execution = _execution(repo)
    claim = repo.claim_execution(execution["id"], owner="dispatcher-a")
    assert claim is not None
    token_sha = hashlib.sha256(b"token").hexdigest()
    repo.configure_execution_callback(
        execution["id"],
        owner="dispatcher-a",
        generation=1,
        token_sha256=token_sha,
        expires_at=(datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
    )
    cancelled = repo.request_execution_cancellation(
        execution["id"],
        reason="Run cancelled manually",
    )
    outcome, _ = repo.accept_execution_callback(
        execution["id"],
        run_id="run-1",
        generation=1,
        token_sha256=token_sha,
        result={"status": "ok"},
    )

    assert cancelled["cancellation_requested_at"]
    assert outcome == "invalid"


@pytest.mark.unit
def test_callback_finalization_claim_is_fenced_and_expired_lease_can_be_taken_over(
    tmp_path: Path,
) -> None:
    storage = _ConflictOnceStorage(tmp_path / "state.json")
    repo = SceneRepository(storage)
    execution = _execution(repo)
    _accepted_callback(repo, execution)

    def concurrent_claim(record: dict) -> None:
        record["callback_finalizer_owner"] = "dispatcher-b"
        record["callback_finalizer_lease_expires_at"] = (
            datetime.now(timezone.utc) + timedelta(minutes=5)
        ).isoformat()

    storage.conflict_once("executions", execution["id"], concurrent_claim)

    assert repo.claim_callback_finalization(execution["id"], owner="dispatcher-a") is None
    assert repo.get_execution(execution["id"])["callback_finalizer_owner"] == "dispatcher-b"

    repo.update_execution(
        execution["id"],
        {"callback_finalizer_lease_expires_at": "2000-01-01T00:00:00+00:00"},
    )
    taken_over = repo.claim_callback_finalization(execution["id"], owner="dispatcher-c")

    assert taken_over is not None
    assert taken_over["callback_finalizer_owner"] == "dispatcher-c"
    assert taken_over["callback_finalization_started_at"]


@pytest.mark.unit
def test_callback_finalization_marker_requires_owner_digest_and_terminal_status(
    tmp_path: Path,
) -> None:
    repo = _repo(tmp_path)
    execution = _execution(repo)
    accepted = _accepted_callback(repo, execution)
    digest = accepted["callback_result_digest"]
    assert repo.claim_callback_finalization(execution["id"], owner="dispatcher-a")

    assert not repo.mark_callback_finalized(
        execution["id"],
        digest,
        owner="dispatcher-a",
    )
    repo.update_execution(
        execution["id"],
        {"status": "finished", "completed_at": datetime.now(timezone.utc).isoformat()},
    )
    assert not repo.mark_callback_finalized(
        execution["id"],
        digest,
        owner="dispatcher-b",
    )
    assert not repo.mark_callback_finalized(
        execution["id"],
        "wrong-digest",
        owner="dispatcher-a",
    )
    assert repo.mark_callback_finalized(
        execution["id"],
        digest,
        owner="dispatcher-a",
    )
    assert repo.mark_callback_finalized(
        execution["id"],
        digest,
        owner="dispatcher-a",
    )
    assert not repo.mark_callback_finalized(
        execution["id"],
        digest,
        owner="dispatcher-b",
    )


@pytest.mark.unit
def test_callback_finalization_marker_loses_to_concurrent_owner_takeover(
    tmp_path: Path,
) -> None:
    storage = _ConflictOnceStorage(tmp_path / "state.json")
    repo = SceneRepository(storage)
    execution = _execution(repo)
    accepted = _accepted_callback(repo, execution)
    digest = accepted["callback_result_digest"]
    assert repo.claim_callback_finalization(execution["id"], owner="dispatcher-a")
    repo.update_execution(
        execution["id"],
        {
            "status": "finished",
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "callback_finalizer_lease_expires_at": "2000-01-01T00:00:00+00:00",
        },
    )

    def concurrent_takeover(record: dict) -> None:
        record["callback_finalizer_owner"] = "dispatcher-b"
        record["callback_finalizer_lease_expires_at"] = (
            datetime.now(timezone.utc) + timedelta(minutes=5)
        ).isoformat()

    storage.conflict_once("executions", execution["id"], concurrent_takeover)

    assert not repo.mark_callback_finalized(
        execution["id"],
        digest,
        owner="dispatcher-a",
    )
    persisted = repo.get_execution(execution["id"])
    assert persisted["callback_state"] == "accepted"
    assert persisted["callback_finalizer_owner"] == "dispatcher-b"


@pytest.mark.unit
def test_cleanup_selection_skips_cleaned_records_across_pages(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    for index in range(101):
        execution = repo.create_execution(
            {
                "run_id": "run-1",
                "project_id": "project-1",
                "batch_id": "batch-1",
                "task_id": f"task-{index}",
                "task_name": f"Task {index}",
                "page_id": "page-1",
                "browser": "chromium",
                "viewport": {"width": 1280, "height": 720},
                "status": "queued",
                "sequence": index + 1,
                "idempotency_key": f"matrix:{index + 1}",
            }
        )
        repo.update_execution(
            execution["id"],
            {
                "status": "finished",
                "created_at": f"2026-01-01T00:00:{index:03d}+00:00",
                "kubernetes_job_name": f"cleaned-{index}",
                "kubernetes_secret_name": f"cleaned-{index}",
                "kubernetes_cleaned_at": "2026-01-01T01:00:00+00:00",
            },
        )
    pending = repo.create_execution(
        {
            "run_id": "run-1",
            "project_id": "project-1",
            "batch_id": "batch-1",
            "task_id": "task-pending",
            "task_name": "Pending cleanup",
            "page_id": "page-1",
            "browser": "chromium",
            "viewport": {"width": 1280, "height": 720},
            "status": "queued",
            "sequence": 102,
            "idempotency_key": "matrix:102",
        }
    )
    repo.update_execution(
        pending["id"],
        {
            "status": "failed",
            "created_at": "2026-01-01T00:01:999+00:00",
            "kubernetes_job_name": "pending-cleanup",
            "kubernetes_secret_name": "pending-cleanup",
        },
    )

    selected = repo.list_cleanup_pending_executions(limit=1)

    assert [record["id"] for record in selected] == [pending["id"]]


@pytest.mark.unit
def test_dispatchable_selection_skips_unavailable_queued_records(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    with repo.transaction():
        for index in range(100):
            execution = repo.create_execution(
                {
                    "run_id": "run-1",
                    "project_id": "project-1",
                    "batch_id": "batch-1",
                    "task_id": f"claimed-{index}",
                    "task_name": f"Claimed {index}",
                    "page_id": "page-1",
                    "browser": "chromium",
                    "viewport": {"width": 1280, "height": 720},
                    "status": "queued",
                    "sequence": index + 1,
                    "idempotency_key": f"claimed:{index}",
                }
            )
            repo.update_execution(
                execution["id"],
                {
                    "created_at": f"2026-01-01T00:00:{index:03d}+00:00",
                    "dispatch_claim_owner": "other-dispatcher",
                    "dispatch_lease_expires_at": "2999-01-01T00:00:00+00:00",
                },
            )
        available = repo.create_execution(
            {
                "run_id": "run-1",
                "project_id": "project-1",
                "batch_id": "batch-1",
                "task_id": "available",
                "task_name": "Available",
                "page_id": "page-1",
                "browser": "chromium",
                "viewport": {"width": 1280, "height": 720},
                "status": "queued",
                "sequence": 101,
                "idempotency_key": "available",
            }
        )
        repo.update_execution(
            available["id"],
            {"created_at": "2026-01-01T00:01:999+00:00"},
        )

    selected = repo.list_dispatchable_executions(limit=1)

    assert [record["id"] for record in selected] == [available["id"]]
