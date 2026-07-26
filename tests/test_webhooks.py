from __future__ import annotations

import hashlib
import socket
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, Sequence, Tuple

import pytest
from pydantic import ValidationError

from app.schemas import BatchComparisonRunCreate
from app.services.storage import LocalDynamoStorage, SceneRepository
from app.services.storage_types import StorageConflictError
from app.services.webhooks import (
    WebhookConfig,
    WebhookConfigurationError,
    WebhookDeliveryWorker,
    attempt_webhook_delivery,
    canonical_event_bytes,
    load_webhook_config,
    materialize_webhook_events,
    signature_headers,
    validate_webhook_url,
)

CRITERION_ID = "11111111-1111-4111-8111-111111111111"
INVOCATION_ID = "22222222-2222-4222-8222-222222222222"
OCCURRED_AT = "2026-07-26T12:34:56+00:00"
PUBLIC_ADDRESS = "93.184.216.34"


def _resolver_for(address: str) -> Callable[..., Sequence[Tuple[object, ...]]]:
    def resolve(
        _hostname: str,
        port: int,
        *,
        type: int,
    ) -> Sequence[Tuple[object, ...]]:
        return [(socket.AF_INET, type, socket.IPPROTO_TCP, "", (address, port))]

    return resolve


def _public_resolver(
    hostname: str,
    port: int,
    *,
    type: int,
) -> Sequence[Tuple[object, ...]]:
    return _resolver_for(PUBLIC_ADDRESS)(hostname, port, type=type)


def _marker() -> Dict[str, object]:
    return {
        "event_id": "event-123",
        "event_type": "run.completed",
        "occurred_at": OCCURRED_AT,
        "environment": "Staging",
        "run_id": "run-789",
        "batch_id": "batch-456",
        "ticket": "SPM-193",
        "criterion_id": CRITERION_ID,
        "invocation_id": INVOCATION_ID,
    }


def _run_payload(**overrides: object) -> Dict[str, object]:
    payload: Dict[str, object] = {
        "project_id": "project-1",
        "batch_id": "batch-1",
        "purpose": "comparison",
        "spm_ticket": "SPM-193",
        "note": (
            f"SPM criterion {CRITERION_ID}; "
            f"invocation {INVOCATION_ID}"
        ),
    }
    payload.update(overrides)
    return payload


def _repo(path: Path) -> tuple[SceneRepository, LocalDynamoStorage]:
    storage = LocalDynamoStorage(path)
    return SceneRepository(storage), storage


def _event_payload(event_id: str = "event-1") -> Dict[str, object]:
    body = b'{"event_id":"event-1","event_type":"run.completed"}'
    return {
        "id": event_id,
        "event_type": "run.completed",
        "occurred_at": OCCURRED_AT,
        "run_id": "run-1",
        "batch_id": "batch-1",
        "body": body.decode("ascii"),
        "body_sha256": hashlib.sha256(body).hexdigest(),
        "created_at": OCCURRED_AT,
    }


def _seed_delivery(
    repo: SceneRepository,
    *,
    endpoint_url: str = "https://hooks.example.test/spm",
) -> Dict[str, object]:
    event = repo.ensure_webhook_event(_event_payload())
    return repo.ensure_webhook_delivery(
        event,
        endpoint_id="spm",
        endpoint_url=endpoint_url,
    )


def _expire(
    storage: LocalDynamoStorage,
    collection: str,
    item_id: str,
    field: str,
) -> None:
    record = storage.get(collection, item_id)
    assert record is not None
    record[field] = "2000-01-01T00:00:00+00:00"
    storage.upsert(collection, item_id, record)


def test_canonical_event_body_is_byte_exact_and_stable() -> None:
    expected = (
        b'{"created_at":"2026-07-26T12:34:56+00:00",'
        b'"delivery_id":"event-123","event_id":"event-123",'
        b'"event_type":"run.completed",'
        b'"occurred_at":"2026-07-26T12:34:56+00:00",'
        b'"scene":{"batch_id":"batch-456","environment":"staging",'
        b'"run_id":"run-789"},"schema_version":1,'
        b'"spm":{"criterion_id":"11111111-1111-4111-8111-111111111111",'
        b'"invocation_id":"22222222-2222-4222-8222-222222222222",'
        b'"ticket":"SPM-193"}}'
    )

    assert canonical_event_bytes(_marker()) == expected
    assert canonical_event_bytes(dict(reversed(list(_marker().items())))) == expected


def test_signature_headers_match_exact_spm_hmac_vector() -> None:
    raw_body = canonical_event_bytes(_marker())

    assert signature_headers(
        event_id="event-123",
        raw_body=raw_body,
        secret="correct horse battery staple",
        timestamp=1_785_072_000,
    ) == {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Scene-Event-Id": "event-123",
        "X-Scene-Timestamp": "1785072000",
        "X-Scene-Signature-Version": "1",
        "X-Scene-Signature": (
            "sha256="
            "53bbe7054878ae833fedfb0399541c84ade54b0f89f9f2eb7bc63f29e3b2e00a"
        ),
    }


@pytest.mark.parametrize("status", [200, 202])
def test_spm_accepted_and_duplicate_responses_are_success(status: int) -> None:
    observed: Dict[str, object] = {}
    raw_body = canonical_event_bytes(_marker())

    def sender(
        url: str,
        body: bytes,
        headers: Dict[str, str],
        timeout_seconds: float,
    ) -> int:
        observed.update(
            url=url,
            body=body,
            headers=headers,
            timeout_seconds=timeout_seconds,
        )
        return status

    result = attempt_webhook_delivery(
        endpoint_url="https://hooks.example.test/spm",
        event_id="event-123",
        raw_body=raw_body,
        secret="shared-secret",
        timeout_seconds=7.0,
        now=datetime(2026, 7, 26, 12, 0, tzinfo=timezone.utc),
        resolver=_public_resolver,
        sender=sender,
    )

    assert result.outcome == "succeeded"
    assert result.response_status == status
    assert result.error_code is None
    assert observed["url"] == "https://hooks.example.test/spm"
    assert observed["body"] is raw_body
    assert observed["timeout_seconds"] == 7.0
    assert observed["headers"]["X-Scene-Event-Id"] == "event-123"
    assert observed["headers"]["X-Scene-Timestamp"] == "1785067200"


@pytest.mark.parametrize("status", [409, 429, 500, 502, 503])
def test_retryable_spm_responses_are_classified_for_retry(status: int) -> None:
    result = attempt_webhook_delivery(
        endpoint_url="https://hooks.example.test/spm",
        event_id="event-123",
        raw_body=b"{}",
        secret="shared-secret",
        timeout_seconds=5.0,
        resolver=_public_resolver,
        sender=lambda *_args: status,
    )

    assert result.outcome == "retry"
    assert result.response_status == status
    assert result.error_code == f"http_{status}"


@pytest.mark.parametrize("status", [300, 400, 401, 403, 404, 410, 422])
def test_non_retryable_spm_responses_are_permanent_failures(status: int) -> None:
    result = attempt_webhook_delivery(
        endpoint_url="https://hooks.example.test/spm",
        event_id="event-123",
        raw_body=b"{}",
        secret="shared-secret",
        timeout_seconds=5.0,
        resolver=_public_resolver,
        sender=lambda *_args: status,
    )

    assert result.outcome == "permanent_failure"
    assert result.response_status == status
    assert result.error_code == f"http_{status}"


def test_sender_timeout_is_retryable_without_leaking_exception_details() -> None:
    def timeout_sender(*_args: object) -> int:
        raise TimeoutError("socket details must not be persisted")

    result = attempt_webhook_delivery(
        endpoint_url="https://hooks.example.test/spm",
        event_id="event-123",
        raw_body=b"{}",
        secret="shared-secret",
        timeout_seconds=1.0,
        resolver=_public_resolver,
        sender=timeout_sender,
    )

    assert result.outcome == "retry"
    assert result.response_status is None
    assert result.error_code == "network_error"


@pytest.mark.parametrize(
    ("url", "error"),
    [
        ("http://hooks.example.test/spm", "webhook_url_scheme_invalid"),
        (
            "https://user:password@hooks.example.test/spm",
            "webhook_url_authority_invalid",
        ),
        (
            "https://hooks.example.test/spm?secret=value",
            "webhook_url_query_or_fragment_forbidden",
        ),
        (
            "https://hooks.example.test/spm#fragment",
            "webhook_url_query_or_fragment_forbidden",
        ),
        ("https://hooks.example.test:8443/spm", "webhook_url_port_invalid"),
    ],
)
def test_webhook_url_rejects_unsafe_authority_and_url_components(
    url: str,
    error: str,
) -> None:
    with pytest.raises(WebhookConfigurationError, match=error):
        validate_webhook_url(url, resolver=_public_resolver)


@pytest.mark.parametrize(
    "address",
    [
        "127.0.0.1",
        "10.0.0.1",
        "169.254.169.254",
        "::1",
        "fc00::1",
    ],
)
def test_webhook_url_rejects_non_global_resolved_addresses(address: str) -> None:
    with pytest.raises(
        WebhookConfigurationError,
        match="webhook_url_address_forbidden",
    ):
        validate_webhook_url(
            "https://hooks.example.test/spm",
            resolver=_resolver_for(address),
        )


def test_webhook_url_rejects_mixed_public_and_private_dns_answers() -> None:
    def mixed_resolver(
        _hostname: str,
        port: int,
        *,
        type: int,
    ) -> Sequence[Tuple[object, ...]]:
        return [
            (socket.AF_INET, type, socket.IPPROTO_TCP, "", (PUBLIC_ADDRESS, port)),
            (socket.AF_INET, type, socket.IPPROTO_TCP, "", ("127.0.0.1", port)),
        ]

    with pytest.raises(
        WebhookConfigurationError,
        match="webhook_url_address_forbidden",
    ):
        validate_webhook_url(
            "https://hooks.example.test/spm",
            resolver=mixed_resolver,
        )


def test_webhook_url_honors_an_exact_production_host_allowlist() -> None:
    assert (
        validate_webhook_url(
            "https://pm.spherical.horse/integrations/scene/1/webhooks",
            allowed_hosts=("PM.SPHERICAL.HORSE",),
            resolver=_public_resolver,
        )
        == "https://pm.spherical.horse/integrations/scene/1/webhooks"
    )
    with pytest.raises(
        WebhookConfigurationError,
        match="webhook_url_host_forbidden",
    ):
        validate_webhook_url(
            "https://other.example.test/webhooks",
            allowed_hosts=("pm.spherical.horse",),
            resolver=_public_resolver,
        )


def test_private_http_webhook_is_only_allowed_by_explicit_override() -> None:
    assert (
        validate_webhook_url(
            "http://127.0.0.1:8080/spm",
            allow_private=True,
            resolver=_resolver_for("127.0.0.1"),
        )
        == "http://127.0.0.1:8080/spm"
    )


def test_enabled_config_requires_safe_values_and_redacts_the_secret_repr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SCENE_WEBHOOK_ENABLED", "true")
    monkeypatch.setenv("SCENE_WEBHOOK_ENDPOINT_ID", "spm-1")
    monkeypatch.setenv("SCENE_WEBHOOK_URL", "http://127.0.0.1:8080/spm")
    monkeypatch.setenv("SCENE_WEBHOOK_SECRET", "must-not-appear")
    monkeypatch.setenv("SCENE_WEBHOOK_ALLOW_PRIVATE_URLS", "true")
    monkeypatch.setenv("SCENE_WEBHOOK_ALLOWED_HOSTS", "127.0.0.1")

    config = load_webhook_config()

    assert config.endpoint_id == "spm-1"
    assert config.secret == "must-not-appear"
    assert config.allowed_hosts == ("127.0.0.1",)
    assert "must-not-appear" not in repr(config)
    monkeypatch.setenv("SCENE_WEBHOOK_ENDPOINT_ID", "../invalid")
    with pytest.raises(
        WebhookConfigurationError,
        match="webhook_endpoint_id_invalid",
    ):
        load_webhook_config()


def test_correlation_note_creates_deterministic_lifecycle_outbox(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("SCENE_ENV", "Staging")
    state_path = tmp_path / "state.json"
    repo, _storage = _repo(state_path)
    run = repo.create_run(
        _run_payload(
            idempotency_key="invocation-1",
        )
    )

    assert [item["event_type"] for item in run["webhook_outbox"]] == ["run.queued"]
    queued_marker = dict(run["webhook_outbox"][0])
    assert queued_marker["environment"] == "staging"
    assert queued_marker["ticket"] == "SPM-193"
    assert queued_marker["criterion_id"] == CRITERION_ID
    assert queued_marker["invocation_id"] == INVOCATION_ID
    assert run["spm_criterion_id"] == CRITERION_ID
    assert run["spm_invocation_id"] == INVOCATION_ID

    started = repo.update_run(
        str(run["id"]),
        {
            "status": "executing",
            "started_at": "2026-07-26T12:35:00+00:00",
        },
    )
    assert started is not None
    repo.update_run(
        str(run["id"]),
        {
            "status": "executing",
            "started_at": "2026-07-26T12:35:00+00:00",
        },
    )
    finished = repo.update_run(
        str(run["id"]),
        {
            "status": "finished",
            "completed_at": "2026-07-26T12:36:00+00:00",
        },
    )
    assert finished is not None
    assert [item["event_type"] for item in finished["webhook_outbox"]] == [
        "run.queued",
        "run.started",
        "run.completed",
        "run.threshold_evaluated",
    ]
    assert len({item["event_id"] for item in finished["webhook_outbox"]}) == 4
    assert finished["webhook_outbox"][0] == queued_marker

    reloaded_repo, _ = _repo(state_path)
    reloaded = reloaded_repo.ensure_run_webhook_markers(str(run["id"]))
    assert reloaded is not None
    assert reloaded["webhook_outbox"] == finished["webhook_outbox"]


def test_invalid_correlation_note_is_ignored_but_explicit_ids_are_supported(
    tmp_path: Path,
) -> None:
    repo, _storage = _repo(tmp_path / "state.json")

    invalid = repo.create_run(
        _run_payload(
            note=f"criterion {CRITERION_ID}; invocation {INVOCATION_ID}",
        )
    )
    explicit = repo.create_run(
        _run_payload(
            note="created by an agent",
            spm_criterion_id=CRITERION_ID.upper(),
            spm_invocation_id=INVOCATION_ID.upper(),
        )
    )

    assert "webhook_outbox" not in invalid
    assert explicit["webhook_outbox"][0]["criterion_id"] == CRITERION_ID
    assert explicit["webhook_outbox"][0]["invocation_id"] == INVOCATION_ID


def test_terminal_run_that_never_started_does_not_emit_a_started_event(
    tmp_path: Path,
) -> None:
    repo, _storage = _repo(tmp_path / "state.json")

    failed = repo.create_run(
        _run_payload(
            status="failed",
            completed_at="2026-07-26T12:36:00+00:00",
        )
    )

    assert [item["event_type"] for item in failed["webhook_outbox"]] == [
        "run.queued",
        "run.failed",
        "run.threshold_evaluated",
    ]


def test_explicit_spm_correlation_fields_require_and_normalize_real_uuids() -> None:
    payload = BatchComparisonRunCreate.model_validate(
        {
            "spm_criterion_id": CRITERION_ID.upper(),
            "spm_invocation_id": INVOCATION_ID.upper(),
        }
    )

    assert payload.spm_criterion_id == CRITERION_ID
    assert payload.spm_invocation_id == INVOCATION_ID
    with pytest.raises(ValidationError, match="must be a valid UUID"):
        BatchComparisonRunCreate.model_validate(
            {
                "spm_criterion_id": "------------------------------------",
                "spm_invocation_id": INVOCATION_ID,
            }
        )


def test_materialization_is_idempotent_and_event_bodies_are_immutable(
    tmp_path: Path,
) -> None:
    repo, _storage = _repo(tmp_path / "state.json")
    run = repo.create_run(
        _run_payload(
            idempotency_key="invocation-1",
        )
    )
    repo.update_run(
        str(run["id"]),
        {
            "status": "executing",
            "started_at": "2026-07-26T12:35:00+00:00",
        },
    )
    repo.update_run(
        str(run["id"]),
        {
            "status": "finished",
            "completed_at": "2026-07-26T12:36:00+00:00",
        },
    )
    config = WebhookConfig(
        enabled=True,
        endpoint_url="https://hooks.example.test/spm",
        secret="shared-secret",
    )

    materialize_webhook_events(repo, config)
    first_events = repo.list_webhook_events(run_id=str(run["id"]))
    first_deliveries = repo.list_webhook_deliveries(run_id=str(run["id"]))
    materialize_webhook_events(repo, config)

    assert len(first_events) == 4
    assert len(repo.list_webhook_events(run_id=str(run["id"]))) == 4
    assert len(first_deliveries) == 4
    assert len(repo.list_webhook_deliveries(run_id=str(run["id"]))) == 4
    assert all(
        hashlib.sha256(str(event["body"]).encode("ascii")).hexdigest()
        == event["body_sha256"]
        for event in first_events
    )

    original = first_events[0]
    changed = dict(original)
    changed["body"] = '{"changed":true}'
    changed["body_sha256"] = hashlib.sha256(
        str(changed["body"]).encode("ascii")
    ).hexdigest()
    with pytest.raises(StorageConflictError, match="cannot change immutable body"):
        repo.ensure_webhook_event(changed)

    delivery = first_deliveries[0]
    preserved_delivery = repo.ensure_webhook_delivery(
        original,
        endpoint_id=str(delivery["endpoint_id"]),
        endpoint_url="https://other.example.test/spm",
    )
    assert preserved_delivery["endpoint_url"] == delivery["endpoint_url"]

    with pytest.raises(ValueError, match="SPM run correlation is immutable"):
        repo.update_run(str(run["id"]), {"spm_ticket": "SPM-CHANGED"})
    materialize_webhook_events(repo, config)
    persisted = repo.get_webhook_event(str(original["id"]))
    assert persisted is not None
    assert persisted["body"] == original["body"]
    assert persisted["body_sha256"] == original["body_sha256"]


@pytest.mark.parametrize("delete_scope", ["run", "batch", "project"])
def test_spm_correlated_runs_are_retained_for_canonical_reconciliation(
    delete_scope: str,
    tmp_path: Path,
) -> None:
    repo, _storage = _repo(tmp_path / f"{delete_scope}.json")
    project = repo.create_project({"name": "Project", "slug": "project"})
    batch = repo.create_batch(
        {"project_id": project["id"], "name": "Batch", "task_ids": []}
    )
    run = repo.create_run(
        {
            **_run_payload(),
            "project_id": project["id"],
            "batch_id": batch["id"],
            "status": "failed",
        }
    )

    with pytest.raises(ValueError, match="SPM-correlated run"):
        if delete_scope == "run":
            repo.delete_run(str(run["id"]))
        elif delete_scope == "batch":
            repo.delete_batch(str(batch["id"]))
        else:
            repo.delete_project(str(project["id"]))

    assert repo.get_project(str(project["id"])) is not None
    assert repo.get_batch(str(batch["id"])) is not None
    assert repo.get_run(str(run["id"])) is not None


def test_idempotent_run_submission_reuses_outbox_and_rejects_changed_correlation(
    tmp_path: Path,
) -> None:
    repo, _storage = _repo(tmp_path / "state.json")
    payload = _run_payload(idempotency_key="spm-invocation-1")

    first = repo.create_run(payload)
    duplicate = repo.create_run(payload)

    assert duplicate["id"] == first["id"]
    assert duplicate["webhook_outbox"] == first["webhook_outbox"]

    with pytest.raises(StorageConflictError):
        repo.create_run(
            {
                **payload,
                "spm_criterion_id": "33333333-3333-4333-8333-333333333333",
                "spm_invocation_id": INVOCATION_ID,
            }
        )


@pytest.mark.parametrize("status", [200, 202])
def test_worker_persists_spm_success_and_does_not_redeliver(
    status: int,
    tmp_path: Path,
) -> None:
    repo, _storage = _repo(tmp_path / "state.json")
    delivery = _seed_delivery(repo)
    calls = 0

    def sender(*_args: object) -> int:
        nonlocal calls
        calls += 1
        return status

    worker = WebhookDeliveryWorker(
        repo,
        WebhookConfig(
            enabled=True,
            endpoint_url="https://hooks.example.test/spm",
            secret="shared-secret",
        ),
        owner="worker-a",
        sender=sender,
        resolver=_public_resolver,
    )

    assert worker.run_cycle() == 1
    assert worker.run_cycle() == 0
    persisted = repo.get_webhook_delivery(str(delivery["id"]))
    assert persisted is not None
    assert persisted["status"] == "succeeded"
    assert persisted["attempt_count"] == 1
    assert persisted["last_response_status"] == status
    assert calls == 1


def test_worker_stops_at_retry_policy_and_preserves_safe_failure(
    tmp_path: Path,
) -> None:
    repo, _storage = _repo(tmp_path / "state.json")
    delivery = _seed_delivery(repo)
    worker = WebhookDeliveryWorker(
        repo,
        WebhookConfig(
            enabled=True,
            endpoint_url="https://hooks.example.test/spm",
            secret="shared-secret",
            max_attempts=1,
        ),
        owner="worker-a",
        sender=lambda *_args: 503,
        resolver=_public_resolver,
    )

    assert worker.run_cycle() == 1
    persisted = repo.get_webhook_delivery(str(delivery["id"]))
    assert persisted is not None
    assert persisted["status"] == "permanent_failure"
    assert persisted["last_response_status"] == 503
    assert persisted["last_error"] == "retry_policy_exhausted"
    assert persisted["next_attempt_at"] is None


def test_manual_redelivery_gets_a_fresh_retry_budget_and_keeps_total_history(
    tmp_path: Path,
) -> None:
    repo, storage = _repo(tmp_path / "state.json")
    delivery = _seed_delivery(repo)
    worker = WebhookDeliveryWorker(
        repo,
        WebhookConfig(
            enabled=True,
            endpoint_url="https://hooks.example.test/spm",
            secret="shared-secret",
            max_attempts=2,
        ),
        owner="worker-a",
        sender=lambda *_args: 503,
        resolver=_public_resolver,
    )

    assert worker.run_cycle() == 1
    _expire(
        storage,
        "webhook_deliveries",
        str(delivery["id"]),
        "next_attempt_at",
    )
    assert worker.run_cycle() == 1
    exhausted = repo.get_webhook_delivery(str(delivery["id"]))
    assert exhausted is not None
    assert exhausted["status"] == "permanent_failure"
    assert exhausted["attempt_count"] == 2
    assert exhausted["generation_attempt_count"] == 2

    redelivered = repo.redeliver_webhook(str(delivery["id"]))
    assert redelivered is not None
    assert redelivered["redelivery_generation"] == 1
    assert redelivered["generation_attempt_count"] == 0
    assert worker.run_cycle() == 1

    retrying = repo.get_webhook_delivery(str(delivery["id"]))
    assert retrying is not None
    assert retrying["status"] == "retry"
    assert retrying["attempt_count"] == 3
    assert retrying["generation_attempt_count"] == 1
    assert retrying["attempts"][-1]["generation"] == 1
    assert retrying["attempts"][-1]["generation_number"] == 1


def test_worker_rejects_a_corrupted_immutable_event_before_network_delivery(
    tmp_path: Path,
) -> None:
    repo, storage = _repo(tmp_path / "state.json")
    delivery = _seed_delivery(repo)
    event = storage.get("webhook_events", str(delivery["event_id"]))
    assert event is not None
    event["body"] = '{"tampered":true}'
    storage.upsert("webhook_events", str(event["id"]), event)
    calls = 0

    def sender(*_args: object) -> int:
        nonlocal calls
        calls += 1
        return 202

    worker = WebhookDeliveryWorker(
        repo,
        WebhookConfig(
            enabled=True,
            endpoint_url="https://hooks.example.test/spm",
            secret="shared-secret",
        ),
        owner="worker-a",
        sender=sender,
        resolver=_public_resolver,
    )

    assert worker.run_cycle() == 1
    persisted = repo.get_webhook_delivery(str(delivery["id"]))
    assert persisted is not None
    assert persisted["status"] == "permanent_failure"
    assert persisted["last_error"] == "event_body_invalid"
    assert calls == 0


def test_delivery_and_worker_leases_survive_restart_and_retry_history_is_preserved(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    repo_a, storage_a = _repo(state_path)
    delivery = _seed_delivery(repo_a)
    responses = iter([503, 202])

    def sender(*_args: object) -> int:
        return next(responses)

    worker_a = WebhookDeliveryWorker(
        repo_a,
        WebhookConfig(
            enabled=True,
            endpoint_url="https://hooks.example.test/spm",
            secret="shared-secret",
        ),
        owner="worker-a",
        sender=sender,
        resolver=_public_resolver,
    )
    assert worker_a.run_cycle() == 1
    retrying = repo_a.get_webhook_delivery(str(delivery["id"]))
    assert retrying is not None
    assert retrying["status"] == "retry"
    assert retrying["attempt_count"] == 1
    assert retrying["attempts"][0]["response_status"] == 503

    repo_b, storage_b = _repo(state_path)
    worker_b = WebhookDeliveryWorker(
        repo_b,
        WebhookConfig(
            enabled=True,
            endpoint_url="https://hooks.example.test/spm",
            secret="shared-secret",
        ),
        owner="worker-b",
        sender=sender,
        resolver=_public_resolver,
    )
    assert worker_b.run_cycle() == 0

    _expire(storage_b, "leases", "webhook-worker", "expires_at")
    retry_record = storage_b.get("webhook_deliveries", str(delivery["id"]))
    assert retry_record is not None
    retry_record["next_attempt_at"] = "2000-01-01T00:00:00+00:00"
    storage_b.upsert("webhook_deliveries", str(delivery["id"]), retry_record)

    assert worker_b.run_cycle() == 1
    succeeded = repo_b.get_webhook_delivery(str(delivery["id"]))
    assert succeeded is not None
    assert succeeded["status"] == "succeeded"
    assert succeeded["attempt_count"] == 2
    assert [attempt["outcome"] for attempt in succeeded["attempts"]] == [
        "retry",
        "succeeded",
    ]
    assert repo_b.operational_metrics()["counters"] == {
        "webhook_attempt_total": 2,
        "webhook_failure_total": 1,
        "webhook_success_total": 1,
    }

    # The first process still has a stale in-memory view, proving the assertions
    # above came from the repository reopened after the simulated restart.
    assert storage_a.get("webhook_deliveries", str(delivery["id"]))["status"] == "retry"


def test_active_delivery_lease_blocks_second_owner_until_expiry(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    repo_a, _storage_a = _repo(state_path)
    delivery = _seed_delivery(repo_a)
    assert repo_a.claim_webhook_delivery(
        str(delivery["id"]),
        owner="worker-a",
        lease_seconds=60,
    )

    repo_b, storage_b = _repo(state_path)
    assert (
        repo_b.claim_webhook_delivery(
            str(delivery["id"]),
            owner="worker-b",
            lease_seconds=60,
        )
        is None
    )

    _expire(
        storage_b,
        "webhook_deliveries",
        str(delivery["id"]),
        "lease_expires_at",
    )
    claimed = repo_b.claim_webhook_delivery(
        str(delivery["id"]),
        owner="worker-b",
        lease_seconds=60,
    )
    assert claimed is not None
    assert claimed["lease_owner"] == "worker-b"


def test_claim_rechecks_due_time_after_a_stale_candidate_read(
    tmp_path: Path,
) -> None:
    repo, storage = _repo(tmp_path / "state.json")
    delivery = _seed_delivery(repo)
    stale_candidate = repo.list_due_webhook_deliveries()[0]
    persisted = storage.get("webhook_deliveries", str(delivery["id"]))
    assert persisted is not None
    persisted["status"] = "retry"
    persisted["next_attempt_at"] = "2999-01-01T00:00:00+00:00"
    storage.upsert("webhook_deliveries", str(delivery["id"]), persisted)

    assert stale_candidate["id"] == delivery["id"]
    assert (
        repo.claim_webhook_delivery(
            str(delivery["id"]),
            owner="stale-worker",
        )
        is None
    )


def test_worker_renews_its_heartbeat_between_network_deliveries(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo, _storage = _repo(tmp_path / "state.json")
    _seed_delivery(repo)
    original_acquire = repo.acquire_webhook_worker_lease
    acquire_calls = 0

    def acquire(owner: str, *, lease_seconds: int = 30) -> bool:
        nonlocal acquire_calls
        acquire_calls += 1
        return original_acquire(owner, lease_seconds=lease_seconds)

    monkeypatch.setattr(repo, "acquire_webhook_worker_lease", acquire)
    worker = WebhookDeliveryWorker(
        repo,
        WebhookConfig(
            enabled=True,
            endpoint_url="https://hooks.example.test/spm",
            secret="shared-secret",
        ),
        owner="worker-a",
        sender=lambda *_args: 202,
        resolver=_public_resolver,
    )

    assert worker.run_cycle() == 1
    assert acquire_calls >= 4
