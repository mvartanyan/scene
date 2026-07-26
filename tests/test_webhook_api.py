from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Generator, Tuple

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services.storage import LocalDynamoStorage, SceneRepository, get_repository


@pytest.fixture
def webhook_api(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> Generator[Tuple[TestClient, SceneRepository, str], None, None]:
    monkeypatch.setenv("SCENE_API_TOKEN", "agent-secret")
    repo = SceneRepository(LocalDynamoStorage(tmp_path / "state.json"))
    raw_body = (
        b'{"event_id":"event-sensitive","event_type":"run.completed",'
        b'"secret":"must-not-be-returned"}'
    )
    event = repo.ensure_webhook_event(
        {
            "id": "event-sensitive",
            "event_type": "run.completed",
            "occurred_at": "2026-07-26T12:34:56+00:00",
            "run_id": "run-1",
            "batch_id": "batch-1",
            "body": raw_body.decode("ascii"),
            "body_sha256": hashlib.sha256(raw_body).hexdigest(),
            "created_at": "2026-07-26T12:34:56+00:00",
        }
    )
    delivery = repo.ensure_webhook_delivery(
        event,
        endpoint_id="spm",
        endpoint_url="https://hooks.example.test/sensitive/path",
    )
    claimed = repo.claim_webhook_delivery(
        str(delivery["id"]),
        owner="worker-a",
    )
    assert claimed is not None
    completed = repo.complete_webhook_delivery_attempt(
        str(delivery["id"]),
        owner="worker-a",
        outcome="permanent_failure",
        response_status=422,
        duration_seconds=0.125,
        error="http_422",
        next_attempt_at=None,
    )
    assert completed is not None

    app.dependency_overrides[get_repository] = lambda: repo
    with TestClient(app) as client:
        yield client, repo, str(delivery["id"])
    app.dependency_overrides.clear()


def _auth_headers() -> dict[str, str]:
    return {"Authorization": "Bearer agent-secret"}


def test_webhook_history_api_requires_agent_auth_and_redacts_payloads(
    webhook_api: Tuple[TestClient, SceneRepository, str],
) -> None:
    client, _repo, delivery_id = webhook_api

    assert client.get("/api/webhook-events").status_code == 401
    assert (
        client.get(
            "/api/webhook-deliveries",
            headers={"Authorization": "Bearer wrong"},
        ).status_code
        == 403
    )

    events_response = client.get(
        "/api/webhook-events?run_id=run-1",
        headers=_auth_headers(),
    )
    assert events_response.status_code == 200
    assert events_response.json() == [
        {
            "id": "event-sensitive",
            "event_type": "run.completed",
            "occurred_at": "2026-07-26T12:34:56+00:00",
            "run_id": "run-1",
            "batch_id": "batch-1",
            "body_sha256": (
                "70df00759be5324f382edc6c8615e968edb000621d5ebde871f07a49fb675a3d"
            ),
            "created_at": "2026-07-26T12:34:56+00:00",
        }
    ]
    assert '"body":' not in events_response.text
    assert "must-not-be-returned" not in events_response.text

    deliveries_response = client.get(
        "/api/webhook-deliveries?run_id=run-1&status=permanent_failure",
        headers=_auth_headers(),
    )
    assert deliveries_response.status_code == 200
    deliveries = deliveries_response.json()
    assert len(deliveries) == 1
    assert deliveries[0]["id"] == delivery_id
    assert deliveries[0]["status"] == "permanent_failure"
    assert deliveries[0]["attempt_count"] == 1
    assert deliveries[0]["redelivery_generation"] == 0
    assert deliveries[0]["generation_attempt_count"] == 1
    assert deliveries[0]["attempts"][0]["error"] == "http_422"
    assert deliveries[0]["attempts"][0]["generation"] == 0
    assert deliveries[0]["attempts"][0]["generation_number"] == 1
    assert "endpoint_url" not in deliveries[0]
    assert "lease_owner" not in deliveries[0]
    assert "lease_expires_at" not in deliveries[0]
    assert "sensitive/path" not in deliveries_response.text

    detail_response = client.get(
        f"/api/webhook-deliveries/{delivery_id}",
        headers=_auth_headers(),
    )
    assert detail_response.status_code == 200
    assert detail_response.json() == deliveries[0]


def test_manual_redelivery_preserves_history_and_redacts_destination(
    webhook_api: Tuple[TestClient, SceneRepository, str],
) -> None:
    client, repo, delivery_id = webhook_api

    response = client.post(
        f"/api/webhook-deliveries/{delivery_id}/redeliver",
        headers=_auth_headers(),
    )

    assert response.status_code == 202
    payload = response.json()
    assert payload["id"] == delivery_id
    assert payload["status"] == "pending"
    assert payload["attempt_count"] == 1
    assert payload["redelivery_generation"] == 1
    assert payload["generation_attempt_count"] == 0
    assert payload["attempts"][0]["response_status"] == 422
    assert "completed_at" not in payload
    assert "endpoint_url" not in payload
    assert "sensitive/path" not in response.text

    persisted = repo.get_webhook_delivery(delivery_id)
    assert persisted is not None
    assert persisted["endpoint_url"] == "https://hooks.example.test/sensitive/path"
    assert persisted["status"] == "pending"
    assert persisted["attempt_count"] == 1
    assert persisted["redelivery_generation"] == 1
    assert persisted["generation_attempt_count"] == 0
    assert "completed_at" not in persisted

    duplicate_request = client.post(
        f"/api/webhook-deliveries/{delivery_id}/redeliver",
        headers=_auth_headers(),
    )
    assert duplicate_request.status_code == 409
    assert duplicate_request.json()["detail"] == "Webhook delivery is not terminal"


def test_webhook_delivery_api_rejects_invalid_filters_and_unknown_ids(
    webhook_api: Tuple[TestClient, SceneRepository, str],
) -> None:
    client, _repo, _delivery_id = webhook_api

    invalid_filter = client.get(
        "/api/webhook-deliveries?status=unknown",
        headers=_auth_headers(),
    )
    missing_detail = client.get(
        "/api/webhook-deliveries/missing",
        headers=_auth_headers(),
    )
    missing_redelivery = client.post(
        "/api/webhook-deliveries/missing/redeliver",
        headers=_auth_headers(),
    )

    assert invalid_filter.status_code == 400
    assert invalid_filter.json()["detail"] == "Invalid webhook delivery status"
    assert missing_detail.status_code == 404
    assert missing_redelivery.status_code == 404
