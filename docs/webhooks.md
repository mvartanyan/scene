# SCENE Outbound Webhooks

SCENE emits durable, signed run lifecycle events to SPM. Webhooks are a
low-latency completion signal; they do not replace
`GET /api/runs/{run_id}/result`, which remains the canonical source for
execution counts, diff metrics, thresholds, failure reasons, and safe links.
SPM continues polling for reconciliation when delivery is delayed or absent.

The initial deployment intentionally configures one SPM endpoint through
environment variables and Kubernetes Secrets. Page, task, batch, browser,
viewport, baseline, threshold, and run configuration remains in SCENE.

## Correlation

SPM comparison launches provide:

- `spm_ticket`;
- a stable `idempotency_key`;
- `spm_criterion_id` and `spm_invocation_id`, when the client supports the
  explicit fields; or
- the compatibility note
  `SPM criterion <criterion UUID>; invocation <invocation UUID>`.

SCENE validates and canonicalizes both UUIDs, persists them on the run, and
treats the captured correlation as immutable. A retried launch with the same
idempotency key and parameters returns the existing run.

An SPM-correlated run is retained and cannot be deleted through run, batch, or
project deletion. SPM may fetch canonical state after accepting a webhook, so a
successful delivery is not sufficient proof that the run can be removed.
Failed or cancelled correlated runs also cannot be reopened for an in-place
execution retry: SPM has already made the invocation terminal. Launch a new SPM
invocation, which creates a new logical SCENE run, instead.

## Durable Event Model

Run creation and lifecycle updates append deterministic outbox markers inside
the same DynamoDB run item as the state transition. Legacy offset-free SCENE
timestamps are interpreted as UTC before a marker becomes immutable. The
webhook worker later materializes each marker into:

- one immutable event body and SHA-256 checksum;
- one deterministic delivery per configured endpoint;
- bounded attempt history, current state, response status, duration, safe error
  code, and next retry time.

This split prevents a committed run transition from losing its event if the app
or worker restarts. Conditional versions, deterministic UUIDv5 identifiers,
worker leases, and per-delivery leases provide at-least-once delivery without
allowing one event ID to acquire a different body.

Events are:

- `run.queued`;
- `run.started`;
- `run.completed`;
- `run.failed`;
- `run.cancelled`;
- `run.threshold_evaluated` for terminal comparison runs.

The exact stored body is reused for every attempt and manual redelivery.

## Version 1 Envelope

```json
{
  "schema_version": 1,
  "event_id": "stable-event-uuid",
  "event_type": "run.completed",
  "occurred_at": "2026-07-26T12:00:00+00:00",
  "created_at": "2026-07-26T12:00:00+00:00",
  "delivery_id": "stable-event-uuid",
  "scene": {
    "environment": "staging",
    "run_id": "scene-run-id",
    "batch_id": "scene-batch-id"
  },
  "spm": {
    "ticket": "SPM-193",
    "criterion_id": "criterion-uuid",
    "invocation_id": "invocation-uuid"
  }
}
```

The envelope never includes credentials, scripts, raw logs, screenshots,
presigned URLs, or result artifacts. SPM authenticates and correlates the event,
then fetches canonical state from SCENE.

## Signing

Every request includes:

- `X-Scene-Event-Id`;
- `X-Scene-Timestamp`, as Unix seconds;
- `X-Scene-Signature-Version: 1`;
- `X-Scene-Signature: sha256=<lowercase hex HMAC>`.

The signature input is the ASCII timestamp, a literal `.`, and the exact raw
body bytes:

```text
HMAC-SHA256(secret, timestamp + "." + raw_body)
```

Redirects are not followed. Production endpoints must use HTTPS on port 443,
must not contain credentials, query strings, or fragments, and must resolve
only to public addresses. Production should also set an exact hostname
allowlist; the k3s deployment permits only `pm.spherical.horse`. Local/private
HTTP endpoints require the explicit development-only private-URL switch.

## Configuration

The dedicated worker reads:

```text
SCENE_WEBHOOK_ENABLED=false
SCENE_WEBHOOK_ENDPOINT_ID=spm-1
SCENE_WEBHOOK_URL=https://pm.spherical.horse/integrations/scene/1/webhooks
SCENE_WEBHOOK_SECRET=<secret-backed value>
SCENE_WEBHOOK_TIMEOUT_SECONDS=10
SCENE_WEBHOOK_MAX_ATTEMPTS=8
SCENE_WEBHOOK_MAX_AGE_SECONDS=86400
SCENE_WEBHOOK_POLL_SECONDS=2
SCENE_WEBHOOK_ALLOW_PRIVATE_URLS=false
SCENE_WEBHOOK_ALLOWED_HOSTS=pm.spherical.horse
SCENE_WEBHOOK_CONNECT_HOST=traefik.kube-system.svc.cluster.local
```

`SCENE_WEBHOOK_SECRET` must come from a protected runtime secret and must never
be committed, printed, or put in an API response. The app needs
`SCENE_WEBHOOK_ENABLED` for aggregate readiness, but only the worker receives
the endpoint secret.

`SCENE_WEBHOOK_CONNECT_HOST` is an optional transport-only DNS override for
clusters that cannot hairpin from a pod to their public load-balancer address.
The worker still validates the public `SCENE_WEBHOOK_URL`, signs the same body,
uses the public hostname for TLS SNI and certificate verification, and sends
the public HTTP `Host` header. Only the TCP destination changes. The horse k3s
deployment pins this value to the in-cluster Traefik Service and restricts the
worker to Traefik's TLS pod port with NetworkPolicy. Do not replace the public
URL with a private HTTP endpoint.

Run the worker directly for a DynamoDB-backed development environment:

```bash
python -m app.services.webhook_worker
```

Do not run the app and worker as separate processes against the local JSON
backend. JSON storage is a single-process development adapter; k3s and any
multi-process staging use DynamoDB.

## Retry Policy

- `200..299`: success, including SPM's duplicate `200`.
- `408`, `409`, `425`, `429`, and `5xx`: retry with bounded exponential
  backoff and jitter, preserving event ID and body.
- Other `4xx`: permanent failure requiring configuration/correlation repair.
- Network failures and timeouts: retry.
- Maximum attempts or event age: permanent failure with
  `retry_policy_exhausted`.

Delivery is at least once. SPM must deduplicate by event ID. A permanent
delivery can be queued again after the cause is fixed. Manual redelivery starts
a new retry generation with a fresh attempt/age budget while preserving total
attempt history:

```bash
curl -X POST \
  -H "Authorization: Bearer $SCENE_API_TOKEN" \
  http://127.0.0.1:8000/api/webhook-deliveries/<delivery-id>/redeliver
```

Only terminal deliveries can be manually redelivered. An in-flight
`pending`/`retry` delivery returns HTTP 409.

The endpoint URL stored on a delivery is immutable. Changing the
environment-configured URL affects newly created deliveries but does not
rewrite or wedge historical records. Reconcile pending deliveries before an
endpoint change; SCENE-23 owns managed endpoint migration.

## Inspection

The protected agent API exposes redacted records:

```text
GET /api/webhook-events?run_id=<run-id>
GET /api/webhook-deliveries?run_id=<run-id>&status=<status>
GET /api/webhook-deliveries/{delivery_id}
POST /api/webhook-deliveries/{delivery_id}/redeliver
```

Responses omit event bodies, endpoint URLs, secrets, request headers, and raw
response bodies. Attempt history retains only bounded status, timing, and safe
error-code fields.

When webhooks are enabled, `/readyz` reports the worker heartbeat and
configuration as a non-blocking dependency. A downstream SPM outage therefore
cannot remove primary SCENE app pods from service. The dedicated webhook-worker
pod has its own required lease/configuration readiness probe. `/metrics`
reports:

- `scene_webhook_worker_enabled`;
- `scene_webhook_delivery_queue_depth`;
- `scene_webhook_delivery_oldest_age_seconds`;
- `scene_webhook_deliveries_total{outcome="attempt|success|failure"}`.

## Secret Rotation

SPM supports current and previous secret references. Rotate without losing
delivery:

1. Provision the new secret in both secret stores without displaying it.
2. Configure SPM to accept the new current secret and retain the old secret as
   previous.
3. Roll the SCENE worker onto the new secret.
4. Confirm successful delivery and an empty/reconciled queue.
5. Retire the old SPM secret only after the replay window and rollout overlap
   have elapsed.

If a delivery reached permanent `401` during a failed rotation, repair the
references and manually redeliver it. The immutable body remains valid; each
attempt receives a fresh timestamp and signature.
