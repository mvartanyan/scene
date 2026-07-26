# SCENE Operations Endpoints

SCENE exposes four process-level endpoints outside the agent API:

- `GET /healthz` returns HTTP 200 when the web process can serve requests. It
  does not initialize or probe DynamoDB, S3, the runner, or Kubernetes.
- `GET /readyz` returns HTTP 200 only when state, artifacts, runner
  configuration, and any required k3s dispatcher are ready. It also reports an
  enabled outbound webhook worker as a non-blocking dependency. It returns a
  sanitized HTTP 503 report for primary dependency initialization errors,
  failed probes, stale dispatcher leases, or denied Kubernetes capabilities.
- `GET /version` returns `SCENE_VERSION`, `SCENE_GIT_SHA`, `SCENE_BUILD_TIME`,
  `SCENE_ENV`, app/runner image references, configured backend names, and the
  persisted-state schema version, using `unknown`/`development` defaults. It
  reads only this explicit allowlist and never enumerates the environment.
- `GET /metrics` returns Prometheus text for process/build identity, readiness,
  retained run/execution status, queues, durations, callback outcomes,
  dispatcher/runner Job lifecycle, artifact count/bytes, and backend
  operation errors/latency.

All responses disable caching. These endpoints intentionally omit credentials,
URLs, object keys, exception text, and configuration values. They are excluded
from the public OpenAPI contract but remain routable for probes and monitoring.

## Probe Semantics

`/readyz` runs dependency checks with a four-second limit per check. The state
and artifact checks exercise each backend's write/read/delete probe. Docker
mode requires valid local runner configuration but no dispatcher. k3s mode also
requires a fresh dispatcher lease and a successful dispatcher-published
SelfSubjectAccessReview covering:

- create/get/delete Jobs;
- create/get/delete execution Secrets;
- get/list runner Pods;
- get runner Pod logs.

The dispatcher republishes the capability result periodically and stops
dispatching while the required permissions are unavailable.

When `SCENE_WEBHOOK_ENABLED=true`, app readiness reports the durable
webhook-worker heartbeat and whether its configuration was validated, but does
not fail primary app readiness when SPM or the worker is unavailable. Runner
callbacks and canonical result polling must remain available during a
downstream outage. The dedicated webhook-worker Deployment has its own required
lease/configuration readiness probe. The app does not receive or inspect the
signing secret; only the dedicated worker does. Disabled environments report
the webhook check as healthy and not required.

## Metrics Collection

The retained-state snapshot is deliberately bounded:

- `SCENE_METRICS_RECORD_LIMIT` defaults to `10000` and is clamped to
  `100..50000`.
- SCENE reads at most the limit plus one record from each of `runs`,
  `executions`, and `baselines`. The extra record proves truncation without an
  unbounded count query.
- A truncated collection reports
  `scene_metrics_collection_truncated{collection="..."} 1`. Status, queue,
  duration, and artifact values then describe only the bounded newest-record
  window; `scene_runs`, `scene_executions`, and `scene_baselines` are a proven
  lower bound rather than an invented exact total.
- Collection is single-flight, cached for five seconds, and limited to two
  seconds. A concurrent or timed-out scrape receives the last snapshot with
  `scene_metrics_collection_stale 1`; the endpoint itself remains available.

Metric labels come only from fixed status, outcome, backend, operation, Job
reason, and build-identity allowlists. SCENE never labels metrics with project,
run, execution, ticket, customer, URL, artifact key, or exception text.

Run and execution duration histograms describe terminal records still retained
inside the bounded window. Artifact gauges sum persisted artifact metadata and
deduplicate identical storage/key/version records; they do not list S3.

Completion callback outcomes are stored in a durable aggregate record. The
dispatcher publishes its process counters and bounded Job terminal reasons into
the leader lease every ten seconds. Those counters reset when their owning
process or environment is replaced, which Prometheus handles as a normal
counter reset.

DynamoDB, S3, and Kubernetes clients publish process-local operation counters
and latency histograms. Scrape each app pod independently and sum counters in
Prometheus; scraping only the Service can alternate between replicas and is not
a reliable per-process time series. Dispatcher Kubernetes totals are also
available through the durable lease-backed dispatcher metrics.

The webhook worker publishes enabled state, queue depth, oldest pending age,
and durable attempt/success/failure counters without endpoint or event labels.
See `docs/webhooks.md` for delivery and retry semantics.

## Kubernetes Wiring

Use `/healthz` for liveness and `/readyz` for readiness. Do not use a dependency
probe for liveness, because a transient AWS or Kubernetes outage must remove the
pod from service without restarting a healthy process. Public operational
endpoint exposure remains behind the environment's ingress authentication.

The committed staging NetworkPolicy does not admit a monitoring namespace.
Inspect `/metrics` through an authenticated ingress or an operator port-forward;
add an explicit namespace and pod selector before connecting a cluster scraper.
Keep ingress BasicAuth or SCENE's eventual OIDC/service authorization in front
of every externally reachable operations endpoint. Monitoring access should use
a dedicated in-cluster NetworkPolicy rule rather than making `/metrics` public.

Set immutable build identity in the app Deployment, for example:

```text
SCENE_VERSION=0.1.0
SCENE_GIT_SHA=<full commit SHA>
SCENE_BUILD_TIME=<UTC RFC3339 timestamp>
SCENE_ENV=staging
SCENE_APP_IMAGE=<immutable app image digest>
SCENE_RUNNER_IMAGE=<immutable runner image digest>
SCENE_METRICS_RECORD_LIMIT=10000
```
