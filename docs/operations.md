# SCENE Operations Endpoints

SCENE exposes four process-level endpoints outside the agent API:

- `GET /healthz` returns HTTP 200 when the web process can serve requests. It
  does not initialize or probe DynamoDB, S3, the runner, or Kubernetes.
- `GET /readyz` returns HTTP 200 only when state, artifacts, runner
  configuration, and any required k3s dispatcher are ready. It returns a
  sanitized HTTP 503 report for initialization errors, failed probes, stale
  dispatcher leases, or denied Kubernetes capabilities.
- `GET /version` returns `SCENE_VERSION`, `SCENE_GIT_SHA`, `SCENE_BUILD_TIME`,
  and `SCENE_ENV`, using `unknown`/`development` defaults. It never enumerates
  the environment.
- `GET /metrics` returns Prometheus text for process uptime, overall readiness,
  bounded dependency readiness, and dispatcher lease state.

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

## Kubernetes Wiring

Use `/healthz` for liveness and `/readyz` for readiness. Do not use a dependency
probe for liveness, because a transient AWS or Kubernetes outage must remove the
pod from service without restarting a healthy process. Public operational
endpoint exposure remains behind the environment's ingress authentication.

The committed staging NetworkPolicy does not admit a monitoring namespace.
Inspect `/metrics` through an authenticated ingress or an operator port-forward;
add an explicit namespace and pod selector before connecting a cluster scraper.

Set immutable build identity in the app Deployment, for example:

```text
SCENE_VERSION=0.1.0
SCENE_GIT_SHA=<full commit SHA>
SCENE_BUILD_TIME=<UTC RFC3339 timestamp>
SCENE_ENV=staging
```
