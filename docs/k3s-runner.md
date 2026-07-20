# SCENE k3s Runner Readiness

SCENE now has an explicit runner backend setting:

- `SCENE_RUNNER_BACKEND=docker` keeps the current local/Linux Docker behavior.
- `SCENE_RUNNER_BACKEND=k3s` makes the FastAPI process persist run intent and
  lets `python -m app.services.dispatcher` create and reconcile Kubernetes Jobs.
- `SCENE_RUNNER_BACKEND=worker` is reserved for a dedicated worker service path.

The web process never launches Jobs and does not start its Docker worker or
watchdog in k3s mode. Before returning from run creation it writes the complete
execution matrix to durable state. The dispatcher is the only Job controller.

## Durable Dispatch Contract

The dispatcher uses a DynamoDB-backed leader lease and conditional execution
claims. Each claim has a monotonic generation and deterministic Job/Secret
names, so a restarted dispatcher adopts a matching Job instead of creating a
duplicate. The runner specification digest must match before adoption.

Each execution uses an immutable Secret containing its generated config and a
single callback token. The token itself is never stored in DynamoDB. SCENE
stores its SHA-256 digest, expiry, run/execution identity, and dispatch
generation. Callback acceptance is persisted before finalization. A leased,
owner-fenced finalizer makes artifact verification and terminalization
restart-safe; another dispatcher may take over only after the lease expires.
Identical callback replays are accepted, while a different payload for the same
generation returns a conflict. If a successful Job's callback is lost, the
dispatcher can accept and finalize the result from its deterministic S3 object.

Cancellation is durable intent. The dispatcher deletes the owned Job and
Secret before terminalizing the execution. Deleting an active k3s run returns
HTTP 409 after requesting cancellation; retry deletion after dispatcher cleanup
has made the run terminal. A terminal execution that still owns either resource
also blocks retry and deletion until cleanup completes. Run deadlines apply to
queued and active work.

The dispatcher classifies image-pull, scheduling, deadline, OOM, eviction,
missing-Job, and non-zero runner failures and stores bounded pod logs when they
are available. Terminal-resource cleanup is replayable after a restart.

## Required k3s Settings

Use a prebuilt Playwright image pinned by digest:

```bash
export SCENE_RUNNER_IMAGE=registry.example.com/scene-playwright-runner@sha256:<digest>
export SCENE_RUNNER_IMAGE_AUTOBUILD=false
export SCENE_K3S_IMAGE_PULL_POLICY=IfNotPresent
```

Use cluster DNS for callbacks:

```bash
export SCENE_RUNNER_BACKEND=k3s
export SCENE_K3S_NAMESPACE=scene
export SCENE_K3S_SERVICE_URL=http://scene.scene.svc.cluster.local
```

Do not use `host.docker.internal` in k3s. The runner pod should call
`$SCENE_K3S_SERVICE_URL/api/executions/{execution_id}/complete`.

Use private S3 artifacts:

```bash
export SCENE_ARTIFACT_STORAGE=s3
export SCENE_S3_BUCKET=scene-staging-artifacts-<account-id>
export SCENE_S3_PREFIX=scene
export SCENE_ARTIFACT_TEMP_ROOT=/tmp/scene-artifacts
```

Only the SCENE app principal has S3 permissions. Runner Jobs receive scoped,
short-lived PUT/GET URLs in their execution config and must not receive AWS
credentials or a shared artifact volume.

The dispatcher ServiceAccount needs namespaced create/get/delete access for
Jobs and immutable execution Secrets, plus get/list/log access for runner Pods.
The runner ServiceAccount has no RBAC and its API token is not mounted. Neither
runner Jobs nor their Secrets contain AWS credentials.

## Browser Runtime Defaults

The runner image is based on `mcr.microsoft.com/playwright/python:v1.47.0-jammy`
and installs Playwright `1.47.0` with browser dependencies. SCENE tasks may
request Chromium, Firefox, or WebKit, but k3s acceptance should start with
Chromium and Firefox until WebKit is validated on the target nodes.

Chromium ignores Playwright's `--disable-dev-shm-usage` default so it uses the
pod's 1 GiB memory-backed `/dev/shm`. The pinned image cannot establish a usable
Chromium sandbox under the restricted non-root pod profile, so Chromium remains
explicitly unsandboxed inside a per-execution pod with no AWS credentials,
Kubernetes token, writable root filesystem, or retained workspace. Runner
readiness imports the production launch helper and exercises the same Chromium
and Firefox settings; native amd64 execution is still required for acceptance.
SCENE-22 owns the supported-image upgrade and long-term sandbox/isolation
posture before customer-controlled targets are accepted.

Start with:

- `SCENE_MAX_CONCURRENT_EXECUTIONS=2`
- `SCENE_RUN_TIMEOUT_SECONDS=900`
- `SCENE_RUNNER_SHM_SIZE=1g` for Docker staging
- k3s runner resource requests: `500m` CPU and `512Mi` memory
- k3s runner limits: `2` CPU and `2Gi` memory

Raise concurrency only after representative batches finish without renderer
crashes or timeouts.

## Runner-Side Readiness

Build and push the runner image before deployment. The image includes
`/opt/scene/runner.py`; production/staging code does not build or inject the
runner implementation on demand.

The runner image includes `scene-runner-readiness`. Run it from the same pod
shape that will execute Playwright:

```bash
scene-runner-readiness \
  --callback-url "$SCENE_K3S_SERVICE_URL" \
  --expected-storage s3 \
  --json
```

This proves that the runner can reach `/api/orchestrator/ping` and write/read
its ephemeral workspace. A real execution proves the presigned S3 path. The
example job in `deploy/k3s/runner-readiness-job.yaml` shows the expected pod wiring.

## Restart Acceptance

Before accepting a release, interrupt the dispatcher after it has claimed an
execution and again while a Job is active. On restart, verify that the same
generation and Job name are adopted, only one terminal result is stored, and
the Job and Secret are removed. Also verify queued and active cancellation,
callback replay/conflict handling, and S3 result recovery after a dropped
callback.
