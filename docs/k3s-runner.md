# SCENE k3s Runner Readiness

SCENE now has an explicit runner backend setting:

- `SCENE_RUNNER_BACKEND=docker` keeps the current local/Linux Docker behavior.
- `SCENE_RUNNER_BACKEND=k3s` disables direct Docker launches from the FastAPI
  process and requires a cluster runner/job launcher path.
- `SCENE_RUNNER_BACKEND=worker` is reserved for a dedicated worker service path.

The k3s mode is deliberately fail-closed until the runner job/worker is
deployed. It prevents the web pod from silently depending on a Docker socket.

## Required k3s Settings

Use a prebuilt, pinned Playwright image:

```bash
export SCENE_RUNNER_IMAGE=registry.example.com/scene-playwright-runner:1.47.0-jammy-20260708
export SCENE_RUNNER_IMAGE_AUTOBUILD=false
export SCENE_K3S_IMAGE_PULL_POLICY=IfNotPresent
```

Use cluster DNS for callbacks:

```bash
export SCENE_RUNNER_BACKEND=k3s
export SCENE_K3S_NAMESPACE=scene
export SCENE_K3S_SERVICE_URL=http://scene.scene.svc.cluster.local:8000
```

Do not use `host.docker.internal` in k3s. The runner pod should call
`$SCENE_K3S_SERVICE_URL/api/executions/{execution_id}/complete`.

Use shared artifacts:

```bash
export SCENE_ARTIFACT_STORAGE=pvc
export SCENE_ARTIFACT_PVC_CLAIM=scene-artifacts
export SCENE_ARTIFACT_ROOT=/artifacts
```

The current artifact store supports filesystem/PVC semantics. Object storage is
called out in readiness checks as not implemented yet, so multi-pod acceptance
must use a PVC until an object-backed `ArtifactStore` exists.

## Browser Runtime Defaults

The runner image is based on `mcr.microsoft.com/playwright/python:v1.47.0-jammy`
and installs Playwright `1.47.0` with browser dependencies. SCENE tasks may
request Chromium, Firefox, or WebKit, but k3s acceptance should start with
Chromium and Firefox until WebKit is validated on the target nodes.

Start with:

- `SCENE_MAX_CONCURRENT_EXECUTIONS=2`
- `SCENE_RUN_TIMEOUT_SECONDS=900`
- `SCENE_RUNNER_SHM_SIZE=1g` for Docker staging
- k3s runner resource requests: `500m` CPU and `512Mi` memory
- k3s runner limits: `2` CPU and `2Gi` memory

Raise concurrency only after representative batches finish without renderer
crashes or timeouts.

## Runner-Side Readiness

Build or pull the runner image before deployment. Production/staging app code
does not build the runner image on demand.

The runner image includes `scene-runner-readiness`. Run it from the same pod
shape that will execute Playwright:

```bash
scene-runner-readiness \
  --callback-url "$SCENE_K3S_SERVICE_URL" \
  --artifact-dir "$SCENE_ARTIFACT_ROOT" \
  --expected-storage pvc \
  --json
```

This proves that the runner can reach `/api/orchestrator/ping` and write/read
the artifact mount. The example job in `deploy/k3s/runner-readiness-job.yaml`
shows the expected pod wiring.
