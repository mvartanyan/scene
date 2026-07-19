# SCENE Linux Docker Staging

This profile is for SCENE acceptance runs on a Linux Docker host. Mac/ARM Docker
runs remain developer evidence only; acceptance evidence should come from this
Linux staging profile.

## Host Layout

Use persistent host paths outside the git checkout:

```bash
sudo mkdir -p /var/lib/scene/staging/state /var/lib/scene/staging/artifacts
sudo chown -R "$USER":"$USER" /var/lib/scene/staging
```

The app container launches sibling runner containers through the host Docker
socket. Keep `SCENE_ARTIFACT_ROOT` identical on the host and in the app
container, because Docker interprets runner bind mounts as host paths.

## Environment

Create the staging env file:

```bash
cp .env.staging.example .env.staging
```

Required staging variables:

- `SCENE_STATE_PATH`: JSON state file, for example `/var/lib/scene/staging/state/scene.dynamodb.json`.
- `SCENE_STATE_BACKEND`: `json` for this single-host profile or `dynamodb` for
  the k3s production profile. DynamoDB also requires `AWS_REGION` and
  `SCENE_DYNAMODB_TABLE`; see `docs/storage.md`.
- `SCENE_ARTIFACT_ROOT`: persistent artifact directory, for example `/var/lib/scene/staging/artifacts`.
- `SCENE_ARTIFACT_BASE_URL`: artifact URL prefix, normally `/artifacts`.
- `SCENE_ARTIFACT_STORAGE`: storage class for this profile. Use `filesystem` for local Linux Docker staging.
- `SCENE_HOST_URL`: stable URL runner containers use for callbacks and reviewers use for artifacts.
- `SCENE_BASE_URL`: stable URL used by the in-repo MCP server when it calls SCENE.
- `SCENE_API_TOKEN`: bearer token required for agent/API/MCP mutation and control calls.
- `SCENE_RUNNER_BACKEND`: runner backend. Use `docker` for this profile.
- `SCENE_RUNNER_IMAGE`: pinned runner image, for example `scene-playwright-runner:1.47.0-jammy`.
- `SCENE_RUNNER_IMAGE_AUTOBUILD`: set to `false` for staging so missing images fail readiness instead of building inside the app.
- `SCENE_RUNNER_SHM_SIZE`: Docker shared memory size for headless browsers. Start with `1g`.
- `SCENE_RUNNER_EXTRA_HOST_GATEWAY`: set to `true` when `host.docker.internal` is used from Linux runner containers.
- `SCENE_MAX_CONCURRENT_EXECUTIONS`: staging runner concurrency. Start with `2`.
- `SCENE_RUN_TIMEOUT_SECONDS`: run timeout. Start with `900`.
- `SCENE_CAPTURE_DELAY_MS`: post-load capture delay. Start with `7000`.
- `SCENE_DIFF_PIXEL_TOLERANCE`: per-channel pixel tolerance for diff/heatmap metrics. Start with `0`.

k3s uses different settings: `SCENE_RUNNER_BACKEND=k3s`,
`SCENE_K3S_SERVICE_URL=http://scene.<namespace>.svc.cluster.local:8000`, and
`SCENE_ARTIFACT_STORAGE=s3` with a private bucket. See `docs/k3s-runner.md` and
`docs/artifacts.md`.

## Stable Protected URL

`SCENE_HOST_URL` must be reachable from runner containers. Use either:

- a DNS-backed HTTPS URL through a host reverse proxy, or
- `http://host.docker.internal:<port>` for host-local smoke checks.

For a protected public staging URL, terminate TLS/auth at a host proxy and route
to `127.0.0.1:${SCENE_HTTP_PORT}`. Do not protect runner callback endpoints in a
way that blocks containers. If using basic auth, exempt:

- `/api/orchestrator/ping`
- `/api/executions/*/complete`

Artifact viewing can remain protected by the proxy, but the smoke check must be
able to fetch `/artifacts/...` from the same URL used by reviewers.

Minimal Caddy-style shape:

```text
scene-staging.example.com {
  encode gzip
  reverse_proxy 127.0.0.1:8010
}
```

## Build And Start

Build the pinned runner image explicitly:

```bash
if docker compose version >/dev/null 2>&1; then
  COMPOSE="docker compose"
else
  COMPOSE="docker-compose"
fi
$COMPOSE --env-file .env.staging -f docker-compose.staging.yml --profile runner-image build runner-image
docker image inspect "$SCENE_RUNNER_IMAGE"
```

Start the app:

```bash
$COMPOSE --env-file .env.staging -f docker-compose.staging.yml up -d --build scene
$COMPOSE --env-file .env.staging -f docker-compose.staging.yml ps
```

For systemd-managed staging, install `deploy/staging/scene-staging.service` as
`/etc/systemd/system/scene-staging.service`, keep the checkout at `/opt/scene`,
then run:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now scene-staging.service
```

## Smoke Check

Run the smoke from the Linux staging host after the app is reachable:

```bash
source .env.staging
python scripts/staging_smoke.py \
  --base-url "$SCENE_HOST_URL" \
  --runner-image "$SCENE_RUNNER_IMAGE" \
  --artifact-root "$SCENE_ARTIFACT_ROOT"
```

The smoke verifies:

- app health via `/api/orchestrator/ping`
- runner config via `/api/orchestrator/readiness`
- pinned runner image is present
- callback reachability from a runner container
- host artifact write and read through `/artifacts/...`
- one baseline run and one comparison run
- Chromium and Firefox launch headlessly on Linux
- observed, baseline, diff, heatmap, trace, and log artifacts are readable

## Agent API And MCP

Staging should expose the agent control plane so SPM or Codex can configure and
run SCENE without using HTMX routes:

```bash
source .env.staging
curl "$SCENE_HOST_URL/api/agent/manifest"
curl -H "Authorization: Bearer $SCENE_API_TOKEN" "$SCENE_HOST_URL/api/config"
SCENE_BASE_URL="$SCENE_HOST_URL" SCENE_API_TOKEN="$SCENE_API_TOKEN" python -m scene_mcp.server
```

The app serves the agent docs at `/api/agent/docs` and OpenAPI at
`/openapi.json`. Keep page, batch, threshold, baseline, and artifact
configuration in SCENE; SPM should reference SCENE batches and consume
`/api/runs/{run_id}/result`.

The script prints JSON with the staging URL, project/batch IDs, run IDs,
baseline ID, execution count, diff percentages, and artifact readback URL.

## Representative Acceptance Batches

After the smoke passes, run at least one representative project batch before
accepting runner or orchestration changes:

1. A small static page batch with Chromium and Firefox at `800x600`.
2. A real SCENE customer-style page with preparatory actions such as
   `disable_animations`, cookie-banner dismissal, or `wait_for_selector`.
3. A comparison run against the latest completed baseline, with artifact viewer
   links checked from the protected staging URL.

Record acceptance evidence as:

- staging URL
- batch ID and run IDs
- baseline ID
- execution count by browser
- diff average and maximum
- threshold result
- observed/baseline/diff/heatmap/log artifact links

## Collaboration Workflow

- Mac/ARM developer evidence can show local reproduction, syntax checks, unit
  tests, and Docker build confidence.
- Linux staging acceptance evidence must come from this staging profile.
- Do not claim acceptance from Mac/ARM-only Playwright runs.
- Do not store staging state, artifacts, traces, videos, or screenshots in the
  tracked repo.
- Do not switch this Docker staging profile to k3s by relying on
  `host.docker.internal`; k3s callbacks must use cluster DNS and the runner
  readiness job in `deploy/k3s/runner-readiness-job.yaml`.

## Cleanup And Retention

Keep `/var/lib/scene/staging/state` backed up with the same cadence as other
acceptance state. Keep `/var/lib/scene/staging/artifacts` large enough for
review windows, then prune old run artifacts after evidence has been recorded.

Suggested retention:

- keep the last 14 days of smoke artifacts
- keep accepted ticket artifacts until the ticket is released or superseded
- preserve baselines for active representative projects
- prune abandoned runner containers with `docker ps -a --filter name=scene-run-`

Stop staging without deleting persistent state:

```bash
docker compose --env-file .env.staging -f docker-compose.staging.yml down
```

Delete persistent staging data only after an explicit backup or approval:

```bash
sudo rm -rf /var/lib/scene/staging/state /var/lib/scene/staging/artifacts
```
