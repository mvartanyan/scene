# SCENE Agent API

SCENE exposes a JSON control plane for agents and a thin MCP server that wraps
the same REST endpoints. The UI remains available for manual configuration, but
agents should use the API contract below instead of HTML or HTMX routes.

## Discovery

- Manifest: `GET /api/agent/manifest`
- Markdown docs: `GET /api/agent/docs`
- OpenAPI: `GET /openapi.json`
- Deployment readiness: `GET /readyz`
- Legacy runner/state readiness: `GET /api/orchestrator/readiness`

The manifest includes the current auth mode, OpenAPI URL, MCP command, and
supported tool names.

## Authentication

Set `SCENE_API_TOKEN` on the SCENE app to require token auth for mutation and
control endpoints. Direct clients should use a Bearer token. The same token is
also accepted in `X-SCENE-API-Token` so a staging client can use HTTP Basic in
`Authorization` at the Traefik ingress without losing SCENE application auth.
Read-only endpoints remain unauthenticated at the application layer so agents
can discover projects, batches, and run results; the staging ingress still
requires HTTP Basic for every path, including those reads.

```bash
export SCENE_API_TOKEN="replace-with-staging-token"
curl -H "Authorization: Bearer $SCENE_API_TOKEN" \
  http://127.0.0.1:8000/api/config
```

Protected endpoints return `401` when neither token form is present and `403`
when the supplied SCENE token is invalid. Traefik removes the staging BasicAuth
header before proxying. Do not put ingress credentials in the URL or command
arguments; use an interactive password prompt for `curl`, or inject the MCP
environment from a protected secret source with shell tracing disabled.

For staging requests, keep the two authentication layers separate:

```bash
export SCENE_INGRESS_BASIC_AUTH_USERNAME="scene-reviewer"
curl --user "$SCENE_INGRESS_BASIC_AUTH_USERNAME" \
  -H "X-SCENE-API-Token: $SCENE_API_TOKEN" \
  https://scene.135.181.140.68.sslip.io/api/config
```

`curl` prompts for the ingress password. Bearer auth remains supported when
calling SCENE directly or through an ingress that does not require BasicAuth.

## Configure SCENE

Read current config:

```bash
curl http://127.0.0.1:8000/api/config
```

Patch config:

```bash
curl -X PATCH http://127.0.0.1:8000/api/config \
  -H "Authorization: Bearer $SCENE_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "browsers": ["chromium", "firefox"],
    "viewports": ["1280x720", "390x844"],
    "scene_host_url": "http://host.docker.internal:8010",
    "max_concurrent_executions": 2,
    "run_timeout_seconds": 900,
    "capture_post_wait_ms": 7000,
    "diff_pixel_tolerance": 0
  }'
```

## Idempotent Setup

Use `POST /api/agent/setup` when an agent needs to provision a complete project
graph in one call. The project is upserted by `slug`; pages, tasks, and batches
are upserted by `name` within that project. Tasks reference pages by
`page_name`; batches reference tasks by `task_names`.

```bash
curl -X POST http://127.0.0.1:8000/api/agent/setup \
  -H "Authorization: Bearer $SCENE_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "project": {"name": "Demo", "slug": "demo"},
    "pages": [
      {"name": "Home", "url": "https://example.com/"}
    ],
    "tasks": [
      {
        "name": "Home visual",
        "page_name": "Home",
        "browsers": ["chromium"],
        "viewports": [{"width": 1280, "height": 720}]
      }
    ],
    "batches": [
      {
        "name": "Smoke",
        "spm_ticket": "SCENE-123",
        "task_names": ["Home visual"],
        "run_diff_threshold": 2.0,
        "execution_diff_threshold": 3.0
      }
    ]
  }'
```

The response returns IDs and `created`/`updated` actions. Local JSON-backed
setups are committed as one transaction and roll back if any reference is
invalid, so a large graph does not leave a partially imported project.

## Run Batches

List batches:

```bash
curl http://127.0.0.1:8000/api/batches
```

List batches suitable for SPM success criteria:

```bash
curl "http://127.0.0.1:8000/api/check-candidates?project_id=<project-id>"
```

Record a baseline:

```bash
curl -X POST http://127.0.0.1:8000/api/runs \
  -H "Authorization: Bearer $SCENE_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "project_id": "<project-id>",
    "batch_id": "<batch-id>",
    "purpose": "baseline_recording",
    "requested_by": "agent",
    "spm_ticket": "SCENE-123",
    "idempotency_key": "spm-invocation-456",
    "task_ids": ["<optional-task-id>"]
  }'
```

Launch a comparison run:

```bash
curl -X POST http://127.0.0.1:8000/api/batches/<batch-id>/comparison-runs \
  -H "Authorization: Bearer $SCENE_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "requested_by": "spm",
    "spm_ticket": "SCENE-123",
    "note": "ticket success criteria",
    "idempotency_key": "spm-invocation-456",
    "task_ids": ["<optional-task-id>"]
  }'
```

`task_ids` is optional on both launch routes and on the corresponding
`scene_record_baseline` and `scene_run_batch` MCP tools. When supplied, every
ID must belong to the selected batch and at least one task is required. SCENE
stores the scope on the run and expands only those tasks, in batch order. Omit
the field to run the complete batch.

SPM and other unattended callers must send a stable `idempotency_key` for each
logical invocation. Retrying the same launch returns the same run. Reusing the
key with different launch parameters returns `409`.

For large reads, use the cursor endpoints instead of collecting an unbounded
list:

```bash
curl "http://127.0.0.1:8000/api/runs/page?project_id=<project-id>&limit=25"
curl "http://127.0.0.1:8000/api/runs/<run-id>/executions/page?limit=50&cursor=<next-cursor>"
```

The response contains `items` and `next_cursor`. Treat the cursor as opaque and
omit it when `next_cursor` is null.

Read status and results:

```bash
curl http://127.0.0.1:8000/api/runs/<run-id>/detail
curl http://127.0.0.1:8000/api/runs/<run-id>/result
curl http://127.0.0.1:8000/api/runs/<run-id>/artifacts
```

The `/result` endpoint is the preferred SPM integration response. It includes
run status, execution counts, diff average/maximum, thresholds, threshold pass
state, the associated `spm_ticket`, failure details, and top artifact/viewer
links.

## Control Runs

```bash
curl -X POST http://127.0.0.1:8000/api/runs/<run-id>/cancel \
  -H "Authorization: Bearer $SCENE_API_TOKEN"

curl -X POST http://127.0.0.1:8000/api/executions/<execution-id>/retry \
  -H "Authorization: Bearer $SCENE_API_TOKEN"

curl -X DELETE http://127.0.0.1:8000/api/runs/<run-id> \
  -H "Authorization: Bearer $SCENE_API_TOKEN"
```

Execution callbacks continue to use their per-execution callback token and are
not authenticated with `SCENE_API_TOKEN`.

## MCP Server

Install SCENE dependencies, then run:

```bash
export SCENE_BASE_URL="http://127.0.0.1:8000"
export SCENE_API_TOKEN="replace-with-staging-token"
python -m scene_mcp.server
```

For the temporary staging ingress, provide both BasicAuth variables in the MCP
process environment as secrets:

```bash
export SCENE_BASE_URL="https://scene.135.181.140.68.sslip.io"
export SCENE_API_TOKEN="replace-with-app-token"
export SCENE_INGRESS_BASIC_AUTH_USERNAME="scene-reviewer"
export SCENE_INGRESS_BASIC_AUTH_PASSWORD="replace-with-ingress-password"
python -m scene_mcp.server
```

When both ingress variables are present, the client uses HTTP Basic for the
ingress and sends `SCENE_API_TOKEN` as `X-SCENE-API-Token`. Both ingress
variables are mandatory as a pair. They are passed through `httpx.BasicAuth`,
never embedded in the request URL, tool arguments, or client error messages.
Inject real values from a mode-0600 environment file or secret manager rather
than storing them in MCP configuration committed to source control.

Available MCP tools:

- `scene_get_manifest`
- `scene_apply_setup`
- `scene_list_projects`
- `scene_list_batches`
- `scene_record_baseline`
- `scene_run_batch`
- `scene_get_run_status`
- `scene_get_run_result`
- `scene_get_artifacts`
- `scene_cancel_run`
- `scene_retry_execution`

The MCP server does not contain SCENE business logic. It forwards tool calls to
the REST API using `SCENE_BASE_URL`, `SCENE_API_TOKEN`, and the optional paired
`SCENE_INGRESS_BASIC_AUTH_USERNAME`/`SCENE_INGRESS_BASIC_AUTH_PASSWORD` values.
