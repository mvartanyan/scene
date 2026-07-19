# SCENE Agent API

SCENE exposes a JSON control plane for agents and a thin MCP server that wraps
the same REST endpoints. The UI remains available for manual configuration, but
agents should use the API contract below instead of HTML or HTMX routes.

## Discovery

- Manifest: `GET /api/agent/manifest`
- Markdown docs: `GET /api/agent/docs`
- OpenAPI: `GET /openapi.json`
- Readiness: `GET /api/orchestrator/readiness`

The manifest includes the current auth mode, OpenAPI URL, MCP command, and
supported tool names.

## Authentication

Set `SCENE_API_TOKEN` on the SCENE app to require bearer-token auth for mutation
and control endpoints. Read-only endpoints remain available so agents can
discover projects, batches, and run results.

```bash
export SCENE_API_TOKEN="replace-with-staging-token"
curl -H "Authorization: Bearer $SCENE_API_TOKEN" \
  http://127.0.0.1:8000/api/config
```

Protected endpoints return `401` when the bearer token is missing and `403` when
it is invalid.

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

The response returns IDs and `created`/`updated` actions.

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
    "spm_ticket": "SCENE-123"
  }'
```

Launch a comparison run:

```bash
curl -X POST http://127.0.0.1:8000/api/batches/<batch-id>/comparison-runs \
  -H "Authorization: Bearer $SCENE_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"requested_by": "spm", "spm_ticket": "SCENE-123", "note": "ticket success criteria"}'
```

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
the REST API using `SCENE_BASE_URL` and `SCENE_API_TOKEN`.
