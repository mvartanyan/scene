# Scene Walkthrough

## Overview
Scene is a FastAPI + HTMX application that manages visual regression "projects", orchestrates Playwright-based screenshot runs inside Docker, and records artifacts (screenshots, diffs, traces, logs) to the local filesystem under `.scene/artifacts/` by default.

The UI is organised into three primary areas:

- **Projects** — define Pages (URL + credentials), Tasks (browser/viewport combos + task JS), and Batches (collections of tasks).
- **Runs** — launch baseline or comparison runs, monitor execution progress, and drill into artifacts via an in-app viewer.
- **Configuration** — manage available browsers, viewport presets, timestamp display, and the default run timeout.
- **Agent API/MCP** — expose the same configuration and run-control workflows through JSON endpoints and a thin MCP server for unattended agent use.

## Environment
- Python 3.12 (virtualenv provided at `venv/`).
- `Dockerfile.playwright` produces the `scene-playwright-runner:latest` image used for orchestration.
- HTMX is vendored locally at `app/static/htmx.min.js` to avoid CDN hiccups.
- Pillow is bundled into the runner image so reference screenshots can be resized to match observed dimensions before slider display.
- Mutable runtime state defaults to `.scene/dev.dynamodb.json`; set `SCENE_STATE_PATH` to point at a different JSON state file. Production selects DynamoDB with `SCENE_STATE_BACKEND=dynamodb`, `AWS_REGION`, and `SCENE_DYNAMODB_TABLE`.
- Mutable artifacts default to `.scene/artifacts/`; set `SCENE_ARTIFACT_ROOT` to
  point at another local directory. Production uses
  `SCENE_ARTIFACT_STORAGE=s3` and a private `SCENE_S3_BUCKET`; app pods retain
  only a bounded temporary workspace while runner pods upload through scoped
  presigned URLs without AWS credentials.
- `dev.dynamodb.json`, when present in an established workspace, is an ignored local data snapshot. Reuse it explicitly with `SCENE_STATE_PATH=dev.dynamodb.json`; it is not supplied by Git.
- Local runtime roots, Playwright reports, traces, videos, screenshots, and temp DBs are ignored. Clean disposable local state with `rm -rf .scene frontend/playwright-report frontend/test-results` when retention is no longer useful.

## Typical Flow
1. **Define a project**: add pages (optionally with preparatory JS, basic-auth), attach tasks, bundle them into batches.
   - Prefer the **Preparatory Actions (JSON)** field to declaratively describe cookie-dismissals, animation suppression, or other Playwright steps before the page stabilises.
   - Custom `preparatory_js` remains available and runs after the action list.
2. **Launch a run**: choose project/batch, set purpose (baseline/comparison), then choose all tasks, the first task as a smoke scope, or explicit tasks. The form shows the resulting execution count and warns when it exceeds 100. The baseline dropdown only lists completed baselines and refreshes in-place when you change project/batch or focus the field, so stale/failed recordings never appear.
3. **Observe progress**: the run log polls every 5s. Run history is paginated at 25 rows and each execution overlay at 50 rows, keeping periodic responses bounded. Selecting an entry opens the modal, displaying execution status and artifact links; direct execution links continue to work across pages.
4. **Inspect artifacts**: each execution exposes Observed/Baseline/Diff/Heatmap downloads and in-app viewers; trace/log/video are available when captured.
   - The execution viewer now keeps metadata pinned, with Observed/Baseline/Reference/Diff/Heatmap/Slider modes. Reference mode uses the same image-compare slider as Baseline so the layouts stay identical.
5. **Reconcile configuration**: update browsers/viewports or the default timeout in the Config modal as requirements evolve.

## Files & Directories
- `app/services/orchestrator.py` — queue-backed orchestrator, Docker runner integration, diffing, and runtime loading of the Playwright runner script.
- `app/services/runner_script.py` — the executable Playwright runner injected into containers (auto-scrolls the detected scrollable element and waits for lazy content).
- `app/services/storage.py` — JSON-backed local persistence, including config defaults, run timeout, and transactional agent setup writes.
- `app/services/dynamodb_storage.py` — production single-table DynamoDB adapter
- `app/services/s3_artifacts.py` — private S3 persistence, deterministic object
  keys, checksums, presigned transfer manifests, and explicit-key deletion.
  with conditional versions, GSIs, and continuation cursors.
- `app/services/config_transfer.py` and `scripts/scene_config.py` — validated,
  idempotent config-only export/import without run or artifact history.
- `app/services/run_scope.py` — task-subset validation and execution-count helpers shared by UI/API launch paths.
- `app/pagination.py` — bounded pagination used by large project and run views.
- `app/templates/runs/*.html` — HTMX partials for the run log, dashboards, viewers, detail modal.
- `.scene/artifacts/` — default local screenshots, traces, videos, logs.
- `dev.dynamodb.json` — optional ignored workspace snapshot, not the mutable default runtime database.
- `tests/` — unit/integration coverage for CRUD, orchestrator behaviour, and dashboard rendering.
- `docs/agent-api.md` — agent-readable REST/MCP contract, served at `/api/agent/docs`.
- `docs/storage.md` — state backend, table key, bounded-read, and migration contract.
- `docs/artifacts.md` — filesystem/S3 artifact contract and runner transfer protocol.
- `scene_mcp/` — MCP server wrapper that forwards tools to SCENE REST APIs.

Refer to `DEVELOPMENT.md` for chronological implementation notes, outstanding issues, and next steps.

## Current Caveats
- Playwright runs are robust but can still fail sporadically with renderer crashes/timeouts; re-running usually succeeds, and further hardening is planned.
- Concurrency above two simultaneous executions still causes resource contention (CPU/memory/disk) and target-site throttling. Keep the cap at two until the host is upsized or a job queue is introduced.
- The run detail modal refreshes only while open; occasional flicker remains while polling. Debounce or SSE-based updates are possible future improvements.
- Project page/task tabs are lazy and paginated, but batch membership editors intentionally render the full task set inside a bounded scroll area so operators can review and edit membership in one form.
- Auto-scroll relies on detecting the active scroll container; pages that inject bespoke scroll hosts after load may still need bespoke preparatory actions.
