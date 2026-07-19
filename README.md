# Scene Development Quickstart

## Prerequisites
- Python 3.12 (a virtual environment is already provided at `venv/`).

## Install Dependencies
```bash
source venv/bin/activate
pip install -e .
```
> The project uses `pyproject.toml`; alternatively run `pip install -r requirements.txt` if you export one later.

## Run the Development Server
```bash
source venv/bin/activate
uvicorn app.main:app --reload
```
- App served on `http://127.0.0.1:8000/`.
- Hot reload is enabled via `--reload`.
- Runtime state is written to `.scene/dev.dynamodb.json` and artifacts to `.scene/artifacts/` by default.
- Override those locations with `SCENE_STATE_PATH=/path/to/state.json` and `SCENE_ARTIFACT_ROOT=/path/to/artifacts`.
- Production state uses `SCENE_STATE_BACKEND=dynamodb`, `AWS_REGION`, and
  `SCENE_DYNAMODB_TABLE`. See `docs/storage.md` for the table contract,
  continuation APIs, and config-only migration workflow.
- Production artifacts use `SCENE_ARTIFACT_STORAGE=s3`, `SCENE_S3_BUCKET`, and
  optional `SCENE_S3_PREFIX`. The bucket stays private; SCENE issues short-lived
  downloads and execution-scoped runner uploads. See `docs/artifacts.md`.

## Runtime Data Policy
- `dev.dynamodb.json` is an ignored local data snapshot, not the default mutable development database. It may exist in an established workspace but is not supplied by Git.
- To reuse that local snapshot as the active database:
  ```bash
  SCENE_STATE_PATH=dev.dynamodb.json uvicorn app.main:app --reload
  ```
- Local runtime data, Playwright reports, traces, videos, screenshots, and temp DB files are ignored. They can be removed when no longer needed; use `rm -rf .scene frontend/playwright-report frontend/test-results` for a local cleanup.
- Tests should create state and artifact roots under pytest temporary directories or explicit env-configured paths, never by writing to a workspace's `dev.dynamodb.json`.

## Key Screens & Workflows
- **Projects**: Select a project then manage Pages, Tasks, and Batches via Bootstrap tabs. Tabs load independently, page/task lists use 25-item pages, and large batch task selectors scroll within a fixed-height region. Inline edit/delete is available via collapsible forms.
- **Runs**: Launch baseline/comparison runs for all tasks, a one-task smoke scope, or an explicit task selection. The launcher estimates executions and warns above 100 targets before submission. Run history uses 25-item pages and execution overlays use 50-item pages so polling remains bounded. The baseline picker refreshes via `/api/batches/{id}/baselines`, filtering out failed recordings so operators only see completed baselines.
- **Config**: Use the gear icon in the navbar to toggle browser availability, manage viewport presets, switch timestamp display, and set the default run timeout. Browsers/viewports that are in use stay locked; add new entries with the inline form.
- **Agent API/MCP**: Agents should discover capabilities via `/api/agent/manifest`, read docs from `/api/agent/docs`, and use `python -m scene_mcp.server` for MCP access. Set `SCENE_API_TOKEN` to require bearer auth for mutation/control endpoints.
- **State transfer**: `scripts/scene_config.py export` creates a private
  config-only snapshot; `import` validates by default and requires `--apply` to
  write. Runs, baselines, executions, and artifact history are never imported.

## Run Tests
```bash
source venv/bin/activate
pytest
```

## Orchestration Notes
- Docker is required; the Playwright container image is `scene-playwright-runner:latest` (built from `Dockerfile.playwright`).
- Run launches send `timeout_seconds`; the orchestrator enforces that value and will cancel lingering executions.
- Run records can carry `task_ids`; when present, the orchestrator expands only that validated subset while preserving the batch's task order. Omitting `task_ids` retains full-batch behaviour.
- REST/SPM callers should provide `idempotency_key` when launching a run. A
  retried request returns the same run; reusing the key for different launch
  parameters returns a conflict.
- Filesystem artifacts are stored under `.scene/artifacts/runs/<run>/<execution>/`
  by default. S3 artifacts use deterministic project/batch/run/execution keys,
  checksums, and private short-lived access through the same `/artifacts/...` URLs.
- Each execution waits up to 60 s for `page.goto(..., networkidle)` and then idles an additional 7 s by default (`post_wait_ms`, configurable under *Capture Stabilization*) before capturing; add preparatory actions such as `disable_animations` or `wait` to tune per-page behaviour. The same actions/JS are replayed for reference URLs prior to their capture, and reference screenshots are padded server-side to match the observed dimensions so sliders stay aligned. The runner logic now lives in `app/services/runner_script.py` (imported at runtime by the orchestrator) and auto-scrolls lazily rendered pages by driving the actual scrolling container, backing off only when `scrollHeight` stops growing.

### Preparatory Actions
- Each page can define **Preparatory Actions** (Projects → Pages → edit) as a JSON array of action objects. They run before any custom preparatory JS and before tasks start.
- Supported `type` values include:
  - `click`, `double_click`, `hover`, `focus`, `check`, `uncheck`
  - `fill`, `type`, `press`, `set_attribute`, `remove`
  - `wait_for_selector`, `wait_for_load_state`, `wait` (milliseconds)
  - `disable_animations` (injects a helper stylesheet)
  - `evaluate` (inline JavaScript with optional `arg`/`args` payload)
  - `scroll_into_view`
- Fields such as `selector`, `value`, `key`, `timeout_ms`, `delay_ms`, and `button` map directly to Playwright options. Unknown types trigger a validation error.
- Example that disables animations and dismisses a cookie banner:

```json
[
  {"type": "disable_animations"},
  {"type": "wait_for_selector", "selector": "#cn-accept-cookie", "state": "visible", "timeout_ms": 5000},
  {"type": "click", "selector": "#cn-accept-cookie"}
]
```
- Task-level actions can also be provided via the API (`task_actions`) and execute immediately after any `task_js` snippets.

### Next Session Launch Checklist
- Run the Playwright worker with at most two concurrent executions unless the host has been upsized; higher levels still show timeouts.
- Confirm any pages using cookie banners or carousels have preparatory actions in place (see example above).
- At the end of each assistant response **always** execute `say "project scene: command complete"` so the audible handoff protocol is honoured.
- When adding new reference captures, verify the slider renders both baseline and reference cleanly; the resizing logic depends on Pillow being available in the runner image.

## Known Issues
- Playwright can still fail sporadically with renderer crashes or screenshot timeouts on complex pages. See `DEVELOPMENT.md` for mitigation ideas planned for the next session.
- The run detail modal now polls only when open, but further smoothing (debounce/event-driven updates) is planned to eliminate the remaining flicker.
