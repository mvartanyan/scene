# Development Notes

## Repository Snapshot
- Current structure includes FastAPI application scaffolding under `app/` (routes, schemas, storage service, templates, static assets) plus `pyproject.toml`, `DEVELOPMENT.md`, and `REQUIREMENTS.md`.
- `REQUIREMENTS.md` remains the authoritative product brief outlining the FastAPI + htmx stack orchestrating Playwright runners in Docker.
- `DEVELOPMENT.md` inaugurated this session; no `WALKTHROUGH.md` exists yet.

## Key Implementation Guidance (from REQUIREMENTS.md)
- Core services to build: FastAPI app (REST + htmx partials), worker service managing a queue, Dockerised Playwright runner, artifact storage abstraction, diff/heatmap generator.
- Primary entities: Projects, Pages, Tasks, Batches, Baselines, Test Runs, Task Executions, Artifacts, Webhooks, Audit Logs.
- Required capabilities called out as missing additions: orchestration layer, storage abstraction, diff/heatmap generation, Playwright trace/video capture, and realtime UI updates via htmx polling.
- UX expectations: Backstop-style viewer with slider/onion-skin/heatmap modes, htmx-driven CRUD flows, stable artifact URLs, and webhook notifications for lifecycle events.
- Testing expectations span unit, integration (Docker runner), and E2E smoke coverage; acceptance criteria emphasise CRUD correctness, artifact handling, stopping semantics, and webhook reliability.

## Workflow Commitment
- This file will be updated at every step with new observations, user directives, and decisions so it remains the authoritative development log.
- Pending user confirmation, the next actionable work should be chosen from the numbered list provided separately in the assistant response.
- At the end of each assistant response, run `say "project scene: command complete"` to acknowledge completion. This audible cue is mandatory for every handoff.

## Orchestration Implementation Plan (2024-08-17)
- Received directive to replace mocked execution data with a fully functioning orchestration layer that launches Playwright inside Docker, records baselines, compares subsequent runs, and surfaces screenshots/diffs/heatmaps/traces in the UI.
- Architectural outline:
  - Extend `SceneRepository` to persist Baselines, Task Executions, and Artifact metadata alongside existing entities; introduce deterministic artifact paths under a new `artifacts/` directory.
  - Add an `ArtifactStore` helper responsible for creating per-run/per-execution folders, writing binary assets, and returning stable URLs routed through FastAPI.
  - Implement a `RunOrchestrator` service with an internal queue and worker thread that expands runs into execution matrices, launches a `DockerPlaywrightRunner`, and updates execution/run/baseline status as containers complete.
  - Provide a container runner abstraction that shells out to `docker run mcr.microsoft.com/playwright/python`, mounting a generated `runner.py` + `config.json`; the script will load page/task configuration, execute preparatory/task JavaScript within the page context, capture screenshots, record trace/video, and emit a structured JSON summary.
  - Generate diffs and heatmaps with Pillow inside the app process (documented deviation from the odiff recommendation for local simplicity) and compute basic pixel/percentage statistics.
  - Update FastAPI APIs + htmx templates so run dashboards poll real execution status, expose per-execution viewers (side-by-side, slider, heatmap overlay), and link to stored artifacts.
- Test strategy:
  - Add integration tests that spin up a tiny HTTP server, run a baseline-recording batch, mutate the served HTML, then run a comparison batch to assert screenshots/diffs/heatmaps exist and metadata reflects the delta.
  - Keep existing CRUD unit tests green; reuse dependency overrides so orchestration can target temporary storage/artifact roots during tests.
- Working assumptions (to revisit with the user if they conflict with future guidance):
  1. Playwright work executes inside a custom image tagged `scene-playwright-runner:latest`, built from `Dockerfile.playwright` (base `mcr.microsoft.com/playwright/python:v1.47.0-jammy`) which pre-installs the Python Playwright package and browsers and still runs containers with `--add-host=host.docker.internal:host-gateway` for host connectivity.
  2. Both `preparatory_js` and `task_js` snippets are executed within the loaded page via `page.evaluate` (browser context). Support for raw Playwright `page.*` API scripts would require a separate execution pipeline.
  3. Artifact diffs/heatmaps are produced with Pillow; swapping to `odiff` or ImageMagick is deferred until we need closer parity with production expectations.
  4. Artifacts are stored under `artifacts/` in the project workspace and served read-only via new FastAPI endpoints; caller environments tolerate local disk growth.

## Session Progress
- Added `pyproject.toml` with FastAPI, Jinja2, boto3 (for eventual DynamoDB parity), and test tooling dependencies.
- Implemented `LocalDynamoStorage` (`app/services/storage.py`) persisting JSON to `dev.dynamodb.json`, plus `SceneRepository` helpers covering Projects, Pages, Tasks, Batches, and Runs.
- Exposed matching JSON APIs under `/api/...` (`app/routes/api.py`) using new Pydantic schemas in `app/schemas.py`.
- Built htmx-driven configuration UI (`app/routes/projects.py`, templates under `app/templates/projects/`) supporting project selection, create/delete, and inline creation/removal of pages, tasks, and batches.
- Created mocked runs dashboard (`app/routes/runs.py`, `app/templates/runs/`) that lists stored runs, supports generating placeholder runs, and renders execution tiles with stubbed artifacts to allow UI iteration ahead of worker/runner integration.
- Added pytest coverage for CRUD endpoints (`tests/test_api_crud.py`) using dependency overrides to isolate each test's LocalDynamoStorage snapshot. Installed project dependencies into `venv/`, appended `python-multipart` to support FastAPI form handlers, configured pytest's `pythonpath`, and the suite now passes (`pytest`).
- Introduced a prominent JIRA issue field for batches and runs (defaulting runs from their batch) across storage, APIs, templates, and mock data. Runs dashboard now includes filters, status/JIRA summaries, and richer cards for evaluating UI potential, with tests covering the new inheritance/update behaviour.
- Replaced custom styling with Bootstrap 5 components (`app/templates/base.html`, templates under `app/templates/projects/`, `app/templates/runs/`, and new `app/templates/config/modal.html`) so project selection uses tabs, modals support edit flows, and layout stays consistent without bespoke CSS. Configuration modal now edits browser/viewport availability with persistence and usage guards.
- Extended persistence/json schema to include baselines, per-execution records, artifact metadata, and summaries; added an `ArtifactStore` helper for deterministic run/baseline directory layouts.
- Implemented a queue-backed `RunOrchestrator` that expands batches into Dockerised Playwright executions, captures screenshots/traces/video, generates diff/heatmap PNGs with Pillow, and finalises baseline records.
- Updated API + htmx UI to consume real execution data, surface artifact links, and render an in-app viewer (slider, diff, heatmap) for each execution; added `/artifacts/*` route for serving stored files.
- Added `Dockerfile.playwright` and switched orchestration to the local `scene-playwright-runner:latest` image to avoid per-run dependency installs; integration tests build the image on demand.
- Authored `tests/test_orchestrator_integration.py` to drive a baseline + comparison run against a lightweight HTTP fixture via real Docker containers, alongside existing CRUD smoke tests.
- Added `tests/test_runs_dashboard.py` to ensure the htmx dashboard renders even without seeded executions, and introduced a container-driven Playwright UI harness under `frontend/` (config + specs) that exercises key project/run views via real browsers.
- Reworked the runs dashboard to prioritise the launcher, followed by filters and a live-updating run log; auto-refresh keeps the modal detail current. Layout/polling polish (ensuring the selected row stays highlighted without pausing refresh) remains TODO.
- Implemented cancellation semantics for both runs and executions, wiring the orchestrator to track active containers, issue `docker kill`, and persist the `cancelled` status alongside refreshed summaries that now count queued/executing/finished/failed/cancelled executions. Long-polling log streams now surface queued/start/command/failure/success events.
- Added a timestamp display preference (UTC vs local browser time) to the configuration modal and centralised rendering through a `format_ts` template filter so all UI timestamps respect the setting.
- Normalised Docker volume mounts to use absolute artifact paths (fixing invalid-name errors) and removed stale runs via the API to keep the live dashboard clean.
- Added optional basic-auth credentials to pages (schema + runner) and exposed them through the unified form experience.
- Resolved run dashboard regressions so selection loads the modal detail reliably, deletions update instantly without a full refresh, and delete controls cascade artifacts while keeping consistent styling; centered project toasts and added Playwright coverage to assert the overlay renders.
- Centralised the run dashboard JavaScript initialisation in `base.html` so htmx swaps reapply listeners, covering selection highlighting, modal refresh, and empty-state resets without relying on inline scripts.
- Vendor-tracked `htmx.min.js` locally to eliminate CDN parsing errors while keeping the runs dashboard behaviour consistent across browsers.
- Hardened the orchestration pipeline so “None” placeholders never execute in Playwright: `_clean_js` now normalises page/task scripts, the container runner strips bogus values, and subsequent runs complete instead of getting stuck after launch.
- Removed the `active` class highlight from the project list to avoid the confusing colour treatment until a revised design lands.
- Confirmed the JSON-backed storage remains sufficient for current scope and added explicit artifact cleanup when deleting runs/baselines so disk state stays in sync with repository records.
- Polished the runs dashboard selection UX: the highlighted row survives periodic refreshes, status/delete controls align vertically, and cascading deletions now remove related baselines and artifacts.
- Consolidated project configuration forms so pages/tasks/batches share a single add/edit surface with cancel affordances and Bootstrap toasts; project-level edit/delete controls now live in the sidebar with a modal editor.
- Added `tests/test_orchestrator_unit.py` to assert Basic Auth credentials flow into the Docker runner config, closing the loop on the new page fields.
- Instrumented `_execute_single`/`_process_run` to trace success hand-offs into `_finalize_execution_success`, including container launch metadata and timeout-aware logging so we can confirm reconciliation flow in production logs.
- Added an orchestrator `reconcile` sweep invoked by `/runs/log` polling to finalise executions with successful `runner.log` output and to auto-cancel any lingering executions once the run deadline passes; run records now track `timeout_reason`/`timeout_notified` to drive UI feedback.
- When a run exceeds its timeout, mark it cancelled automatically, persist a descriptive `timeout_reason`, and surface an amber toast on the runs dashboard so operators know the system stopped the run without manual intervention.
- Cancellation pathways now stamp a human-readable reason into each execution log/message so timeout-driven stops are clearly identified without resembling manual operator actions.
- Runner containers now emit a `result.json` artifact alongside stdout, and the orchestrator falls back to that or on-disk artifact checks when logs are truncated; reconciliation can promote those executions to `finished` rather than letting runs stall until timeout.
- Parallel execution is now configurable via `max_concurrent_executions` (default 4). The orchestrator uses a thread pool per run, honouring the ceiling while queuing additional tasks, so multi-task batches can progress simultaneously instead of serialising container launches.
- Run detail modal now scrolls within the viewport, making large multi-task batches easier to monitor without the footer jumping off-screen.
- Added a configurable `scene_host_url` so we can direct Playwright containers back to whatever host serves the orchestrator (defaults to `http://host.docker.internal:8000` for local dev) in preparation for callback-driven completion handling.
- Replaced the synchronous `subprocess.run` orchestration with a Docker-backed callback model: we now start containers via a Docker backend (SDK when available, CLI fallback when offline), stream logs continuously, and allow containers to POST their results back; completions drive run progression through an internal queue.
- Restored the run detail modal `id` so HTMX swaps target the correct node, resolving the `insertBefore` TypeError during live refreshes.
- Integration tests now spin up a lightweight callback HTTP server that forwards container POSTs into the orchestrator, ensuring the new async pipeline (containers + callbacks) is exercised end-to-end.

## Test Evidence (2024-08-17)
- `venv/bin/pytest -m 'not integration'`
- `venv/bin/pytest -m integration`
- `docker run --rm -v $(pwd):/workspace -w /workspace/frontend --add-host=host.docker.internal:host-gateway mcr.microsoft.com/playwright:v1.47.0-jammy npx playwright test`

## Open Questions / Pending Focus
- Monitor JSON-backed storage performance; add lightweight indexing only if repository scans become a bottleneck.
- Storage layer still lacks webhook definitions and artifact lifecycle policies; define the order for these schema extensions.
- Wire toast feedback for project creation/deletion to match the new edit flow and ensure accessibility pass on the refreshed forms.
- Ensure the Playwright run overlay test executes against a predictable dataset or seed helper to avoid relying on fixture skips.

## Outstanding Issues Requiring Follow-Up
- **Run completion detection remains unreliable**
  - **Problem**: Certain runs (notably UNFCCC baseline launches) remain in the `executing` state even though the Playwright container exits successfully and artifacts (`observed.png`, `trace.zip`, `runner.log`) are present. The run-level summary never flips to `finished`, preventing artifact access via the UI.
  - **What we tried**:
    - Normalised `preparatory_js`/`task_js` values (`_clean_js`) to avoid `"None"` strings executing.
    - Introduced configurable `run_timeout_seconds`, per-execution deadlines, and container-level timeouts (16894fe) to catch hung runs.
    - Added local HTMX bundle and rewrote the run dashboard scripts to ensure polling/refresh work reliably.
    - Manual reconciliation confirms the data model can recover: calling `_finalize_execution_success` and `_refresh_run_summary` promotes the execution/run to `finished`, and the dashboard unblocks artifacts.
  - **User feedback**: Runs still appear to stall; retrying or launching new runs exhibits the same behaviour (“containers come up, work for ~5s, exit, but the status never changes”).
  - **Next steps**:
    1. Instrument `_execute_single` and `_process_run` for success path logging to verify the code path reaches `_finalize_execution_success`.
    2. Add a reconciliation sweep after each polling cycle (or on `/runs/log`) that inspects `runner.log` for `"status": "ok"` and updates any executions still marked `executing`.
    3. Expose a visible warning in the UI when a run exceeds its timeout so users know they can cancel/retry.
    4. Provide a script/CLI helper for manual reconciliation until the automated fix lands.
5. Manual workaround (for now):
   ```bash
   venv/bin/python - <<'PY'
   from app.services.storage import get_repository
   from app.services.orchestrator import get_orchestrator, RunnerResult
       from pathlib import Path

       run_id = "<stuck-run-id>"
       repo = get_repository()
       orch = get_orchestrator()
       run = repo.get_run(run_id)
       baseline = orch._resolve_baseline_record(run)

       for execution in repo.list_executions(run_id=run_id):
           if execution["status"] != "executing":
               continue
           execution_dir = orch._artifacts.execution_dir(run_id, execution["id"])
           log_path = orch._log_path(run_id, execution["id"])
           if log_path.exists() and '"status": "ok"' in log_path.read_text(encoding="utf-8"):
               result = RunnerResult(
                   success=True,
                   screenshot=execution_dir / "observed.png",
                   trace=execution_dir / "trace.zip",
                   video=orch._find_video(execution_dir),
                   log=log_path,
               )
               orch._finalize_execution_success(
                   run=run,
                   execution=execution,
                   baseline=baseline,
                   execution_dir=execution_dir,
                   result=result,
       )
   orch._refresh_run_summary(run_id, force_status=None)
   PY
   ```

## 2025-10-28 – Diff thresholds and viewer overhaul
- Added batch-level `run_diff_threshold` and `execution_diff_threshold` to drive “no/minor/major diff” badges raised in the run dashboard and execution viewer.
- Orchestrator now persists per-execution `diff_level`, aggregates run-level averages/maxima, and hardens log output with structured runner progress messages.
- UI refresh: run overlay shows diff badges/levels per execution, disables retry/kill once runs finish, and removes direct artifact download buttons in favour of the updated viewer.
- Viewer gains top-aligned controls, full-width slider/heatmap modes, and exposes diff metrics alongside download shortcuts.
- Run list highlights the current diff badge so high-variance runs surface immediately.

## 2025-10-31 – Runner hardening & modal stability
- Playwright containers now receive host DNS mappings and launch Chromium with `--disable-dev-shm-usage --no-sandbox`, plus a fresh-context retry for screenshot crashes.
- `/runs/launch` accepts baseline-id prefixes, resolving them to a unique stored baseline or returning an error if ambiguous.
- Run overlay polling is paused when hidden, locks onto the user-selected run, and trims noisy error logs to their first line, eliminating the previous modal “flicker”.

## 2025-10-31 – Dashboard guardrails & launcher UX
- Instrumented the Playwright runner to emit navigation status/URL metadata and stack traces for screenshot retry failures, with orchestrator logging the reconciled error payload for failed executions.
- Run overlay and log modals now exchange SHA1 signatures via `X-Scene-Run-Hash` headers to skip redundant swaps, ignore payloads for other runs, and keep the overlay visible while the execution log is open.
- Execution viewer enforces single-mode rendering (Observed/Baseline/Diff/Heatmap/Slider), defaults to the slider when both baseline and observed assets exist, and keeps the toolbar button states/ARIA flags in sync.
- Launch form hydrates projects/batches client-side (with server-rendered fallbacks), filters batches by project, and surfaces stored baselines through a dropdown that disables during baseline-recording runs while preserving a manual custom-ID option.
- Dashboard context now ships baseline metadata and the previous launch defaults so the form retains caller inputs after submissions, with template-side fallbacks guarding empty workspaces.
- Diff badges swap to Bootstrap’s amber palette for better contrast, the run detail modal no longer auto-switches to the most recent run when polling updates arrive, and slider/diff rendering gained pointer-driven controls plus higher-contrast highlights.
- Viewer now bundles the Image Compare viewer library for the slider experience, keeps execution metadata in a sticky header, captures per-execution reference screenshots whenever a page defines `reference_url`, and exposes baseline/reference sliders in dedicated modes.
- Run selection is persisted via `data-scene-selected-run`, so htmx refreshes keep the user’s chosen execution in focus, preparatory/task actions can now be configured declaratively alongside custom JS with structured logging, cookie-banner/animation suppression lives in configuration instead of bespoke snippets, and the default post-capture wait is configurable (currently 7 s) to absorb late UI transitions.

## Session Updates
- ESC from the execution viewer now reopens the originating run modal, while ESC from the run modal returns the dashboard to `/runs`.
- The Playwright runner auto-scrolls before captures so narrow/mobile viewports produce full-height screenshots instead of truncating long pages.
- Missing `scene-playwright-runner:latest` images now trigger an automatic `Dockerfile.playwright` build before launching containers, with run logs capturing the build status.
- Execution cancellation logging now distinguishes between run-level and per-execution cancels to simplify diagnosis when orchestration aborts early.
- Docker CLI launches no longer pass `--rm`, keeping containers around long enough for log streaming before the orchestrator removes them explicitly.
- Fixed the embedded runner script indentation so containerized executions reach the navigation phase without `IndentationError`.
- Corrected the reference-capture block indentation inside the runner so follow-up Playwright steps execute.
- Hardened the runner retry loop so the success `break` now executes via an `else` clause, eliminating syntax errors and preserving retry semantics.
- Restored the capture attempt block to live inside the retry loop, preventing invalid `except` placement and allowing retries to run as intended.

## Pending Issues for Next Session

### Process hygiene
- **Requirement:** At the end of every assistant handoff, run `say "project scene: command complete"`. This is now documented here, in `README.md`, and has to be followed strictly next session.


## Local Usage Notes
- Activate the venv and launch the FastAPI dev server with `source venv/bin/activate` followed by `uvicorn app.main:app --reload`.
- Run tests via `source venv/bin/activate && pytest`.
