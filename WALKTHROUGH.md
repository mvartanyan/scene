# Scene Walkthrough

## Overview
Scene is a FastAPI + HTMX application that manages visual regression “projects”, orchestrates Playwright-based screenshot runs inside Docker, and records artifacts (screenshots, diffs, traces, logs) to the local filesystem under `artifacts/`.

The UI is organised into three primary areas:

- **Projects** — define Pages (URL + credentials), Tasks (browser/viewport combos + task JS), and Batches (collections of tasks).
- **Runs** — launch baseline or comparison runs, monitor execution progress, and drill into artifacts via an in-app viewer.
- **Configuration** — manage available browsers, viewport presets, timestamp display, and the default run timeout.

## Environment
- Python 3.12 (virtualenv provided at `venv/`).
- `Dockerfile.playwright` produces the `scene-playwright-runner:latest` image used for orchestration.
- HTMX is vendored locally at `app/static/htmx.min.js` to avoid CDN hiccups.
- Pillow is bundled into the runner image so reference screenshots can be resized to match observed dimensions before slider display.

## Typical Flow
1. **Define a project**: add pages (optionally with preparatory JS, basic-auth), attach tasks, bundle them into batches.
   - Prefer the **Preparatory Actions (JSON)** field to declaratively describe cookie-dismissals, animation suppression, or other Playwright steps before the page stabilises.
   - Custom `preparatory_js` remains available and runs after the action list.
2. **Launch a run**: choose project/batch, set purpose (baseline/comparison), adjust timeout if desired, and submit.
3. **Observe progress**: the run log polls every 5s. Selecting an entry opens the modal, displaying execution status and artifact links.
4. **Inspect artifacts**: each execution exposes Observed/Baseline/Diff/Heatmap downloads and in-app viewers; trace/log/video are available when captured.
   - The execution viewer now keeps metadata pinned, with Observed/Baseline/Reference/Diff/Heatmap/Slider modes. Reference mode uses the same image-compare slider as Baseline so the layouts stay identical.
5. **Reconcile configuration**: update browsers/viewports or the default timeout in the Config modal as requirements evolve.

## Files & Directories
- `app/services/orchestrator.py` — queue-backed orchestrator, Docker runner integration, diffing.
- `app/services/storage.py` — JSON-backed persistence, including config defaults and run timeout.
- `app/templates/runs/*.html` — HTMX partials for the run log, dashboards, viewers, detail modal.
- `artifacts/` — persisted screenshots, traces, videos, logs.
- `tests/` — unit/integration coverage for CRUD, orchestrator behaviour, and dashboard rendering.

Refer to `DEVELOPMENT.md` for chronological implementation notes, outstanding issues, and next steps.

## Current Caveats
- Playwright runs are robust but can still fail sporadically with renderer crashes/timeouts; re-running usually succeeds, and further hardening is planned.
- Concurrency above two simultaneous executions still causes resource contention (CPU/memory/disk) and target-site throttling. Keep the cap at two until the host is upsized or a job queue is introduced.
- The run detail modal refreshes only while open; occasional flicker remains while polling. Debounce or SSE-based updates are possible future improvements.
