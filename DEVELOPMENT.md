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

## Session Progress
- Added `pyproject.toml` with FastAPI, Jinja2, boto3 (for eventual DynamoDB parity), and test tooling dependencies.
- Implemented `LocalDynamoStorage` (`app/services/storage.py`) persisting JSON to `dev.dynamodb.json`, plus `SceneRepository` helpers covering Projects, Pages, Tasks, Batches, and Runs.
- Exposed matching JSON APIs under `/api/...` (`app/routes/api.py`) using new Pydantic schemas in `app/schemas.py`.
- Built htmx-driven configuration UI (`app/routes/projects.py`, templates under `app/templates/projects/`) supporting project selection, create/delete, and inline creation/removal of pages, tasks, and batches.
- Created mocked runs dashboard (`app/routes/runs.py`, `app/templates/runs/`) that lists stored runs, supports generating placeholder runs, and renders execution tiles with stubbed artifacts to allow UI iteration ahead of worker/runner integration.
- Added pytest coverage for CRUD endpoints (`tests/test_api_crud.py`) using dependency overrides to isolate each test's LocalDynamoStorage snapshot. Installed project dependencies into `venv/`, appended `python-multipart` to support FastAPI form handlers, configured pytest's `pythonpath`, and the suite now passes (`pytest`).
- Introduced a prominent JIRA issue field for batches and runs (defaulting runs from their batch) across storage, APIs, templates, and mock data. Runs dashboard now includes filters, status/JIRA summaries, and richer cards for evaluating UI potential, with tests covering the new inheritance/update behaviour.
- Replaced custom styling with Bootstrap 5 components (`app/templates/base.html`, templates under `app/templates/projects/`, `app/templates/runs/`, and new `app/templates/config/modal.html`) so project selection uses tabs, modals support edit flows, and layout stays consistent without bespoke CSS. Configuration modal now edits browser/viewport availability with persistence and usage guards.

## Open Questions / Pending Focus
- DynamoDB parity can stay simple—the local JSON-backed repository may remain index-free unless future scale demands otherwise.
- Mock runs expose execution placeholders only; once backend orchestration exists we must replace `_mock_executions` with real data and wire artifact URLs/viewers.
- Storage layer currently lacks baselines, artifacts, and webhook persistence; define priority order for extending the schema.

## Local Usage Notes
- Activate the venv and launch the FastAPI dev server with `source venv/bin/activate` followed by `uvicorn app.main:app --reload`.
- Run tests via `source venv/bin/activate && pytest`.
