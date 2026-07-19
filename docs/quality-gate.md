# SCENE Quality Gate

SCENE changes should pass the same deterministic gate locally and in CI before they are used as SPM success criteria. The gate keeps runtime state and generated artifacts out of tracked files by defaulting to `.scene/quality-gate/`, which is covered by `.gitignore`.

## Local Commands

Run the fast unit gate:

```bash
venv/bin/python scripts/quality_gate.py unit
```

Run Docker-backed integration coverage:

```bash
venv/bin/python scripts/quality_gate.py integration
```

Run frontend Playwright coverage. By default this starts an isolated local app on
an available loopback port with state and artifacts under `.scene/quality-gate/`:

```bash
venv/bin/python scripts/quality_gate.py frontend
```

Set `BASE_URL` and `API_BASE_URL` to test an already-running environment. A
Playwright-managed server is never reused unless `PW_REUSE_EXISTING_SERVER=true`
is set explicitly, which prevents an unrelated local service from satisfying the
readiness probe.

Render the staging Compose configuration:

```bash
venv/bin/python scripts/quality_gate.py staging-config
```

Run the full local gate:

```bash
venv/bin/python scripts/quality_gate.py all
```

## CI Contract

A Linux CI job should run these commands from a clean checkout after installing Python and frontend dependencies:

```bash
python -m pytest -m "not integration" -q
python -m pytest -m integration -q
npm --prefix frontend ci
python scripts/quality_gate.py frontend
docker compose -f docker-compose.staging.yml config
```

Use isolated paths for state and artifacts when invoking the app under test:

```bash
export SCENE_STATE_PATH="$PWD/.scene/quality-gate/state/dev.dynamodb.json"
export SCENE_ARTIFACT_ROOT="$PWD/.scene/quality-gate/artifacts"
```

## Determinism Coverage

The unit gate includes deterministic image fixtures for:

- identical screenshots producing zero diff;
- one-pixel canvas mismatches being padded without false heatmap differences;
- exactly one changed pixel being counted once above tolerance and ignored at tolerance;
- transparent PNG hidden RGB normalization;
- run and execution threshold classification.

The orchestrator gate covers:

- callback success and invalid-token rejection;
- missing callback with container exit reconciliation;
- fail-closed behavior when only an observed image exists;
- timeout cancellation;
- manual run cancellation;
- retry creation;
- artifact/log fallback reconciliation.

## Known Dependencies

SCENE-4 owns the remaining frontend run modal refresh, execution viewer mode, and execution log polling UX stabilization. SCENE-8 documents and runs the frontend gate, but should not rewrite those dashboard surfaces while SCENE-4 is active.

SCENE-7 owns Linux/k3s runner execution. Until a Linux Docker staging host is available, Mac/ARM Docker evidence is developer evidence only; final acceptance should include a Linux headless browser run using the same quality-gate commands.
