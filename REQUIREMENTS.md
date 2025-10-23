# 1) Purpose & Scope

Design a simple but powerful web front‑end and backend to orchestrate Playwright-based visual checks across multiple browser/viewport combinations, store baselines, and compare subsequent runs. Built with **FastAPI** (Python) and **htmx** on the frontend. Execution happens in **ephemeral Docker containers**.

**Primary goals**

* CRUD entities: Projects, Pages, Tasks, Batches, Baselines, Test Runs.
* Run tasks as a matrix over browser runtimes × viewport sizes with optional page JS.
* Capture artifacts: screenshots, diffs/heatmaps, logs, traces, and (optional) videos.
* Compare against baselines and/or a “reference page” (e.g., production) per Page.
* Real-time visibility of container status and artifacts as they complete.

**Non-goals (v1)**

* Full end-to-end functional testing; this is focused on visual/regression checks.
* Multi-tenant SaaS with billing. (Single-tenant or intra‑org first.)

---

# 2) Domain Model (Concepts)

**Project** – Top-level grouping. Owns Pages and Tasks.

**Page** – Named URL (starting URL) with optional preparatory JS. Optional **reference_url** (e.g., prod) for side-by-side/ref diffs.

**Task** – Belongs to a Project, associated with exactly one Page. Defines:

* One or more **browser runtimes** (chromium/firefox/webkit) and **viewport sizes**.
* Optional **task JS** to execute during the run (e.g., click, login, dismiss popups).
* One **default baseline policy** (e.g., “use latest baseline from Batch X” or “none”).

**Batch** – A named set of Tasks intended to run together. Used to create **Baselines** and **Test Runs**.

**Baseline** – Immutable snapshot results for a full Batch executed at time *T*. Stores per‑task artifacts that later runs compare against. A Test Run with `purpose=baseline_recording` produces a Baseline once all task executions finish successfully.

**Test Run** – An execution instance of a Batch against a chosen Baseline (or none). Expands into **Task Executions** (matrix of browser × viewport per Task). Streams status and artifacts as containers complete.

**Task Execution** – The atomic unit run in one container for (task, browser, viewport).

> Note: You initially said “Baselines are within batch” and “Tasks are associated with one page and one baseline.” To avoid conflict, this spec treats **Baseline** as a *result of a Batch* at a specific time. Tasks don’t own a baseline; **Test Runs select** which Baseline (or none) to compare against.

---

# 3) What’s Missing / Recommended Additions

1. **Orchestration layer** ✅ (add): Minimal queue + worker that launches Docker containers (Python Docker SDK). Supports retries (per‑exec), simple concurrency cap, and hard timeouts. States limited to **queued → executing → finished | failed**.
2. **Artifact storage abstraction** ✅ (add): Local FS for dev; S3-compatible backend optional behind a simple `Storage` interface. Deterministic paths and stable URLs.
3. **Diff/heatmap generation** ✅ (add): Use an external image diff tool from the runner rather than app‑level logic. **Recommended**: `odiff` (Rust CLI) for fast pixel diff; fallback to ImageMagick `compare`. Generate a mask and an overlay heatmap (PNG) plus `stats.json`.
4. **Playwright tracing & video** ✅ (add): Enable trace and video for debugging; store per Task Execution.
5. **Realtime updates (simplified)** ✅ (add): No WebSockets. Use **htmx polling** (e.g., `hx-get` every 2–5s) to refresh run/execution tiles. Statuses are only: **in queue**, **executing**, **finished**, **failed**.

> Omitted for v1 per product decision: Scheduling, RBAC & audit, configuration versioning, secrets manager, explicit resource controls, flaky surfacing in UI (kept inside JS), and retention/quota management.

---

# 4) High-Level Architecture

* **FastAPI App**: REST + server-rendered fragments for htmx.
* **Worker Service**: Python process consuming a queue; launches Docker containers per Task Execution.
* **Dockerized Playwright Runner**: A small CLI that accepts a JSON payload, runs the job, emits artifacts to a known folder, and exits with a status code.
* **Artifact Storage**: Pluggable backend (FileSystem / S3).
* **Diff/Heatmap Generator**: Library invoked post-screenshot to produce diffs.
* **Web UI (htmx)**: CRUD screens + live run dashboards + artifact viewers.
* **Realtime**: WebSockets (prefer) or SSE for push updates; htmx swaps for partials.

---

# 5) Data Model (ER Sketch)

```
Project (id, name, slug, created_at, updated_at)
  ├─ Page (id, project_id, name, url, reference_url, preparatory_js, created_at)
  └─ Task (id, project_id, page_id, name, task_js, browsers[], viewports[], default_baseline_policy, created_at)

Batch (id, project_id, name, description, created_by, created_at)
  ├─ BatchTask (id, batch_id, task_id, enabled)
  ├─ Baseline (id, batch_id, created_at, created_by, status, notes)
  └─ BaselineItem (id, baseline_id, task_id, browser, viewport, artifacts_path)

TestRun (id, batch_id, baseline_id NULLABLE, purpose ENUM('baseline_recording','comparison'),
         status ENUM('queued','executing','finished','failed'),
         started_at, finished_at, requested_by)
  └─ TaskExecution (id, test_run_id, task_id, browser, viewport, status,
                    exit_code, started_at, finished_at, container_id, retries,
                    artifacts_path, metrics_json, no_retry, stop_requested_at)

Artifact (id, task_execution_id, type ENUM('screenshot','diff','heatmap','trace','video','log'),
          path, width, height, created_at)

Webhook (id, project_id NULL, endpoint_url, secret, event_types[], enabled, custom_headers)
WebhookDelivery (id, webhook_id, event_type, status, attempts, next_attempt_at, response_code, response_ms, error, payload_json, delivery_id, created_at)

AuditLog (id, actor, action, entity_type, entity_id, data_json, created_at)
```

Project (id, name, slug, created_at, updated_at)
├─ Page (id, project_id, name, url, reference_url, preparatory_js, created_at)
└─ Task (id, project_id, page_id, name, task_js, browsers[], viewports[], default_baseline_policy, created_at)

Batch (id, project_id, name, description, created_by, created_at)
├─ BatchTask (id, batch_id, task_id, enabled)
└─ Baseline (id, batch_id, created_at, created_by, status, notes)

TestRun (id, batch_id, baseline_id NULLABLE, purpose ENUM('baseline_recording','comparison'),
status ENUM('queued','executing','finished','failed'),
started_at, finished_at, requested_by)
└─ TaskExecution (id, test_run_id, task_id, browser, viewport, status,
exit_code, started_at, finished_at, container_id, retries,
artifacts_path, metrics_json)

Artifact (id, task_execution_id, type ENUM('screenshot','diff','heatmap','trace','video','log'),
path, width, height, created_at)

AuditLog (id, actor, action, entity_type, entity_id, data_json, created_at)

````

**Indices**: `(project_id, slug)`, `(batch_id, created_at)`, `(test_run_id, status)`, `(task_id, browser, viewport)`.

---

# 6) API Design (FastAPI)
Use Pydantic models for requests/responses. htmx endpoints return HTML partials for list/detail panes.

**Projects**
- `GET /projects` (list) – htmx fragment + full page
- `POST /projects` (create)
- `GET /projects/{id}` (detail)
- `PUT /projects/{id}` (update)
- `DELETE /projects/{id}` (delete)

**Pages**
- `GET /projects/{pid}/pages`
- `POST /projects/{pid}/pages`
- `GET /pages/{id}` / `PUT /pages/{id}` / `DELETE /pages/{id}`

**Tasks**
- `GET /projects/{pid}/tasks`
- `POST /projects/{pid}/tasks` (payload includes `page_id`, `browsers`, `viewports`, `task_js`)
- `GET /tasks/{id}` / `PUT /tasks/{id}` / `DELETE /tasks/{id}`

**Batches & Baselines**
- `GET /projects/{pid}/batches` / `POST /projects/{pid}/batches`
- `GET /batches/{id}` / `PUT /batches/{id}` / `DELETE /batches/{id}`
- `GET /batches/{id}/baselines` (list)
- `POST /batches/{id}/baselines/from-test-run/{run_id}` (finalize)

**Test Runs**
- `GET /runs?batch_id=&status=&q=` (chronological list, filters)
- `POST /runs` (create: `{batch_id, baseline_id|null, purpose}`)
- `GET /runs/{id}` (dashboard)
- `POST /runs/{id}/cancel`
- `POST /runs/{id}/stop` (body: `{mode: 'soft'|'hard'}`) – propagate stop to all executions in the run.
- `GET /runs/{id}/viewer` – Backstop‑style aggregate viewer for a run (paginated executions).

**Task Executions**
- `GET /runs/{id}/executions` (list)
- `GET /executions/{id}` (detail + artifacts)
- `GET /executions/{id}/viewer` – Backstop‑style viewer for a single execution.
- `POST /executions/{id}/stop` (body: `{mode: 'soft'|'hard'}`) – stop a single execution.

**Artifacts**
- `GET /artifacts/{id}` (serve/redirect to storage)

**Webhooks**
- `GET /webhooks` / `POST /webhooks` / `GET /webhooks/{id}` / `PUT /webhooks/{id}` / `DELETE /webhooks/{id}`
- `GET /webhooks/{id}/deliveries` (list) / `GET /deliveries/{delivery_id}` (detail)
- `POST /deliveries/{delivery_id}/redeliver`

**Admin / System**
- `GET /health` `GET /metrics` (Prometheus) `GET /version`

---

# 7) Frontend (htmx) UX Outline
- **Projects List** → Project Detail tabs: Pages | Tasks | Batches | Settings.
- **Pages**: table with inline create/edit (htmx modals). Show `url`, `reference_url`, JS editors.
- **Tasks**: table + detail. Matrix preview of browsers × viewports. Test “dry run” button per task.
- **Batches**: select Tasks (checkboxes). Show last baselines and last run status.
- **Runs (global)**: chronological stream with filters (project, batch, status, actor, time).
- **Run Dashboard**: grid of Task Executions with **htmx polling** updates every 2–5s; each tile reveals screenshot, diff, heatmap, trace/video links as they appear. Controls: cancel run, approve as baseline when `purpose=baseline_recording`.
- **Artifact viewers**: lightbox with side‑by‑side, slider, and flicker modes. Heatmap toggle and threshold slider.

**Realtime**: htmx polling only; no WebSockets/SSE in v1.

## 7.1 Backstop‑style Diff Viewer (include)
A built‑in viewer inspired by **BackstopJS** reports, available for each **Run** and **Task Execution**.

**Modes**
- **Side‑by‑side**: baseline ⟷ observed.
- **Slider (before/after)**: draggable handle reveals observed over baseline.
- **Onion‑skin**: opacity slider of observed over baseline.
- **Heatmap overlay**: toggle to overlay diff/heatmap above observed.

**Controls**
- Keyboard: `←/→` prev/next execution, `S` slider mode, `O` onion‑skin, `H` heatmap toggle, `F` flicker.
- UI toggles: **Baseline | Observed | Diff | Heatmap** buttons, **% threshold** (if supported by odiff).

**Implementation**
- Pure HTML/CSS/JS component; no external libs required. (Optional: swap to JuxtaposeJS later.)
- Images loaded from stable artifact URLs.
- CSS layering: baseline (bottom), observed (middle), heatmap (top, initially hidden). Range input controls clip/opacity.
- htmx fragments for list pane and detail pane; partial swaps on navigation.

**HTMX fragment (outline)**
```html
<div id="viewer" hx-get="/executions/{{eid}}/viewer" hx-trigger="load from:body" hx-swap="outerHTML">
  <div class="toolbar">
    <button data-mode="side">Side</button>
    <button data-mode="slider">Slider</button>
    <button data-mode="onion">Onion</button>
    <label><input type="checkbox" id="toggle-heatmap"> Heatmap</label>
  </div>
  <div class="canvas relative overflow-hidden">
    <img id="img-base" src="{{baseline_url}}" class="block select-none"/>
    <img id="img-observed" src="{{observed_url}}" class="absolute inset-0 select-none"/>
    <img id="img-heat" src="{{heatmap_url}}" class="absolute inset-0 hidden mix-blend-multiply"/>
    <input id="slider" type="range" min="0" max="100" value="50" class="absolute bottom-2 left-2 right-2" />
  </div>
  <script>
    (function(){
      const base = document.getElementById('img-base');
      const obs = document.getElementById('img-observed');
      const heat = document.getElementById('img-heat');
      const slider = document.getElementById('slider');
      const toggle = document.getElementById('toggle-heatmap');
      function applyClip(pct){ obs.style.clipPath = `inset(0 ${100-pct}% 0 0)`; }
      slider.addEventListener('input', e => applyClip(+e.target.value));
      toggle.addEventListener('change', e => heat.classList.toggle('hidden', !e.target.checked));
      applyClip(50);
    })();
  </script>
</div>
````

**Artifacts expected**

* `baseline_url`: baseline screenshot for this execution (task+browser+viewport)
* `observed_url`: current execution screenshot
* `diff_url` (optional): raw diff (magenta style)
* `heatmap_url` (optional): overlay for the heatmap mode
* `stats_json`: numbers for UI (changed_pixels, % changed)

---

# 8) Orchestration & Docker Execution

* **Queue**: Simple DB‑backed or Redis queue (Dramatiq/RQ or custom). Minimal: FIFO per run; global concurrency cap.

* **Worker** takes Test Run, enumerates Task Executions (cartesian product of browsers×viewports for each enabled Task in Batch), enqueues them.

* **Docker**: Use Python Docker SDK. For each Task Execution:

  * Image: `mcr.microsoft.com/playwright/python:<tag>` or a pinned custom image with `odiff` and ImageMagick installed.
  * Mount: `/artifacts/<project>/<run>/<execution_id>/` for outputs.
  * Env/Input: serialized payload (URLs, JS, timeouts, auth cookies, flags) via file or env var.
  * Limits: basic timeouts only in v1; other resource controls omitted.

* **Status lifecycle**: `queued → executing → finished | failed`.

## 8.1 Stopping & Cancellation

* **Soft stop (execution)**: mark `no_retry=true` and set `stop_requested_at` on the execution; send a signal to the container to exit gracefully. When it exits, mark final status according to exit code (finished/failed) **but never retry**, even if `max_retries` not reached.

* **Hard stop (execution)**: immediately kill the container; delete any queued retry; remove any pending job from the queue; set `reason='hard_stop'` and status `failed`.

* **Run-level stop**: apply soft/hard to all active executions in the run; remove queued jobs accordingly.

* **Queue contract**: must support deleting a queued job, checking in‑flight attempts, and visibility timeouts. Implement adapters for Redis/Dramatiq/RQ with a uniform interface.

* **Retry policy** (still internal, not surfaced in UI): exponential backoff with jitter; emit a `retry_enqueued` webhook (see §11A) with attempt count.

---

# 9) Playwright Runner (inside container)

* **Input JSON**:

```json
{
  "page": {"url": "https://...", "reference_url": "https://...", "preparatory_js": "..."},
  "task": {"task_js": "...", "selectors": [], "wait_until": "networkidle"},
  "browser": "chromium",
  "viewport": {"width": 1280, "height": 800},
  "artifacts_dir": "/artifacts/...",
  "flags": {"trace": true, "video": true, "screenshot_full_page": true},
  "timeouts": {"navigation_ms": 30000, "action_ms": 10000},
  "execution_id": 123,
  "run_id": 456
}
```

* **Steps**: launch → newContext(viewport) → newPage → go to `page.url` → run preparatory JS → run task JS → capture screenshot → if `reference_url` present, also screenshot that in a second context → emit Playwright trace and console/network logs.
* **Output**: `result.json` (metadata), `page.png`, `reference.png` (optional), `trace.zip`, `video.webm`, `console.log`, `network.har`.
* **Signals**: respond to soft stop by closing context/browser and exiting; hard stop handled by Docker kill.

---

# 10) Diff & Heatmap Generation

* **When**: After screenshots, a post‑processor compares:

  1. `page.png` vs Baseline’s `page.png` for the same task/browser/viewport.
  2. `page.png` vs `reference.png` (if available) for environment diffs.
* **Tooling (v1)**: Prefer **`odiff` CLI** for pixel diffs and mask; fallback **ImageMagick `compare`**. Both run inside the Playwright runner container.
* **Outputs** (plus `webhook_stats` for external consumers):

  * `diff.png` (highlighted differing pixels)
  * `heatmap.png` (overlay built from the mask)
  * `stats.json` (changed_pixels, percent_changed, bounding_boxes[])
* **Thresholds**: CLI `--threshold` exposed via Task settings; anti‑aliasing ignore where available.
* **Note**: If the runner is switched to **Node + @playwright/test**, we could use `expect(page).toHaveScreenshot()` for built‑in snapshot diffs; however, our Python runner keeps diffs as a separate post‑step for control over artifacts and layout.

---

# 11) Baselines – Lifecycle

* **Create**: Start a Test Run with `purpose=baseline_recording` on a Batch.
* **Finalize**: When all Task Executions succeed, freeze artifacts as a new Baseline row.
* **Use**: Subsequent Test Runs against this Batch can reference a selected Baseline (default = latest successful).
* **Retention**: Keep last N; soft-delete older (configurable).

---

# 11A) Webhooks (status & artifacts)

**Goal**: Push rich lifecycle events to user-defined HTTPS endpoints when work is **enqueued, started, finished, failed, or enqueued for retry**, and on run/baseline milestones.

**Entity: Webhook**

* `id`, `project_id|null` (null = global), `endpoint_url`, `secret` (HMAC), `event_types[]`, `enabled`, `custom_headers{}`.

**Entity: WebhookDelivery**

* `id`, `webhook_id`, `event_type`, `status` (pending|succeeded|failed), `attempts`, `next_attempt_at`, `response_code`, `response_ms`, `error`, `payload_json`, `delivery_id` (uuid), `created_at`.

**Event types**

* `run.started`, `run.finished`
* `baseline.finalized`
* `task.enqueued` (when actually queued)
* `task.started`
* `task.finished`
* `task.failed`
* `task.retry_enqueued` (includes `attempt`)

**Headers**

* `X-Scene-Event: <event_type>`
* `X-Scene-Delivery: <uuid>` (idempotency)
* `X-Scene-Timestamp: <RFC3339>`
* `X-Scene-Signature: sha256=<HMAC_HEX>` over `<timestamp>.<raw-body>` using the webhook secret

**Sample payload: `task.enqueued`**

```json
{
  "event": "task.enqueued",
  "delivery_id": "b8c1...",
  "occurred_at": "2025-10-23T12:34:56Z",
  "attempt": 0,
  "project": {"id": 1, "name": "Website"},
  "batch": {"id": 7, "name": "Release 1.2"},
  "baseline": {"id": 22, "created_at": "2025-10-20T10:00:00Z"},
  "run": {"id": 101, "purpose": "comparison"},
  "task": {"id": 55, "name": "Homepage"},
  "page": {"id": 9, "name": "Home", "url": "https://staging.example.com/", "reference_url": "https://www.example.com/"},
  "execution": {
    "id": 999,
    "browser": "chromium",
    "viewport": {"width":1280,"height":800}
  }
}
```

**Sample payload: `task.finished`**

```json
{
  "event": "task.finished",
  "delivery_id": "e7a2...",
  "occurred_at": "2025-10-23T12:39:12Z",
  "attempt": 1,
  "duration_ms": 24750,
  "project": {"id": 1, "name": "Website"},
  "run": {"id": 101, "viewer_url": "/runs/101/viewer?exec=999"},
  "task": {"id": 55, "name": "Homepage"},
  "execution": {
    "id": 999,
    "status": "finished",
    "artifacts": {
      "observed_url": "/artifacts/123/page.png",
      "baseline_url": "/artifacts/88/page.png",
      "diff_url": "/artifacts/123/diff.png",
      "heatmap_url": "/artifacts/123/heatmap.png",
      "trace_url": "/artifacts/123/trace.zip",
      "video_url": "/artifacts/123/video.webm"
    },
    "stats": {"changed_pixels": 2312, "percent_changed": 0.42}
  }
}
```

**Delivery**

* At-least-once with exponential backoff (e.g., 15s, 60s, 5m, 20m).
* `2xx` = success; `>=400` retried up to `max_attempts`.
* Redelivery supported via API.

---

# 11B) Baseline semantics & compatibility rules

* **Scope:** A Baseline belongs to a **Batch** and represents the artifacts produced by that batch's enabled tasks at time T.
* **Coverage:** Baseline contains zero or more `BaselineItem` rows keyed by `(task_id, browser, viewport)` and pointing to frozen `artifacts_path`.
* **Missing items:** During a comparison run, if a corresponding BaselineItem is missing, the UI marks the execution **No baseline**; observed artifacts are still captured and published and (if `reference_url` exists) the reference diff is still produced.
* **Batch changes after baseline:** If tasks are added/removed from a Batch, existing baselines are **not mutated**. New tasks have no baseline until a future baseline run records them. Removed tasks remain in old baselines for historical viewing but are ignored by new runs unless re‑enabled and matched.
* **Immutability:** Baselines are immutable. New baselines supersede via selection (default = latest successful).
* **Webhooks:** `baseline.finalized` includes coverage counts (`items_total`, `items_succeeded`, `items_missing`).

---

# 12) Security, Secrets, Compliance (v1 minimal)

* **Auth**: optional basic auth/reverse proxy auth (e.g., behind company SSO via proxy). No RBAC in v1.
* **PII**: avoid collecting user data; redact logs.

---

# 13) Observability & Ops

* **Logging**: structured JSON logs; correlation IDs: project/batch/run/execution.
* **Metrics**: Prometheus counters/gauges (runs started/succeeded/failed, avg durations, queue depth).
* **Tracing**: OpenTelemetry for API + worker.
* **Health**: `/health` (db, queue, storage), `/metrics` (Prometheus).

---

# 14) Performance & Reliability

* Concurrency controls per Run and globally.
* Backoff/retries for transient failures.
* Image pre‑pulling and caching.
* Timeouts per navigation and overall execution.
* Atomic write of artifacts + checksum manifest.

---

# 15) Testing Strategy

* Unit tests: models, diff wrapper, API, webhook signing & deliveries.
* Integration: spin up Docker-in-Docker, run a minimal Task Execution; assert webhooks emitted at `enqueued`, `started`, `finished/failed`.
* E2E (smoke): create Project→Page→Task→Batch→Run; verify artifacts, diffs, viewer, and webhook deliveries.

---

# 16) Acceptance Criteria (condensed)

**Projects/Pages/Tasks**

* CRUD works via htmx without full-page reloads.
* Page supports `preparatory_js`; Task supports `task_js`, browsers[], viewports[].

**Batches/Baselines**

* Batch selects tasks; can start baseline run; successful completion creates immutable Baseline.

**Runs**

* Can start a Run with or without Baseline; tiles update via polling; artifacts appear incrementally.
* Per-execution artifacts include screenshot, logs, optional trace & video.
* Diff & heatmap generated when Baseline or reference_url available; UI exposes them.

**Backstop‑style Viewer**

* Run viewer shows baseline/observed **slider**, **side‑by‑side**, **onion‑skin**, **flicker**, and **heatmap overlay**.
* Keyboard shortcuts work; navigation across executions works without full reload (htmx swaps).

**Webhooks**

* When an execution is **enqueued**, a webhook fires including rich context (project, batch, page, baseline, run, task, execution).
* Webhooks fire on **started**, **finished**, **failed**, and **retry_enqueued** with attempt count.
* Finished event contains stable artifact URLs and viewer URLs.
* HMAC signature headers present; at-least-once delivery with redelivery API.

**Stopping**

* `POST /executions/{id}/stop` supports `mode=soft|hard` with semantics above.
* `POST /runs/{id}/stop` stops the whole run with the chosen mode; queued retries removed on hard stop.

**Statuses**

* Each Task Execution and the parent Run reflect only: `in queue`, `executing`, `finished`, or `failed`.

**Storage & Links**

* All artifacts accessible via stable URLs; downloadable.

---

# 17) Example Pydantic Models (snippets for Codex)

```python
from pydantic import BaseModel, HttpUrl, Field
from typing import List, Optional, Literal

class Viewport(BaseModel):
    width: int
    height: int

class PageIn(BaseModel):
    name: str
    url: HttpUrl
    reference_url: Optional[HttpUrl] = None
    preparatory_js: Optional[str] = None

class TaskIn(BaseModel):
    project_id: int
    page_id: int
    name: str
    task_js: Optional[str] = None
    browsers: List[Literal['chromium','firefox','webkit']]
    viewports: List[Viewport]

class CreateRun(BaseModel):
    batch_id: int
    baseline_id: Optional[int] = None
    purpose: Literal['baseline_recording','comparison']
```

---

# 18) Storage Layout (example)

```
/artifacts/
  project_{pid}/
    batch_{bid}/
      run_{rid}/
        exec_{eid}/
          page.png
          reference.png
          diff.png
          heatmap.png
          trace.zip
          video.webm
          network.har
          console.log
          result.json
```

---

# 19) Developer Notes for Codex (prompt/style hints)

* Be explicit with types and enums; include docstrings and logging.
* Keep functions small; separate concerns (API, service layer, storage, docker, diff).
* Provide one `run_playwright(payload: dict) -> int` entrypoint inside container.
* Use dependency injection in FastAPI for db/session/queue clients.
* Prefer HTML partials for htmx (`hx-get`, `hx-post`, `hx-swap-oob`).
* Return early on errors; attach correlation IDs to logs.

---

# 20) Future Extensions

* Scheduled runs and webhooks (notify Slack when diffs exceed threshold).
* Baseline approval workflow (multi‑review).
* DOM snapshots and element-level diff heatmaps.
* Parallelization across multiple worker hosts.
* Multi-tenancy and org/project permissions.

---

# 21) Glossary

* **Heatmap**: visual overlay highlighting pixel-diff intensity between two images.
* **Baseline**: frozen set of artifacts from a Batch run at a moment in time used for comparison.
* **Task Execution**: (task, browser, viewport) unit executed in a container.
