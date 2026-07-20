# SCENE State Storage

SCENE supports two state backends behind `SceneRepository`:

- `json` is the default for local development and tests. It persists one file at
  `SCENE_STATE_PATH` and is not suitable for multiple processes or pods.
- `dynamodb` is the production backend. It uses optimistic versions,
  conditional writes, indexed project/run access, and opaque continuation
  tokens.

## Production Configuration

```bash
export AWS_REGION=eu-central-1
export SCENE_STATE_BACKEND=dynamodb
export SCENE_DYNAMODB_TABLE=scene-staging
```

`SCENE_DYNAMODB_ENDPOINT_URL` is available for disposable local integration
tests only. AWS credentials come from the SCENE app or worker principal. Do not
put AWS credentials in runner manifests or Jobs.

When `SCENE_STATE_BACKEND=dynamodb`, constructing the adapter validates the
table and all index key schemas. `GET /readyz` performs the write/read/delete
probe and reports a sanitized failure without coupling process liveness to an
AWS outage. The legacy `GET /api/orchestrator/readiness` also exposes state and
runner readiness for agent compatibility.

## Table Model

The table uses string partition and sort keys named `pk` and `sk`. Each item
contains an application `payload` map and an integer `version` used by
conditional writes.

| Access path | Index partition key | Index sort key |
| --- | --- | --- |
| Entity by ID | `pk=COLLECTION#<type>` | `sk=<id>` |
| Project pages/tasks/batches/runs/baselines | `gsi1pk=PROJECT#<project-id>#<type>` | `gsi1sk=<created>#<id>` |
| Batch runs/baselines | `gsi2pk=BATCH#<batch-id>#<type>` | `gsi2sk=<created>#<id>` |
| Run executions | `gsi2pk=RUN#<run-id>#executions` | `gsi2sk=<sequence>#<created>#<id>` |
| Collection chronology | `gsi3pk=COLLECTION#<type>` | `gsi3sk=<created>#<id>` |

The required GSIs are named `gsi1`, `gsi2`, and `gsi3`, each with corresponding
`<index>pk` and `<index>sk` keys. All indexes project the complete item. The
CloudFormation definition under `deploy/aws/` is the canonical infrastructure
definition once SCENE-13 is deployed.

Updates read the current version and use a conditional put. Repository updates
retry bounded conflicts and return HTTP `409` if contention does not converge.
Run and execution creation support caller-stable idempotency keys. Reusing a key
with a different launch payload is a conflict. Competing terminal execution
transitions use first-successful-write semantics, so a late completion cannot
overwrite a cancellation and vice versa.

## Bounded Reads

The following REST endpoints return at most 100 records and an opaque
`next_cursor`:

- `GET /api/projects/page`
- `GET /api/projects/{project_id}/pages/page`
- `GET /api/projects/{project_id}/tasks/page`
- `GET /api/projects/{project_id}/batches/page`
- `GET /api/runs/page`
- `GET /api/runs/{run_id}/executions/page`

Use the returned cursor unchanged on the next request. Cursors are backend
specific and malformed cursors return `400`. The HTML project tabs, run history,
and execution overlay preserve their numbered URLs but fetch each active page
through the same bounded repository operations.

## Configuration Transfer

Export only global config, projects, pages, tasks, and batches:

```bash
venv/bin/python scripts/scene_config.py export \
  --output /secure/path/scene-config.json
```

The exporter preserves IDs, timestamps, basic-auth credentials, actions,
browser/viewports, thresholds, and SPM references. It writes the output with
mode `0600`. The document intentionally excludes runs, executions, baselines,
webhook delivery history, and artifact references.

Validate an import without writing:

```bash
SCENE_STATE_BACKEND=dynamodb \
SCENE_DYNAMODB_TABLE=scene-staging \
AWS_REGION=eu-central-1 \
venv/bin/python scripts/scene_config.py import \
  --input /secure/path/scene-config.json
```

Apply the same validated document:

```bash
SCENE_STATE_BACKEND=dynamodb \
SCENE_DYNAMODB_TABLE=scene-staging \
AWS_REGION=eu-central-1 \
venv/bin/python scripts/scene_config.py import \
  --input /secure/path/scene-config.json \
  --apply
```

Reports contain create/update/skip/error counts and excluded runtime-record
counts only. They never print imported field values. The complete reference
graph is validated before the first write. JSON imports roll back as one local
transaction; DynamoDB imports are idempotent and safe to replay after an
interruption.
