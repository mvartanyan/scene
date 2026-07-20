# SCENE Artifact Storage

SCENE supports two final artifact backends:

- `filesystem` is the default for development and single-host Docker staging.
- `s3` is the production/k3s backend. The application owns AWS access; browser
  runner Jobs do not receive AWS credentials.

## S3 Configuration

```bash
export AWS_REGION=eu-central-1
export SCENE_ARTIFACT_STORAGE=s3
export SCENE_S3_BUCKET=scene-staging-artifacts-<account-id>
export SCENE_S3_PREFIX=scene
export SCENE_S3_GET_TTL_SECONDS=300
export SCENE_S3_PUT_TTL_SECONDS=900
export SCENE_ARTIFACT_TEMP_ROOT=/tmp/scene-artifacts
```

The bucket must be private, encrypted, versioned, and configured with all public
access blocks. Stored artifact metadata includes storage backend, bucket/key,
content type, byte size, SHA-256, ETag, and version ID when S3 returns one.
Database records contain stable SCENE URLs, never presigned query strings.

Keys are deterministic:

```text
<prefix>/<environment>/projects/<project>/batches/<batch>/runs/<run>/executions/<execution>/<kind>/<filename>
<prefix>/<environment>/projects/<project>/batches/<batch>/baselines/<baseline>/executions/<execution>/<kind>/<filename>
```

Run and baseline deletion enumerates artifact metadata already attached to the
records and considers only keys in the matching run/baseline scope. For each
explicit key, SCENE lists that key's versions and delete markers and removes
them by version ID, then re-lists in bounded sweeps to catch a concurrent late
version. Run deletion is rejected while any execution-scoped PUT URL remains
valid, which fences uploads before metadata is removed. It never performs an
unscoped bucket scan. Any S3 batch-delete error or non-converging sweep aborts
metadata deletion so cleanup can be retried safely.

## Runner Transfer

For an S3 execution, SCENE creates a short-lived transfer manifest containing
one exact key and presigned PUT URL per possible output. The manifest is part of
that execution's generated config and is not stored with its URLs. The runner:

1. captures into its ephemeral workspace;
2. uploads observed/reference/trace/video outputs;
3. writes and uploads `result.json` last;
4. callbacks with key, size, SHA-256, and ETag receipts only.

SCENE compares every receipt with its persisted execution scope, confirms the
object exists and has the expected size, and streams it through SHA-256
verification when S3 metadata does not carry the checksum. Presigned URLs and
their query strings are removed from diagnostics and callback payloads.
Once verification records an S3 version ID, materialization, checksum reads,
and presigned downloads address that exact version instead of the mutable
latest object at the key.

Baseline/diff processing downloads only the object required for the current
execution into `SCENE_ARTIFACT_TEMP_ROOT`. Final diff and heatmap files are then
uploaded through the app's S3 principal.

## Readiness

`GET /readyz` performs an S3 write/read/delete probe when the S3 backend
is selected. When bucket versioning returns a version ID, SCENE deletes that
exact probe version so readiness checks do not accumulate retained delete
markers or noncurrent probe objects. Runner readiness checks only callback
reachability and ephemeral workspace access; per-execution uploads prove the
presigned transport itself.
