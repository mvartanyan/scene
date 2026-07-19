from __future__ import annotations

import hashlib
import io
from pathlib import Path

import pytest

from app.services.s3_artifacts import S3ArtifactStore
from app.routes.artifacts import _artifact_for_key


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], dict[str, object]] = {}
        self.deleted_batches: list[list[str]] = []
        self.presigned: list[tuple[str, str, int]] = []

    def upload_file(self, filename: str, bucket: str, key: str, ExtraArgs: dict) -> None:
        body = Path(filename).read_bytes()
        self.objects[(bucket, key)] = {
            "body": body,
            "ContentType": ExtraArgs["ContentType"],
            "Metadata": dict(ExtraArgs.get("Metadata") or {}),
            "VersionId": "v1",
        }

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, ContentType: str) -> None:
        self.objects[(Bucket, Key)] = {
            "body": Body,
            "ContentType": ContentType,
            "Metadata": {},
            "VersionId": "v1",
        }

    def get_object(self, *, Bucket: str, Key: str) -> dict:
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)]["body"])}

    def head_object(self, *, Bucket: str, Key: str) -> dict:
        value = self.objects[(Bucket, Key)]
        body = value["body"]
        return {
            "ContentLength": len(body),
            "ContentType": value["ContentType"],
            "Metadata": value["Metadata"],
            "ETag": f'"{hashlib.md5(body).hexdigest()}"',  # noqa: S324 - S3-compatible ETag fixture
            "VersionId": value["VersionId"],
        }

    def download_file(self, bucket: str, key: str, filename: str) -> None:
        Path(filename).write_bytes(self.objects[(bucket, key)]["body"])

    def generate_presigned_url(
        self,
        operation: str,
        *,
        Params: dict,
        ExpiresIn: int,
        HttpMethod: str | None = None,
    ) -> str:
        key = Params["Key"]
        self.presigned.append((operation, key, ExpiresIn))
        return f"https://s3.test/{Params['Bucket']}/{key}?signature=secret"

    def delete_objects(self, *, Bucket: str, Delete: dict) -> None:
        keys = [item["Key"] for item in Delete["Objects"]]
        self.deleted_batches.append(keys)
        for key in keys:
            self.objects.pop((Bucket, key), None)

    def delete_object(self, *, Bucket: str, Key: str) -> None:
        self.objects.pop((Bucket, Key), None)


def _store(tmp_path: Path, client: FakeS3Client) -> S3ArtifactStore:
    return S3ArtifactStore(
        bucket="scene-private",
        prefix="scene",
        environment="staging",
        region="eu-central-1",
        root=tmp_path / "workspace",
        client=client,
        get_ttl_seconds=120,
        put_ttl_seconds=300,
    )


@pytest.mark.unit
def test_s3_persist_uses_deterministic_key_and_complete_metadata(tmp_path: Path) -> None:
    client = FakeS3Client()
    store = _store(tmp_path, client)
    source = store.execution_dir("run-1", "exec-1") / "observed.png"
    source.write_bytes(b"png-content")

    artifact = store.persist(
        source,
        project_id="project-1",
        batch_id="batch-1",
        run_id="run-1",
        execution_id="exec-1",
        kind="observed",
        label="Observed Screenshot",
        content_type="image/png",
    )

    assert artifact["key"] == (
        "scene/staging/projects/project-1/batches/batch-1/runs/run-1/"
        "executions/exec-1/observed/observed.png"
    )
    assert artifact["storage"] == "s3"
    assert artifact["bucket"] == "scene-private"
    assert artifact["sha256"] == hashlib.sha256(b"png-content").hexdigest()
    assert artifact["etag"]
    assert artifact["version_id"] == "v1"
    assert "signature=" not in str(artifact["url"])

    materialized = store.materialize(artifact, tmp_path / "download" / "observed.png")
    assert materialized.read_bytes() == b"png-content"
    assert "signature=secret" in str(store.download_url(artifact))


@pytest.mark.unit
def test_s3_transfer_receipts_are_scope_checked_and_verified(tmp_path: Path) -> None:
    client = FakeS3Client()
    store = _store(tmp_path, client)
    transfer = store.create_execution_transfer(
        project_id="project-1",
        batch_id="batch-1",
        run_id="run-1",
        execution_id="exec-1",
    )
    observed = transfer["outputs"]["observed"]
    body = b"observed"
    client.objects[(store.bucket, observed["key"])] = {
        "body": body,
        "ContentType": "image/png",
        "Metadata": {"sha256": hashlib.sha256(body).hexdigest()},
        "VersionId": "v2",
    }

    verified = store.verify_upload_receipts(
        transfer,
        {
            "observed": {
                "key": observed["key"],
                "size_bytes": len(body),
                "sha256": hashlib.sha256(body).hexdigest(),
            }
        },
    )

    assert verified["observed"]["key"] == observed["key"]
    assert all("url" in output for output in transfer["outputs"].values())
    with pytest.raises(ValueError, match="wrong key"):
        store.verify_upload_receipts(
            transfer,
            {"observed": {"key": "scene/staging/other", "size_bytes": len(body)}},
        )


@pytest.mark.unit
def test_s3_deletion_requires_explicit_keys_and_never_lists_bucket(tmp_path: Path) -> None:
    client = FakeS3Client()
    store = _store(tmp_path, client)
    artifact = {
        "kind": "observed",
        "key": "scene/staging/projects/p/batches/b/runs/run-1/executions/e/observed/a.png",
        "path": "scene/staging/projects/p/batches/b/runs/run-1/executions/e/observed/a.png",
    }
    client.objects[(store.bucket, artifact["key"])] = {
        "body": b"x",
        "ContentType": "image/png",
        "Metadata": {},
        "VersionId": "v1",
    }

    with pytest.raises(ValueError, match="explicit artifact metadata"):
        store.purge_run("run-1")
    store.purge_run("run-1", [artifact])

    assert client.deleted_batches == [[artifact["key"]]]
    assert not client.objects


@pytest.mark.unit
def test_s3_readiness_probe_writes_reads_and_deletes(tmp_path: Path) -> None:
    client = FakeS3Client()
    store = _store(tmp_path, client)

    report = store.probe()

    assert report == {
        "backend": "s3",
        "bucket": "scene-private",
        "prefix": "scene",
        "environment": "staging",
        "region": "eu-central-1",
    }
    assert not client.objects


@pytest.mark.unit
def test_s3_download_key_must_match_recorded_execution_artifact() -> None:
    key = "scene/staging/projects/p/batches/b/runs/r/executions/e/observed/observed.png"

    class Repo:
        def get_execution(self, execution_id: str) -> dict | None:
            if execution_id != "e":
                return None
            return {
                "id": "e",
                "run_id": "r",
                "artifacts": {
                    "observed": {"kind": "observed", "key": key, "path": key},
                },
            }

        def get_baseline(self, baseline_id: str) -> None:
            return None

    repo = Repo()

    assert _artifact_for_key(repo, key)["key"] == key
    assert _artifact_for_key(repo, key.replace("observed.png", "unrecorded.png")) is None
