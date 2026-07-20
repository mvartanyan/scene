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
        self.deleted_requests: list[list[dict[str, str]]] = []
        self.deleted_objects: list[dict[str, str]] = []
        self.presigned: list[tuple[str, str, int]] = []
        self.presigned_params: list[dict[str, object]] = []
        self.head_requests: list[dict[str, str]] = []
        self.get_requests: list[dict[str, str]] = []
        self.download_requests: list[dict[str, object]] = []
        self.versions: list[dict[str, str]] = []
        self.delete_markers: list[dict[str, str]] = []
        self.delete_errors: list[dict[str, str]] = []
        self.versions_after_delete: list[list[dict[str, str]]] = []
        self.add_version_after_every_delete = False
        self.versioned_objects: dict[tuple[str, str, str], dict[str, object]] = {}

    def upload_file(self, filename: str, bucket: str, key: str, ExtraArgs: dict) -> None:
        body = Path(filename).read_bytes()
        self.objects[(bucket, key)] = {
            "body": body,
            "ContentType": ExtraArgs["ContentType"],
            "Metadata": dict(ExtraArgs.get("Metadata") or {}),
            "VersionId": "v1",
        }

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, ContentType: str) -> dict:
        self.objects[(Bucket, Key)] = {
            "body": Body,
            "ContentType": ContentType,
            "Metadata": {},
            "VersionId": "v1",
        }
        return {"VersionId": "v1"}

    def _object(self, bucket: str, key: str, version_id: str | None = None) -> dict[str, object]:
        if version_id:
            versioned = self.versioned_objects.get((bucket, key, version_id))
            if versioned is not None:
                return versioned
            current = self.objects[(bucket, key)]
            if str(current.get("VersionId") or "") == version_id:
                return current
            raise KeyError((bucket, key, version_id))
        return self.objects[(bucket, key)]

    def get_object(self, *, Bucket: str, Key: str, VersionId: str | None = None) -> dict:
        request = {"Bucket": Bucket, "Key": Key}
        if VersionId:
            request["VersionId"] = VersionId
        self.get_requests.append(request)
        return {"Body": io.BytesIO(self._object(Bucket, Key, VersionId)["body"])}

    def head_object(self, *, Bucket: str, Key: str, VersionId: str | None = None) -> dict:
        request = {"Bucket": Bucket, "Key": Key}
        if VersionId:
            request["VersionId"] = VersionId
        self.head_requests.append(request)
        value = self._object(Bucket, Key, VersionId)
        body = value["body"]
        return {
            "ContentLength": len(body),
            "ContentType": value["ContentType"],
            "Metadata": value["Metadata"],
            "ETag": f'"{hashlib.md5(body).hexdigest()}"',  # noqa: S324 - S3-compatible ETag fixture
            "VersionId": value["VersionId"],
        }

    def download_file(
        self,
        bucket: str,
        key: str,
        filename: str,
        ExtraArgs: dict[str, str] | None = None,
    ) -> None:
        version_id = str((ExtraArgs or {}).get("VersionId") or "") or None
        self.download_requests.append(
            {"Bucket": bucket, "Key": key, "ExtraArgs": dict(ExtraArgs or {})}
        )
        Path(filename).write_bytes(self._object(bucket, key, version_id)["body"])

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
        self.presigned_params.append(dict(Params))
        return f"https://s3.test/{Params['Bucket']}/{key}?signature=secret"

    def list_object_versions(self, *, Bucket: str, Prefix: str, **_kwargs: object) -> dict:
        versions = [item for item in self.versions if item["Key"].startswith(Prefix)]
        if not versions:
            versions = [
                {"Key": key, "VersionId": str(value["VersionId"])}
                for (bucket, key), value in self.objects.items()
                if bucket == Bucket and key.startswith(Prefix)
            ]
        return {
            "Versions": versions,
            "DeleteMarkers": [
                item for item in self.delete_markers if item["Key"].startswith(Prefix)
            ],
            "IsTruncated": False,
        }

    def delete_objects(self, *, Bucket: str, Delete: dict) -> dict:
        keys = [item["Key"] for item in Delete["Objects"]]
        self.deleted_batches.append(keys)
        self.deleted_requests.append([dict(item) for item in Delete["Objects"]])
        if not self.delete_errors:
            for item in Delete["Objects"]:
                key = item["Key"]
                version_id = item.get("VersionId")
                self.versions = [
                    version
                    for version in self.versions
                    if not (version["Key"] == key and version["VersionId"] == version_id)
                ]
                self.delete_markers = [
                    marker
                    for marker in self.delete_markers
                    if not (marker["Key"] == key and marker["VersionId"] == version_id)
                ]
                value = self.objects.get((Bucket, key))
                if value and str(value.get("VersionId") or "") == version_id:
                    self.objects.pop((Bucket, key), None)
        if self.versions_after_delete:
            self.versions.extend(self.versions_after_delete.pop(0))
        if self.add_version_after_every_delete:
            for key in keys:
                self.versions.append(
                    {"Key": key, "VersionId": f"late-{len(self.deleted_requests)}"}
                )
        return {"Errors": list(self.delete_errors)}

    def delete_object(
        self,
        *,
        Bucket: str,
        Key: str,
        VersionId: str | None = None,
    ) -> None:
        request = {"Bucket": Bucket, "Key": Key}
        if VersionId:
            request["VersionId"] = VersionId
        self.deleted_objects.append(request)
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
def test_s3_reads_pin_recorded_artifact_version(tmp_path: Path) -> None:
    client = FakeS3Client()
    store = _store(tmp_path, client)
    key = "scene/staging/projects/p/batches/b/runs/r/executions/e/observed/observed.png"
    client.objects[(store.bucket, key)] = {
        "body": b"new-version",
        "ContentType": "image/png",
        "Metadata": {},
        "VersionId": "v2",
    }
    client.versioned_objects[(store.bucket, key, "v1")] = {
        "body": b"recorded-version",
        "ContentType": "image/png",
        "Metadata": {},
        "VersionId": "v1",
    }
    artifact = {"storage": "s3", "key": key, "version_id": "v1"}

    destination = store.materialize(artifact, tmp_path / "recorded.png")
    url = store.download_url(artifact)

    assert destination.read_bytes() == b"recorded-version"
    assert "signature=secret" in str(url)
    assert client.download_requests[-1]["ExtraArgs"] == {"VersionId": "v1"}
    assert client.presigned_params[-1]["VersionId"] == "v1"


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
def test_s3_receipt_verification_pins_version_for_head_body_and_metadata(tmp_path: Path) -> None:
    client = FakeS3Client()
    store = _store(tmp_path, client)
    transfer = store.create_execution_transfer(
        project_id="project-1",
        batch_id="batch-1",
        run_id="run-1",
        execution_id="exec-1",
    )
    observed = transfer["outputs"]["observed"]
    recorded_body = b"recorded"
    client.objects[(store.bucket, observed["key"])] = {
        "body": b"late-overwrite",
        "ContentType": "image/png",
        "Metadata": {},
        "VersionId": "v2",
    }
    client.versioned_objects[(store.bucket, observed["key"], "v1")] = {
        "body": recorded_body,
        "ContentType": "image/png",
        "Metadata": {},
        "VersionId": "v1",
    }

    verified = store.verify_upload_receipts(
        transfer,
        {
            "observed": {
                "key": observed["key"],
                "version_id": "v1",
                "size_bytes": len(recorded_body),
                "sha256": hashlib.sha256(recorded_body).hexdigest(),
            }
        },
    )

    assert verified["observed"]["version_id"] == "v1"
    assert client.get_requests[-1]["VersionId"] == "v1"
    assert [request.get("VersionId") for request in client.head_requests[-2:]] == ["v1", "v1"]


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
    assert client.deleted_requests == [[{"Key": artifact["key"], "VersionId": "v1"}]]
    assert not client.objects


@pytest.mark.unit
def test_s3_purge_deletes_all_versions_and_markers_for_explicit_keys(tmp_path: Path) -> None:
    client = FakeS3Client()
    store = _store(tmp_path, client)
    key = "scene/staging/projects/p/batches/b/runs/run-1/executions/e/result/result.json"
    unrelated = f"{key}.unrelated"
    client.versions = [
        {"Key": key, "VersionId": "v1"},
        {"Key": key, "VersionId": "v2"},
        {"Key": unrelated, "VersionId": "other"},
    ]
    client.delete_markers = [{"Key": key, "VersionId": "marker-1"}]

    store.purge_run("run-1", [{"key": key}])

    assert {tuple(sorted(item.items())) for item in client.deleted_requests[0]} == {
        tuple(sorted({"Key": key, "VersionId": "v1"}.items())),
        tuple(sorted({"Key": key, "VersionId": "v2"}.items())),
        tuple(sorted({"Key": key, "VersionId": "marker-1"}.items())),
    }
    assert all(item["Key"] != unrelated for item in client.deleted_requests[0])


@pytest.mark.unit
def test_s3_purge_re_lists_and_deletes_a_late_version(tmp_path: Path) -> None:
    client = FakeS3Client()
    store = _store(tmp_path, client)
    key = "scene/staging/projects/p/batches/b/runs/run-1/executions/e/result/result.json"
    client.versions = [{"Key": key, "VersionId": "v1"}]
    client.versions_after_delete = [[{"Key": key, "VersionId": "late-v2"}]]

    store.purge_run("run-1", [{"key": key}])

    assert client.deleted_requests == [
        [{"Key": key, "VersionId": "v1"}],
        [{"Key": key, "VersionId": "late-v2"}],
    ]


@pytest.mark.unit
def test_s3_purge_fails_when_versions_keep_appearing(tmp_path: Path) -> None:
    client = FakeS3Client()
    store = _store(tmp_path, client)
    key = "scene/staging/projects/p/batches/b/runs/run-1/executions/e/result/result.json"
    client.versions = [{"Key": key, "VersionId": "v1"}]
    client.add_version_after_every_delete = True

    with pytest.raises(RuntimeError, match="versions continued to appear"):
        store.purge_run("run-1", [{"key": key}])

    assert len(client.deleted_requests) > 1


@pytest.mark.unit
def test_s3_delete_errors_are_not_silently_ignored(tmp_path: Path) -> None:
    client = FakeS3Client()
    store = _store(tmp_path, client)
    key = "scene/staging/projects/p/batches/b/runs/run-1/executions/e/result/result.json"
    client.versions = [{"Key": key, "VersionId": "v1"}]
    client.delete_errors = [{"Key": key, "VersionId": "v1", "Code": "AccessDenied"}]

    with pytest.raises(RuntimeError, match="AccessDenied"):
        store.purge_run("run-1", [{"key": key}])


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
    assert len(client.deleted_objects) == 1
    assert client.deleted_objects[0]["VersionId"] == "v1"
    assert client.get_requests[0]["VersionId"] == "v1"


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
