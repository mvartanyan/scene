from __future__ import annotations

import hashlib
import json
import logging
import mimetypes
import os
import shutil
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, Mapping, Optional

import boto3
from botocore.exceptions import ClientError

from app.services.artifacts import ArtifactStore, SCENE_ARTIFACT_TEMP_ROOT_ENV

LOGGER = logging.getLogger(__name__)

SCENE_S3_BUCKET_ENV = "SCENE_S3_BUCKET"
SCENE_S3_PREFIX_ENV = "SCENE_S3_PREFIX"
SCENE_S3_ENDPOINT_URL_ENV = "SCENE_S3_ENDPOINT_URL"
SCENE_S3_GET_TTL_SECONDS_ENV = "SCENE_S3_GET_TTL_SECONDS"
SCENE_S3_PUT_TTL_SECONDS_ENV = "SCENE_S3_PUT_TTL_SECONDS"
SCENE_ENV_ENV = "SCENE_ENV"
AWS_REGION_ENV = "AWS_REGION"

DEFAULT_GET_TTL_SECONDS = 300
DEFAULT_PUT_TTL_SECONDS = 900
MAX_PRESIGN_TTL_SECONDS = 3600


def _positive_ttl(name: str, default: int) -> int:
    raw = os.environ.get(name)
    try:
        value = int(raw) if raw else default
    except ValueError:
        value = default
    return max(30, min(value, MAX_PRESIGN_TTL_SECONDS))


def _clean_segment(value: object) -> str:
    return str(value or "").strip().strip("/")


class S3ArtifactStore(ArtifactStore):
    """S3-backed artifacts with a bounded local workspace for image processing."""

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str,
        environment: str,
        region: str,
        root: Optional[Path] = None,
        base_url: str = "/artifacts",
        client: object = None,
        get_ttl_seconds: int = DEFAULT_GET_TTL_SECONDS,
        put_ttl_seconds: int = DEFAULT_PUT_TTL_SECONDS,
    ) -> None:
        if not bucket.strip():
            raise ValueError("SCENE_S3_BUCKET is required for S3 artifact storage.")
        workspace = root or Path(tempfile.gettempdir()) / "scene-artifacts"
        super().__init__(root=workspace, base_url=base_url)
        self.bucket = bucket.strip()
        self.prefix = _clean_segment(prefix)
        self.environment = _clean_segment(environment) or "staging"
        self.region = region.strip() or "eu-central-1"
        self.get_ttl_seconds = max(30, min(int(get_ttl_seconds), MAX_PRESIGN_TTL_SECONDS))
        self.put_ttl_seconds = max(30, min(int(put_ttl_seconds), MAX_PRESIGN_TTL_SECONDS))
        self._client = client or boto3.client("s3", region_name=self.region)

    @classmethod
    def from_environment(cls, *, base_url: str = "/artifacts") -> "S3ArtifactStore":
        region = os.environ.get(AWS_REGION_ENV, "eu-central-1")
        endpoint_url = os.environ.get(SCENE_S3_ENDPOINT_URL_ENV) or None
        client = boto3.client("s3", region_name=region, endpoint_url=endpoint_url)
        temp_root = Path(
            os.environ.get(
                SCENE_ARTIFACT_TEMP_ROOT_ENV,
                str(Path(tempfile.gettempdir()) / "scene-artifacts"),
            )
        ).expanduser()
        return cls(
            bucket=os.environ.get(SCENE_S3_BUCKET_ENV, ""),
            prefix=os.environ.get(SCENE_S3_PREFIX_ENV, "scene"),
            environment=os.environ.get(SCENE_ENV_ENV, "staging"),
            region=region,
            root=temp_root,
            base_url=base_url,
            client=client,
            get_ttl_seconds=_positive_ttl(SCENE_S3_GET_TTL_SECONDS_ENV, DEFAULT_GET_TTL_SECONDS),
            put_ttl_seconds=_positive_ttl(SCENE_S3_PUT_TTL_SECONDS_ENV, DEFAULT_PUT_TTL_SECONDS),
        )

    @property
    def backend(self) -> str:
        return "s3"

    def deterministic_key(self, **scope: object) -> str:
        relative = super().deterministic_key(**scope)
        parts = [self.prefix, self.environment, relative]
        return "/".join(part for part in parts if part)

    def persist(
        self,
        source: Path,
        *,
        project_id: str,
        batch_id: str,
        kind: str,
        label: str,
        content_type: Optional[str] = None,
        run_id: Optional[str] = None,
        execution_id: Optional[str] = None,
        baseline_id: Optional[str] = None,
        filename: Optional[str] = None,
    ) -> Dict[str, object]:
        if not source.exists() or not source.is_file():
            raise FileNotFoundError(source)
        object_name = filename or source.name
        key = self.deterministic_key(
            project_id=project_id,
            batch_id=batch_id,
            run_id=run_id,
            execution_id=execution_id,
            baseline_id=baseline_id,
            kind=kind,
            filename=object_name,
        )
        resolved_type = content_type or mimetypes.guess_type(object_name)[0] or "application/octet-stream"
        sha256 = self.checksum(source)
        self._client.upload_file(
            str(source),
            self.bucket,
            key,
            ExtraArgs={
                "ContentType": resolved_type,
                "Metadata": {"sha256": sha256},
            },
        )
        return self._metadata_from_head(
            key=key,
            kind=kind,
            label=label,
            content_type=resolved_type,
            sha256=sha256,
        )

    def _metadata_from_head(
        self,
        *,
        key: str,
        kind: str,
        label: str,
        content_type: Optional[str] = None,
        sha256: Optional[str] = None,
    ) -> Dict[str, object]:
        head = self._client.head_object(Bucket=self.bucket, Key=key)
        metadata = head.get("Metadata") or {}
        checksum = sha256 or metadata.get("sha256")
        result: Dict[str, object] = {
            "kind": kind,
            "storage": self.backend,
            "bucket": self.bucket,
            "key": key,
            "path": key,
            "url": self.object_url(key),
            "label": label,
            "content_type": content_type or head.get("ContentType") or "application/octet-stream",
            "size_bytes": int(head.get("ContentLength") or 0),
        }
        if checksum:
            result["sha256"] = str(checksum)
        etag = str(head.get("ETag") or "").strip('"')
        if etag:
            result["etag"] = etag
        if head.get("VersionId"):
            result["version_id"] = str(head["VersionId"])
        return result

    def materialize(self, artifact: Mapping[str, object], destination: Optional[Path] = None) -> Path:
        if artifact.get("storage") == "workspace":
            return super().materialize(artifact, destination)
        key = str(artifact.get("key") or artifact.get("path") or "")
        if not key:
            raise FileNotFoundError("Artifact metadata has no S3 key.")
        if destination is None:
            digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
            suffix = Path(key).suffix
            destination = self.root / "materialized" / f"{digest}{suffix}"
        destination.parent.mkdir(parents=True, exist_ok=True)
        self._client.download_file(self.bucket, key, str(destination))
        return destination

    def download_url(self, artifact: Mapping[str, object]) -> Optional[str]:
        if artifact.get("storage") == "workspace":
            value = artifact.get("url")
            return str(value) if value else None
        key = str(artifact.get("key") or artifact.get("path") or "")
        if not key:
            return None
        return self._client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": key},
            ExpiresIn=self.get_ttl_seconds,
        )

    def presign_upload(
        self,
        *,
        key: str,
        content_type: str,
        sha256: Optional[str] = None,
    ) -> str:
        params: Dict[str, object] = {
            "Bucket": self.bucket,
            "Key": key,
            "ContentType": content_type,
        }
        if sha256:
            params["Metadata"] = {"sha256": sha256}
        return self._client.generate_presigned_url(
            "put_object",
            Params=params,
            ExpiresIn=self.put_ttl_seconds,
            HttpMethod="PUT",
        )

    def create_execution_transfer(
        self,
        *,
        project_id: str,
        batch_id: str,
        run_id: str,
        execution_id: str,
        baseline_artifact: Optional[Mapping[str, object]] = None,
    ) -> Dict[str, object]:
        outputs = {
            "observed": ("observed.png", "image/png"),
            "reference": ("reference.png", "image/png"),
            "trace": ("trace.zip", "application/zip"),
            "video": ("video.webm", "video/webm"),
            "result": ("result.json", "application/json"),
        }
        manifest_outputs: Dict[str, object] = {}
        for kind, (filename, content_type) in outputs.items():
            key = self.deterministic_key(
                project_id=project_id,
                batch_id=batch_id,
                run_id=run_id,
                execution_id=execution_id,
                kind=kind,
                filename=filename,
            )
            manifest_outputs[kind] = {
                "method": "PUT",
                "key": key,
                "filename": filename,
                "content_type": content_type,
                "required": kind not in {"reference", "video"},
                "url": self.presign_upload(key=key, content_type=content_type),
            }
        inputs: Dict[str, object] = {}
        if baseline_artifact:
            baseline_key = str(baseline_artifact.get("key") or baseline_artifact.get("path") or "")
            if baseline_key:
                inputs["baseline"] = {
                    "method": "GET",
                    "key": baseline_key,
                    "filename": "baseline.png",
                    "url": self.download_url(baseline_artifact),
                }
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=self.put_ttl_seconds)
        return {
            "version": 1,
            "expires_at": expires_at.isoformat(),
            "outputs": manifest_outputs,
            "inputs": inputs,
        }

    def verify_upload_receipts(
        self,
        transfer: Mapping[str, object],
        receipts: Mapping[str, object],
    ) -> Dict[str, Dict[str, object]]:
        configured_outputs = transfer.get("outputs")
        if not isinstance(configured_outputs, Mapping):
            raise ValueError("Execution transfer has no output manifest.")
        verified: Dict[str, Dict[str, object]] = {}
        for kind, raw_receipt in receipts.items():
            expected = configured_outputs.get(kind)
            if not isinstance(expected, Mapping) or not isinstance(raw_receipt, Mapping):
                raise ValueError(f"Unexpected artifact upload receipt '{kind}'.")
            key = str(raw_receipt.get("key") or "")
            if not key or key != str(expected.get("key") or ""):
                raise ValueError(f"Artifact upload receipt '{kind}' has the wrong key.")
            head = self._client.head_object(Bucket=self.bucket, Key=key)
            expected_size = raw_receipt.get("size_bytes")
            if expected_size is not None and int(expected_size) != int(head.get("ContentLength") or 0):
                raise ValueError(f"Artifact upload receipt '{kind}' has the wrong size.")
            expected_etag = str(raw_receipt.get("etag") or "").strip('"')
            actual_etag = str(head.get("ETag") or "").strip('"')
            if expected_etag and actual_etag and expected_etag != actual_etag:
                raise ValueError(f"Artifact upload receipt '{kind}' has the wrong ETag.")
            expected_sha = str(raw_receipt.get("sha256") or "")
            stored_sha = str((head.get("Metadata") or {}).get("sha256") or "")
            if stored_sha and expected_sha and stored_sha != expected_sha:
                raise ValueError(f"Artifact upload receipt '{kind}' has the wrong checksum.")
            verified_sha = stored_sha
            if expected_sha and not verified_sha:
                response = self._client.get_object(Bucket=self.bucket, Key=key)
                digest = hashlib.sha256()
                body = response["Body"]
                for chunk in iter(lambda: body.read(1024 * 1024), b""):
                    digest.update(chunk)
                verified_sha = digest.hexdigest()
                if verified_sha != expected_sha:
                    raise ValueError(f"Artifact upload receipt '{kind}' has the wrong checksum.")
            verified[str(kind)] = self._metadata_from_head(
                key=key,
                kind=str(kind),
                label=str(kind).replace("_", " ").title(),
                content_type=str(expected.get("content_type") or "application/octet-stream"),
                sha256=verified_sha or expected_sha or None,
            )
        return verified

    def delete_artifacts(self, artifacts: Iterable[Mapping[str, object]]) -> None:
        identifiers = []
        seen: set[str] = set()
        for artifact in artifacts:
            key = str(artifact.get("key") or artifact.get("path") or "")
            if key and key not in seen:
                seen.add(key)
                identifiers.append({"Key": key})
        for start in range(0, len(identifiers), 1000):
            self._client.delete_objects(
                Bucket=self.bucket,
                Delete={"Objects": identifiers[start : start + 1000], "Quiet": True},
            )

    def purge_run(
        self,
        run_id: str,
        artifacts: Optional[Iterable[Mapping[str, object]]] = None,
    ) -> None:
        if artifacts is None:
            raise ValueError("S3 run deletion requires explicit artifact metadata.")
        marker = f"/runs/{run_id}/"
        scoped = [
            artifact
            for artifact in artifacts
            if marker in f"/{str(artifact.get('key') or artifact.get('path') or '').lstrip('/')}"
        ]
        self.delete_artifacts(scoped)
        shutil.rmtree(self.root / "runs" / run_id, ignore_errors=True)

    def purge_baseline(
        self,
        baseline_id: str,
        artifacts: Optional[Iterable[Mapping[str, object]]] = None,
    ) -> None:
        if artifacts is None:
            raise ValueError("S3 baseline deletion requires explicit artifact metadata.")
        marker = f"/baselines/{baseline_id}/"
        scoped = [
            artifact
            for artifact in artifacts
            if marker in f"/{str(artifact.get('key') or artifact.get('path') or '').lstrip('/')}"
        ]
        self.delete_artifacts(scoped)
        shutil.rmtree(self.root / "baselines" / baseline_id, ignore_errors=True)

    def probe(self) -> Dict[str, object]:
        key = "/".join(part for part in [self.prefix, self.environment, ".probe", str(uuid.uuid4())] if part)
        body = json.dumps({"probe": "scene"}).encode("utf-8")
        try:
            self._client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body,
                ContentType="application/json",
            )
            response = self._client.get_object(Bucket=self.bucket, Key=key)
            received = response["Body"].read()
            if received != body:
                raise RuntimeError("S3 artifact probe read did not match its write.")
        finally:
            try:
                self._client.delete_object(Bucket=self.bucket, Key=key)
            except ClientError:
                LOGGER.exception("Failed to remove S3 artifact readiness probe object %s", key)
        return {
            "backend": self.backend,
            "bucket": self.bucket,
            "prefix": self.prefix,
            "environment": self.environment,
            "region": self.region,
        }
