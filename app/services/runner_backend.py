from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping, Optional
from urllib.parse import urlparse

SCENE_RUNNER_BACKEND_ENV = "SCENE_RUNNER_BACKEND"
SCENE_RUNNER_IMAGE_ENV = "SCENE_RUNNER_IMAGE"
SCENE_RUNNER_IMAGE_AUTOBUILD_ENV = "SCENE_RUNNER_IMAGE_AUTOBUILD"
SCENE_RUNNER_SHM_SIZE_ENV = "SCENE_RUNNER_SHM_SIZE"
SCENE_RUNNER_EXTRA_HOST_GATEWAY_ENV = "SCENE_RUNNER_EXTRA_HOST_GATEWAY"
SCENE_RUNNER_CALLBACK_BASE_URL_ENV = "SCENE_RUNNER_CALLBACK_BASE_URL"
SCENE_ARTIFACT_STORAGE_ENV = "SCENE_ARTIFACT_STORAGE"
SCENE_ARTIFACT_PVC_CLAIM_ENV = "SCENE_ARTIFACT_PVC_CLAIM"
SCENE_ARTIFACT_OBJECT_URL_ENV = "SCENE_ARTIFACT_OBJECT_URL"
SCENE_S3_BUCKET_ENV = "SCENE_S3_BUCKET"
SCENE_S3_PREFIX_ENV = "SCENE_S3_PREFIX"
SCENE_K3S_NAMESPACE_ENV = "SCENE_K3S_NAMESPACE"
SCENE_K3S_SERVICE_URL_ENV = "SCENE_K3S_SERVICE_URL"
SCENE_K3S_RUNNER_SERVICE_ACCOUNT_ENV = "SCENE_K3S_RUNNER_SERVICE_ACCOUNT"
SCENE_K3S_IMAGE_PULL_POLICY_ENV = "SCENE_K3S_IMAGE_PULL_POLICY"
SCENE_K3S_JOB_TTL_SECONDS_ENV = "SCENE_K3S_JOB_TTL_SECONDS"
SCENE_RUNNER_CPU_REQUEST_ENV = "SCENE_RUNNER_CPU_REQUEST"
SCENE_RUNNER_CPU_LIMIT_ENV = "SCENE_RUNNER_CPU_LIMIT"
SCENE_RUNNER_MEMORY_REQUEST_ENV = "SCENE_RUNNER_MEMORY_REQUEST"
SCENE_RUNNER_MEMORY_LIMIT_ENV = "SCENE_RUNNER_MEMORY_LIMIT"

DEFAULT_RUNNER_IMAGE = "scene-playwright-runner:latest"
SUPPORTED_BACKENDS = {"docker", "k3s", "worker"}
SUPPORTED_ARTIFACT_STORAGE = {"filesystem", "pvc", "s3"}
PRODUCTION_ENVIRONMENTS = {"prod", "production", "staging", "k3s"}


def _clean(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _env_bool(name: str, default: bool) -> bool:
    raw = _clean(os.environ.get(name))
    if raw is None:
        return default
    return raw.lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = _clean(os.environ.get(name))
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _default_allow_image_build(backend: str) -> bool:
    environment = (
        os.environ.get("SCENE_ENV")
        or os.environ.get("APP_ENV")
        or os.environ.get("ENVIRONMENT")
        or ""
    ).strip().lower()
    return backend == "docker" and environment not in PRODUCTION_ENVIRONMENTS


@dataclass(frozen=True)
class RunnerRuntimeConfig:
    backend: str
    image: str
    callback_base_url: str
    artifact_root: Path
    artifact_storage: str
    allow_image_build: bool
    shm_size: str
    add_host_gateway: bool
    max_concurrency: int
    artifact_pvc_claim: Optional[str] = None
    artifact_object_url: Optional[str] = None
    s3_bucket: Optional[str] = None
    s3_prefix: str = "scene"
    k3s_namespace: str = "scene"
    k3s_service_url: Optional[str] = None
    k3s_runner_service_account: str = "scene-runner"
    k3s_image_pull_policy: str = "IfNotPresent"
    k3s_job_ttl_seconds: int = 3600
    cpu_request: str = "500m"
    cpu_limit: str = "2"
    memory_request: str = "512Mi"
    memory_limit: str = "2Gi"

    @property
    def supports_inline_launch(self) -> bool:
        return self.backend == "docker"

    def as_dict(self) -> Dict[str, Any]:
        return {
            "backend": self.backend,
            "image": self.image,
            "callback_base_url": self.callback_base_url,
            "artifact_root": str(self.artifact_root),
            "artifact_storage": self.artifact_storage,
            "allow_image_build": self.allow_image_build,
            "shm_size": self.shm_size,
            "add_host_gateway": self.add_host_gateway,
            "max_concurrency": self.max_concurrency,
            "artifact_pvc_claim": self.artifact_pvc_claim,
            "artifact_object_url": self.artifact_object_url,
            "s3_bucket": self.s3_bucket,
            "s3_prefix": self.s3_prefix,
            "k3s_namespace": self.k3s_namespace,
            "k3s_service_url": self.k3s_service_url,
            "k3s_runner_service_account": self.k3s_runner_service_account,
            "k3s_image_pull_policy": self.k3s_image_pull_policy,
            "k3s_job_ttl_seconds": self.k3s_job_ttl_seconds,
            "cpu_request": self.cpu_request,
            "cpu_limit": self.cpu_limit,
            "memory_request": self.memory_request,
            "memory_limit": self.memory_limit,
        }


@dataclass(frozen=True)
class ReadinessIssue:
    level: str
    code: str
    message: str

    def as_dict(self) -> Dict[str, str]:
        return {
            "level": self.level,
            "code": self.code,
            "message": self.message,
        }


@dataclass(frozen=True)
class RunnerReadinessReport:
    config: RunnerRuntimeConfig
    issues: tuple[ReadinessIssue, ...]

    @property
    def ok(self) -> bool:
        return not any(issue.level == "error" for issue in self.issues)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "config": self.config.as_dict(),
            "issues": [issue.as_dict() for issue in self.issues],
        }


def load_runner_runtime_config(
    app_config: Optional[Mapping[str, object]] = None,
    *,
    artifact_root: Optional[Path] = None,
) -> RunnerRuntimeConfig:
    config = app_config or {}
    backend = (os.environ.get(SCENE_RUNNER_BACKEND_ENV) or "docker").strip().lower()
    image = _clean(os.environ.get(SCENE_RUNNER_IMAGE_ENV)) or DEFAULT_RUNNER_IMAGE
    scene_host_url = str(config.get("scene_host_url") or "http://host.docker.internal:8000")
    k3s_service_url = _clean(os.environ.get(SCENE_K3S_SERVICE_URL_ENV))
    callback_override = _clean(os.environ.get(SCENE_RUNNER_CALLBACK_BASE_URL_ENV))
    callback_base_url = callback_override or (
        k3s_service_url if backend == "k3s" and k3s_service_url else scene_host_url
    )
    artifact_storage = (
        os.environ.get(SCENE_ARTIFACT_STORAGE_ENV) or "filesystem"
    ).strip().lower()
    if artifact_storage == "object":
        artifact_storage = "s3"
    root = artifact_root or Path(os.environ.get("SCENE_ARTIFACT_ROOT", ".scene/artifacts"))
    max_concurrency = int(config.get("max_concurrent_executions") or 4)

    return RunnerRuntimeConfig(
        backend=backend,
        image=image,
        callback_base_url=callback_base_url.rstrip("/"),
        artifact_root=root.expanduser(),
        artifact_storage=artifact_storage,
        allow_image_build=_env_bool(
            SCENE_RUNNER_IMAGE_AUTOBUILD_ENV,
            _default_allow_image_build(backend),
        ),
        shm_size=_clean(os.environ.get(SCENE_RUNNER_SHM_SIZE_ENV)) or "1g",
        add_host_gateway=_env_bool(SCENE_RUNNER_EXTRA_HOST_GATEWAY_ENV, backend == "docker"),
        max_concurrency=max_concurrency,
        artifact_pvc_claim=_clean(os.environ.get(SCENE_ARTIFACT_PVC_CLAIM_ENV)),
        artifact_object_url=_clean(os.environ.get(SCENE_ARTIFACT_OBJECT_URL_ENV)),
        s3_bucket=_clean(os.environ.get(SCENE_S3_BUCKET_ENV)),
        s3_prefix=_clean(os.environ.get(SCENE_S3_PREFIX_ENV)) or "scene",
        k3s_namespace=_clean(os.environ.get(SCENE_K3S_NAMESPACE_ENV)) or "scene",
        k3s_service_url=k3s_service_url,
        k3s_runner_service_account=(
            _clean(os.environ.get(SCENE_K3S_RUNNER_SERVICE_ACCOUNT_ENV)) or "scene-runner"
        ),
        k3s_image_pull_policy=(
            _clean(os.environ.get(SCENE_K3S_IMAGE_PULL_POLICY_ENV)) or "IfNotPresent"
        ),
        k3s_job_ttl_seconds=_env_int(SCENE_K3S_JOB_TTL_SECONDS_ENV, 3600),
        cpu_request=_clean(os.environ.get(SCENE_RUNNER_CPU_REQUEST_ENV)) or "500m",
        cpu_limit=_clean(os.environ.get(SCENE_RUNNER_CPU_LIMIT_ENV)) or "2",
        memory_request=_clean(os.environ.get(SCENE_RUNNER_MEMORY_REQUEST_ENV)) or "512Mi",
        memory_limit=_clean(os.environ.get(SCENE_RUNNER_MEMORY_LIMIT_ENV)) or "2Gi",
    )


def validate_runner_runtime_config(config: RunnerRuntimeConfig) -> RunnerReadinessReport:
    issues: list[ReadinessIssue] = []

    def error(code: str, message: str) -> None:
        issues.append(ReadinessIssue("error", code, message))

    def warning(code: str, message: str) -> None:
        issues.append(ReadinessIssue("warning", code, message))

    if config.backend not in SUPPORTED_BACKENDS:
        error(
            "unsupported_backend",
            f"SCENE_RUNNER_BACKEND must be one of {sorted(SUPPORTED_BACKENDS)}.",
        )
    if not config.image:
        error("missing_runner_image", "SCENE_RUNNER_IMAGE must be set.")
    if config.artifact_storage not in SUPPORTED_ARTIFACT_STORAGE:
        error(
            "unsupported_artifact_storage",
            f"SCENE_ARTIFACT_STORAGE must be one of {sorted(SUPPORTED_ARTIFACT_STORAGE)}.",
        )

    callback = urlparse(config.callback_base_url)
    if callback.scheme not in {"http", "https"} or not callback.netloc:
        error(
            "invalid_callback_url",
            "Runner callback base URL must be an absolute http(s) URL.",
        )

    hostname = callback.hostname or ""
    if config.backend in {"k3s", "worker"}:
        if hostname == "host.docker.internal":
            error(
                "host_docker_internal_for_cluster",
                "k3s/worker runners must use a cluster service URL, not host.docker.internal.",
            )
        if config.allow_image_build:
            error(
                "image_autobuild_enabled",
                "k3s/worker runners must pull a prebuilt image; disable SCENE_RUNNER_IMAGE_AUTOBUILD.",
            )
        if config.image.endswith(":latest"):
            error(
                "unpinned_runner_image",
                "k3s/worker runners must use an explicitly versioned runner image tag.",
            )
        if config.artifact_storage == "filesystem":
            error(
                "local_artifact_storage_for_cluster",
                "k3s/worker runners require shared artifact storage via PVC or object storage.",
            )
        if config.artifact_storage == "pvc" and not config.artifact_pvc_claim:
            error(
                "missing_artifact_pvc",
                "SCENE_ARTIFACT_PVC_CLAIM is required when SCENE_ARTIFACT_STORAGE=pvc.",
            )
        if config.artifact_storage == "s3" and not config.s3_bucket:
            error(
                "missing_s3_bucket",
                "SCENE_S3_BUCKET is required when SCENE_ARTIFACT_STORAGE=s3.",
            )
    if config.backend == "k3s":
        if not config.k3s_service_url:
            error(
                "missing_k3s_service_url",
                "SCENE_K3S_SERVICE_URL must point at the SCENE service DNS name in the cluster.",
            )
        elif config.callback_base_url.rstrip("/") != config.k3s_service_url.rstrip("/"):
            warning(
                "callback_url_differs_from_k3s_service_url",
                "SCENE_RUNNER_CALLBACK_BASE_URL differs from SCENE_K3S_SERVICE_URL; verify runner pods use the intended service URL.",
            )
    if config.backend == "docker":
        if hostname == "host.docker.internal" and not config.add_host_gateway:
            warning(
                "host_gateway_disabled",
                "host.docker.internal callbacks usually require SCENE_RUNNER_EXTRA_HOST_GATEWAY=true in Linux Docker.",
            )
        if config.image.endswith(":latest"):
            warning(
                "unpinned_local_runner_image",
                "The local Docker runner image is using :latest; staging should pin SCENE_RUNNER_IMAGE.",
            )
    if config.max_concurrency > 2 and config.backend in {"k3s", "worker"}:
        warning(
            "cluster_concurrency_above_default",
            "Start k3s/worker runner concurrency at 1-2 until browser resource limits are validated.",
        )
    if not config.shm_size:
        error("missing_shm_size", "SCENE_RUNNER_SHM_SIZE must be set for headless browser stability.")

    return RunnerReadinessReport(config=config, issues=tuple(issues))
