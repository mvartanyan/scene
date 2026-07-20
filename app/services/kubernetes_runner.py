from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

try:
    from kubernetes import client, config
    from kubernetes.client.exceptions import ApiException
except ModuleNotFoundError:  # pragma: no cover - dependency is present in staging images
    client = None  # type: ignore[assignment]
    config = None  # type: ignore[assignment]

    class ApiException(Exception):
        status: Optional[int] = None


LOGGER = logging.getLogger("scene.kubernetes_runner")

MANAGED_LABEL = "scene.spherical.horse/managed"
EXECUTION_LABEL = "scene.spherical.horse/execution-id"
GENERATION_LABEL = "scene.spherical.horse/dispatch-generation"
SPEC_DIGEST_ANNOTATION = "scene.spherical.horse/spec-digest"


@dataclass(frozen=True)
class KubernetesExecutionStatus:
    phase: str
    reason: Optional[str] = None
    message: Optional[str] = None
    pod_name: Optional[str] = None
    exit_code: Optional[int] = None

    @property
    def terminal(self) -> bool:
        return self.phase in {
            "succeeded",
            "failed",
            "deadline_exceeded",
            "image_pull",
            "oom_killed",
            "evicted",
            "unschedulable",
            "missing",
        }


class KubernetesRunnerClient:
    """Create, adopt, inspect, and delete one Kubernetes Job per execution."""

    def __init__(
        self,
        *,
        namespace: str,
        image: str,
        service_account: str,
        image_pull_policy: str,
        ttl_seconds: int,
        cpu_request: str,
        cpu_limit: str,
        memory_request: str,
        memory_limit: str,
        batch_api: Any = None,
        core_api: Any = None,
        auth_api: Any = None,
        load_config: bool = True,
    ) -> None:
        if client is None:
            raise RuntimeError("The kubernetes package is required for the k3s runner backend.")
        if load_config and (batch_api is None or core_api is None):
            try:
                config.load_incluster_config()
            except config.ConfigException:
                config.load_kube_config()
        self.namespace = namespace
        self.image = image
        self.service_account = service_account
        self.image_pull_policy = image_pull_policy
        self.ttl_seconds = max(60, int(ttl_seconds))
        self.cpu_request = cpu_request
        self.cpu_limit = cpu_limit
        self.memory_request = memory_request
        self.memory_limit = memory_limit
        self.batch_api = batch_api or client.BatchV1Api()
        self.core_api = core_api or client.CoreV1Api()
        self.auth_api = auth_api or client.AuthorizationV1Api()

    @staticmethod
    def spec_digest(config_payload: Dict[str, object]) -> str:
        canonical = json.dumps(config_payload, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @staticmethod
    def _metadata(
        *,
        name: str,
        execution_id: str,
        generation: int,
        spec_digest: str,
    ) -> Any:
        return client.V1ObjectMeta(
            name=name,
            labels={
                MANAGED_LABEL: "true",
                EXECUTION_LABEL: str(execution_id),
                GENERATION_LABEL: str(generation),
                "app.kubernetes.io/name": "scene",
                "app.kubernetes.io/component": "runner",
            },
            annotations={SPEC_DIGEST_ANNOTATION: spec_digest},
        )

    def build_secret(
        self,
        *,
        name: str,
        execution_id: str,
        generation: int,
        runner_config: Dict[str, object],
        callback_token: str,
        spec_digest: str,
    ) -> Any:
        payload = dict(runner_config)
        payload["artifacts_dir"] = "/workspace"
        return client.V1Secret(
            api_version="v1",
            kind="Secret",
            metadata=self._metadata(
                name=name,
                execution_id=execution_id,
                generation=generation,
                spec_digest=spec_digest,
            ),
            immutable=True,
            type="Opaque",
            string_data={
                "config.json": json.dumps(payload, separators=(",", ":"), default=str),
                "callback-token": callback_token,
            },
        )

    def build_job(
        self,
        *,
        name: str,
        secret_name: str,
        execution_id: str,
        run_id: str,
        generation: int,
        callback_url: str,
        timeout_seconds: int,
        spec_digest: str,
    ) -> Any:
        labels = {
            MANAGED_LABEL: "true",
            EXECUTION_LABEL: str(execution_id),
            GENERATION_LABEL: str(generation),
            "app.kubernetes.io/name": "scene",
            "app.kubernetes.io/component": "runner",
        }
        security_context = client.V1SecurityContext(
            allow_privilege_escalation=False,
            capabilities=client.V1Capabilities(drop=["ALL"]),
            read_only_root_filesystem=True,
            run_as_non_root=True,
            run_as_user=1000,
            run_as_group=1000,
        )
        container = client.V1Container(
            name="runner",
            image=self.image,
            image_pull_policy=self.image_pull_policy,
            command=["python", "/opt/scene/runner.py", "/config/config.json"],
            env=[
                client.V1EnvVar(name="SCENE_CALLBACK_URL", value=callback_url),
                client.V1EnvVar(
                    name="SCENE_CALLBACK_TOKEN",
                    value_from=client.V1EnvVarSource(
                        secret_key_ref=client.V1SecretKeySelector(
                            name=secret_name,
                            key="callback-token",
                        )
                    ),
                ),
                client.V1EnvVar(name="SCENE_EXECUTION_ID", value=str(execution_id)),
                client.V1EnvVar(name="SCENE_RUN_ID", value=str(run_id)),
                client.V1EnvVar(name="SCENE_DISPATCH_GENERATION", value=str(generation)),
                client.V1EnvVar(name="HOME", value="/tmp"),
            ],
            resources=client.V1ResourceRequirements(
                requests={"cpu": self.cpu_request, "memory": self.memory_request},
                limits={"cpu": self.cpu_limit, "memory": self.memory_limit},
            ),
            security_context=security_context,
            volume_mounts=[
                client.V1VolumeMount(name="config", mount_path="/config", read_only=True),
                client.V1VolumeMount(name="workspace", mount_path="/workspace"),
                client.V1VolumeMount(name="tmp", mount_path="/tmp"),
                client.V1VolumeMount(name="dev-shm", mount_path="/dev/shm"),
            ],
        )
        pod_spec = client.V1PodSpec(
            restart_policy="Never",
            service_account_name=self.service_account,
            automount_service_account_token=False,
            node_selector={"kubernetes.io/arch": "amd64"},
            containers=[container],
            security_context=client.V1PodSecurityContext(
                run_as_non_root=True,
                run_as_user=1000,
                run_as_group=1000,
                fs_group=1000,
                fs_group_change_policy="OnRootMismatch",
                seccomp_profile=client.V1SeccompProfile(type="RuntimeDefault")
            ),
            volumes=[
                client.V1Volume(
                    name="config",
                    secret=client.V1SecretVolumeSource(
                        secret_name=secret_name,
                        default_mode=0o440,
                    ),
                ),
                client.V1Volume(name="workspace", empty_dir=client.V1EmptyDirVolumeSource()),
                client.V1Volume(name="tmp", empty_dir=client.V1EmptyDirVolumeSource()),
                client.V1Volume(
                    name="dev-shm",
                    empty_dir=client.V1EmptyDirVolumeSource(medium="Memory", size_limit="1Gi"),
                ),
            ],
        )
        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels=labels),
            spec=pod_spec,
        )
        return client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=self._metadata(
                name=name,
                execution_id=execution_id,
                generation=generation,
                spec_digest=spec_digest,
            ),
            spec=client.V1JobSpec(
                template=template,
                backoff_limit=0,
                active_deadline_seconds=max(30, int(timeout_seconds)),
                ttl_seconds_after_finished=self.ttl_seconds,
            ),
        )

    @staticmethod
    def _assert_adoptable(resource: Any, *, spec_digest: str, kind: str) -> None:
        annotations = getattr(getattr(resource, "metadata", None), "annotations", None) or {}
        if annotations.get(SPEC_DIGEST_ANNOTATION) != spec_digest:
            raise RuntimeError(f"Existing Kubernetes {kind} has a different SCENE spec digest.")

    def create_or_adopt(self, *, secret: Any, job: Any, spec_digest: str) -> None:
        try:
            self.core_api.create_namespaced_secret(self.namespace, secret)
        except ApiException as exc:
            if exc.status != 409:
                raise
            existing = self.core_api.read_namespaced_secret(secret.metadata.name, self.namespace)
            self._assert_adoptable(existing, spec_digest=spec_digest, kind="Secret")
        try:
            self.batch_api.create_namespaced_job(self.namespace, job)
        except ApiException as exc:
            if exc.status != 409:
                raise
            existing = self.batch_api.read_namespaced_job(job.metadata.name, self.namespace)
            self._assert_adoptable(existing, spec_digest=spec_digest, kind="Job")

    def delete(self, *, job_name: str, secret_name: str) -> bool:
        """Request cleanup and report whether every execution resource is absent."""
        body = client.V1DeleteOptions(propagation_policy="Foreground")
        if job_name:
            try:
                self.batch_api.delete_namespaced_job(job_name, self.namespace, body=body)
            except ApiException as exc:
                if exc.status != 404:
                    raise

            try:
                self.batch_api.read_namespaced_job(job_name, self.namespace)
                return False
            except ApiException as exc:
                if exc.status != 404:
                    raise

            pods = self.core_api.list_namespaced_pod(
                self.namespace,
                label_selector=f"job-name={job_name}",
            ).items
            if pods:
                return False

        if not secret_name:
            return True
        try:
            self.core_api.delete_namespaced_secret(secret_name, self.namespace, body=body)
        except ApiException as exc:
            if exc.status != 404:
                raise

        try:
            self.core_api.read_namespaced_secret(secret_name, self.namespace)
            return False
        except ApiException as exc:
            if exc.status != 404:
                raise
        return True

    @staticmethod
    def _pod_failure(pod: Any) -> Optional[KubernetesExecutionStatus]:
        status = getattr(pod, "status", None)
        pod_name = getattr(getattr(pod, "metadata", None), "name", None)
        if getattr(status, "reason", None) == "Evicted":
            return KubernetesExecutionStatus("evicted", "Evicted", status.message, pod_name)
        for condition in getattr(status, "conditions", None) or []:
            if (
                getattr(condition, "type", None) == "PodScheduled"
                and getattr(condition, "status", None) == "False"
                and getattr(condition, "reason", None) == "Unschedulable"
            ):
                return KubernetesExecutionStatus(
                    "unschedulable",
                    "Unschedulable",
                    getattr(condition, "message", None),
                    pod_name,
                )
        for container_status in getattr(status, "container_statuses", None) or []:
            state = getattr(container_status, "state", None)
            waiting = getattr(state, "waiting", None)
            if waiting and getattr(waiting, "reason", None) in {
                "ErrImagePull",
                "ImagePullBackOff",
                "InvalidImageName",
            }:
                return KubernetesExecutionStatus(
                    "image_pull",
                    getattr(waiting, "reason", None),
                    getattr(waiting, "message", None),
                    pod_name,
                )
            terminated = getattr(state, "terminated", None)
            if terminated:
                reason = getattr(terminated, "reason", None)
                exit_code = getattr(terminated, "exit_code", None)
                if reason == "OOMKilled":
                    return KubernetesExecutionStatus("oom_killed", reason, None, pod_name, exit_code)
                if exit_code not in {None, 0}:
                    return KubernetesExecutionStatus("failed", reason, None, pod_name, exit_code)
        return None

    def status(self, job_name: str) -> KubernetesExecutionStatus:
        try:
            job = self.batch_api.read_namespaced_job(job_name, self.namespace)
        except ApiException as exc:
            if exc.status == 404:
                return KubernetesExecutionStatus("missing", "JobNotFound")
            raise
        pods = self.core_api.list_namespaced_pod(
            self.namespace,
            label_selector=f"job-name={job_name}",
        ).items
        for pod in pods:
            failure = self._pod_failure(pod)
            if failure:
                return failure
        conditions = getattr(getattr(job, "status", None), "conditions", None) or []
        for condition in conditions:
            if getattr(condition, "status", None) != "True":
                continue
            if getattr(condition, "type", None) == "Complete":
                pod_name = getattr(getattr(pods[0], "metadata", None), "name", None) if pods else None
                return KubernetesExecutionStatus("succeeded", pod_name=pod_name)
            if getattr(condition, "type", None) == "Failed":
                reason = getattr(condition, "reason", None)
                phase = "deadline_exceeded" if reason == "DeadlineExceeded" else "failed"
                return KubernetesExecutionStatus(
                    phase,
                    reason,
                    getattr(condition, "message", None),
                )
        if int(getattr(getattr(job, "status", None), "active", 0) or 0) > 0:
            pod_name = getattr(getattr(pods[0], "metadata", None), "name", None) if pods else None
            return KubernetesExecutionStatus("active", pod_name=pod_name)
        return KubernetesExecutionStatus("pending")

    def logs(self, pod_name: str, *, tail_lines: int = 500) -> str:
        return str(
            self.core_api.read_namespaced_pod_log(
                pod_name,
                self.namespace,
                tail_lines=max(1, min(int(tail_lines), 5000)),
                timestamps=True,
            )
        )

    def probe_permissions(self) -> Dict[str, bool]:
        checks: Dict[str, bool] = {}
        for verb, resource, group, subresource in [
            ("create", "jobs", "batch", None),
            ("get", "jobs", "batch", None),
            ("delete", "jobs", "batch", None),
            ("create", "secrets", "", None),
            ("get", "secrets", "", None),
            ("delete", "secrets", "", None),
            ("get", "pods", "", None),
            ("list", "pods", "", None),
            ("get", "pods", "", "log"),
        ]:
            review = client.V1SelfSubjectAccessReview(
                spec=client.V1SelfSubjectAccessReviewSpec(
                    resource_attributes=client.V1ResourceAttributes(
                        namespace=self.namespace,
                        verb=verb,
                        resource=resource,
                        group=group,
                        subresource=subresource,
                    )
                )
            )
            response = self.auth_api.create_self_subject_access_review(review)
            resource_name = f"{resource}/{subresource}" if subresource else resource
            checks[f"{verb}:{group or 'core'}:{resource_name}"] = bool(
                response.status.allowed
            )
        return checks
