from __future__ import annotations

from types import SimpleNamespace

import pytest
from kubernetes.client.exceptions import ApiException

from app.services.kubernetes_runner import (
    SPEC_DIGEST_ANNOTATION,
    KubernetesRunnerClient,
)


class FakeBatchApi:
    def __init__(self, *, defer_delete: bool = False) -> None:
        self.jobs = {}
        self.deleted = []
        self.delete_bodies = []
        self.defer_delete = defer_delete

    def create_namespaced_job(self, namespace, job):
        if job.metadata.name in self.jobs:
            raise ApiException(status=409)
        self.jobs[job.metadata.name] = job

    def read_namespaced_job(self, name, namespace):
        if name not in self.jobs:
            raise ApiException(status=404)
        return self.jobs[name]

    def delete_namespaced_job(self, name, namespace, body):
        if name not in self.jobs:
            raise ApiException(status=404)
        self.deleted.append(name)
        self.delete_bodies.append(body)
        if not self.defer_delete:
            del self.jobs[name]


class FakeCoreApi:
    def __init__(self) -> None:
        self.secrets = {}
        self.pods = []
        self.deleted = []

    def create_namespaced_secret(self, namespace, secret):
        if secret.metadata.name in self.secrets:
            raise ApiException(status=409)
        self.secrets[secret.metadata.name] = secret

    def read_namespaced_secret(self, name, namespace):
        if name not in self.secrets:
            raise ApiException(status=404)
        return self.secrets[name]

    def delete_namespaced_secret(self, name, namespace, body):
        if name not in self.secrets:
            raise ApiException(status=404)
        self.deleted.append(name)
        del self.secrets[name]

    def list_namespaced_pod(self, namespace, label_selector):
        return SimpleNamespace(items=self.pods)


class FakeAuthApi:
    def __init__(self, *, denied_resource: str | None = None) -> None:
        self.denied_resource = denied_resource

    def create_self_subject_access_review(self, review):
        attributes = review.spec.resource_attributes
        resource = (
            f"{attributes.resource}/{attributes.subresource}"
            if attributes.subresource
            else attributes.resource
        )
        return SimpleNamespace(
            status=SimpleNamespace(allowed=resource != self.denied_resource)
        )


def _runner(batch=None, core=None, auth=None) -> KubernetesRunnerClient:
    return KubernetesRunnerClient(
        namespace="scene",
        image="registry.example/scene-runner@sha256:" + "a" * 64,
        service_account="scene-runner",
        image_pull_policy="IfNotPresent",
        ttl_seconds=3600,
        cpu_request="500m",
        cpu_limit="2",
        memory_request="512Mi",
        memory_limit="2Gi",
        batch_api=batch or FakeBatchApi(),
        core_api=core or FakeCoreApi(),
        auth_api=auth or FakeAuthApi(),
        load_config=False,
    )


@pytest.mark.unit
def test_job_is_restricted_and_has_no_aws_credentials() -> None:
    runner = _runner()
    job = runner.build_job(
        name="scene-exec-abc-g1",
        secret_name="scene-exec-abc-g1",
        execution_id="execution-1",
        run_id="run-1",
        generation=1,
        callback_url="http://scene.scene.svc.cluster.local/api/executions/execution-1/complete",
        timeout_seconds=900,
        spec_digest="digest",
    )
    pod = job.spec.template.spec
    container = pod.containers[0]
    env_names = {entry.name for entry in container.env}

    assert job.spec.backoff_limit == 0
    assert job.spec.active_deadline_seconds == 900
    assert job.spec.ttl_seconds_after_finished == 3600
    assert pod.restart_policy == "Never"
    assert pod.service_account_name == "scene-runner"
    assert pod.automount_service_account_token is False
    assert pod.node_selector == {"kubernetes.io/arch": "amd64"}
    assert pod.security_context.run_as_non_root is True
    assert pod.security_context.run_as_user == 1000
    assert pod.security_context.run_as_group == 1000
    assert pod.security_context.fs_group == 1000
    assert pod.security_context.fs_group_change_policy == "OnRootMismatch"
    assert not any(name.startswith("AWS_") for name in env_names)
    assert container.env[1].value_from.secret_key_ref.key == "callback-token"
    assert container.security_context.read_only_root_filesystem
    assert container.security_context.run_as_non_root is True
    assert container.security_context.run_as_user == 1000
    assert container.security_context.run_as_group == 1000
    config_volume = next(volume for volume in pod.volumes if volume.name == "config")
    assert config_volume.secret.default_mode == 0o440
    assert next(volume for volume in pod.volumes if volume.name == "dev-shm").empty_dir.medium == "Memory"


@pytest.mark.unit
def test_create_or_adopt_is_idempotent_and_digest_guarded() -> None:
    batch = FakeBatchApi()
    core = FakeCoreApi()
    runner = _runner(batch, core)
    payload = {"url": "https://example.test", "browser": "chromium"}
    digest = runner.spec_digest(payload)
    secret = runner.build_secret(
        name="scene-exec-abc-g1",
        execution_id="execution-1",
        generation=1,
        runner_config=payload,
        callback_token="secret-token",
        spec_digest=digest,
    )
    job = runner.build_job(
        name="scene-exec-abc-g1",
        secret_name="scene-exec-abc-g1",
        execution_id="execution-1",
        run_id="run-1",
        generation=1,
        callback_url="http://scene/api/executions/execution-1/complete",
        timeout_seconds=60,
        spec_digest=digest,
    )

    runner.create_or_adopt(secret=secret, job=job, spec_digest=digest)
    runner.create_or_adopt(secret=secret, job=job, spec_digest=digest)

    assert len(batch.jobs) == 1
    assert len(core.secrets) == 1
    assert core.secrets[secret.metadata.name].string_data["callback-token"] == "secret-token"
    batch.jobs[job.metadata.name].metadata.annotations[SPEC_DIGEST_ANNOTATION] = "other"
    with pytest.raises(RuntimeError, match="different SCENE spec digest"):
        runner.create_or_adopt(secret=secret, job=job, spec_digest=digest)


@pytest.mark.unit
def test_status_classifies_image_pull_oom_and_completion() -> None:
    batch = FakeBatchApi()
    core = FakeCoreApi()
    runner = _runner(batch, core)
    job = SimpleNamespace(
        metadata=SimpleNamespace(name="job"),
        status=SimpleNamespace(active=1, conditions=[]),
    )
    batch.jobs["job"] = job
    core.pods = [
        SimpleNamespace(
            metadata=SimpleNamespace(name="pod"),
            status=SimpleNamespace(
                reason=None,
                conditions=[],
                container_statuses=[
                    SimpleNamespace(
                        state=SimpleNamespace(
                            waiting=SimpleNamespace(reason="ImagePullBackOff", message="not found"),
                            terminated=None,
                        )
                    )
                ],
            ),
        )
    ]
    assert runner.status("job").phase == "image_pull"

    core.pods[0].status.container_statuses[0].state = SimpleNamespace(
        waiting=None,
        terminated=SimpleNamespace(reason="OOMKilled", exit_code=137),
    )
    assert runner.status("job").phase == "oom_killed"

    core.pods = []
    job.status = SimpleNamespace(
        active=0,
        conditions=[SimpleNamespace(type="Complete", status="True", reason=None, message=None)],
    )
    assert runner.status("job").phase == "succeeded"


@pytest.mark.unit
def test_delete_removes_owned_job_and_secret_and_tolerates_replay() -> None:
    batch = FakeBatchApi()
    core = FakeCoreApi()
    runner = _runner(batch, core)
    batch.jobs["job"] = SimpleNamespace()
    core.secrets["secret"] = SimpleNamespace()

    assert runner.delete(job_name="job", secret_name="secret") is True
    assert runner.delete(job_name="job", secret_name="secret") is True

    assert batch.deleted == ["job"]
    assert batch.delete_bodies[0].propagation_policy == "Foreground"
    assert core.deleted == ["secret"]


@pytest.mark.unit
def test_delete_waits_for_job_and_pod_absence_before_removing_secret() -> None:
    batch = FakeBatchApi(defer_delete=True)
    core = FakeCoreApi()
    runner = _runner(batch, core)
    batch.jobs["job"] = SimpleNamespace()
    core.secrets["secret"] = SimpleNamespace()
    core.pods = [SimpleNamespace(metadata=SimpleNamespace(name="pod"))]

    assert runner.delete(job_name="job", secret_name="secret") is False
    assert "secret" in core.secrets

    del batch.jobs["job"]
    assert runner.delete(job_name="job", secret_name="secret") is False
    assert "secret" in core.secrets

    core.pods = []
    assert runner.delete(job_name="job", secret_name="secret") is True
    assert "secret" not in core.secrets


@pytest.mark.unit
def test_permission_probe_covers_every_dispatcher_operation() -> None:
    runner = _runner(auth=FakeAuthApi(denied_resource="pods/log"))

    permissions = runner.probe_permissions()

    assert set(permissions) == {
        "create:batch:jobs",
        "get:batch:jobs",
        "delete:batch:jobs",
        "create:core:secrets",
        "get:core:secrets",
        "delete:core:secrets",
        "get:core:pods",
        "list:core:pods",
        "get:core:pods/log",
    }
    assert permissions["get:core:pods/log"] is False
    assert all(
        allowed
        for operation, allowed in permissions.items()
        if operation != "get:core:pods/log"
    )
