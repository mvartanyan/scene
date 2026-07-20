from __future__ import annotations

import logging
import os
import signal
import socket
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Optional

from app.services.artifacts import get_artifact_store
from app.services.kubernetes_runner import KubernetesExecutionStatus, KubernetesRunnerClient
from app.services.orchestrator import RunOrchestrator
from app.services.runner_backend import validate_runner_runtime_config
from app.services.storage import SceneRepository, get_repository

LOGGER = logging.getLogger("scene.dispatcher")


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class KubernetesDispatcher:
    """Leader-elected reconciliation loop for durable Playwright Jobs."""

    def __init__(
        self,
        *,
        repo: Optional[SceneRepository] = None,
        orchestrator: Optional[RunOrchestrator] = None,
        runner: Optional[KubernetesRunnerClient] = None,
        owner: Optional[str] = None,
        lease_seconds: int = 30,
        callback_grace_seconds: int = 30,
    ) -> None:
        self.repo = repo or get_repository()
        self.orchestrator = orchestrator or RunOrchestrator(
            repo=self.repo,
            artifacts=get_artifact_store(),
            auto_start=False,
        )
        runtime = self.orchestrator.runner_runtime
        report = validate_runner_runtime_config(runtime)
        if runtime.backend != "k3s":
            raise RuntimeError("The Kubernetes dispatcher requires SCENE_RUNNER_BACKEND=k3s.")
        if not report.ok:
            messages = "; ".join(issue.message for issue in report.issues if issue.level == "error")
            raise RuntimeError(f"Kubernetes dispatcher configuration is invalid: {messages}")
        self.runner = runner or KubernetesRunnerClient(
            namespace=runtime.k3s_namespace,
            image=runtime.image,
            service_account=runtime.k3s_runner_service_account,
            image_pull_policy=runtime.k3s_image_pull_policy,
            ttl_seconds=runtime.k3s_job_ttl_seconds,
            cpu_request=runtime.cpu_request,
            cpu_limit=runtime.cpu_limit,
            memory_request=runtime.memory_request,
            memory_limit=runtime.memory_limit,
        )
        self.owner = owner or f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
        self.lease_seconds = max(10, int(lease_seconds))
        self.callback_grace_seconds = max(5, int(callback_grace_seconds))
        self.max_concurrency = max(1, int(runtime.max_concurrency))
        self._capabilities_checked_at = 0.0
        self._capabilities_ok = False

    def _publish_capabilities(self, *, force: bool = False) -> bool:
        now = time.monotonic()
        if not force and now - self._capabilities_checked_at < 30.0:
            return self._capabilities_ok
        try:
            capabilities = self.runner.probe_permissions()
        except Exception:  # noqa: BLE001
            LOGGER.exception("Kubernetes dispatcher permission probe failed")
            capabilities = {"self_subject_access_review": False}
        self._capabilities_checked_at = now
        self._capabilities_ok = bool(capabilities) and all(capabilities.values())
        reported = self.repo.report_dispatcher_health(
            self.owner,
            capabilities=capabilities,
        )
        return reported and self._capabilities_ok

    def _logs(self, status: KubernetesExecutionStatus) -> Optional[str]:
        if not status.pod_name:
            return None
        try:
            return self.runner.logs(status.pod_name)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Unable to collect logs from pod %s: %s", status.pod_name, exc)
            return None

    def _cancel(self, execution: Dict[str, object]) -> bool:
        if not self._confirm_execution_cleanup(execution):
            return False
        return self.orchestrator.finalize_k3s_failure(
            str(execution["id"]),
            message=str(execution.get("message") or "Execution cancelled"),
            cancelled=True,
        )

    def _confirm_execution_cleanup(self, execution: Dict[str, object]) -> bool:
        if execution.get("kubernetes_cleaned_at"):
            return True
        job_name = str(execution.get("kubernetes_job_name") or "")
        secret_name = str(execution.get("kubernetes_secret_name") or "")
        if (job_name or secret_name) and not self.runner.delete(
            job_name=job_name,
            secret_name=secret_name,
        ):
            if not execution.get("kubernetes_cleanup_requested_at"):
                self.repo.update_execution(
                    str(execution["id"]),
                    {"kubernetes_cleanup_requested_at": _utcnow()},
                )
            return False
        self.repo.update_execution(
            str(execution["id"]),
            {"kubernetes_cleaned_at": _utcnow()},
        )
        return True

    def _failure_message(self, status: KubernetesExecutionStatus) -> str:
        labels = {
            "deadline_exceeded": "Kubernetes Job exceeded its execution deadline",
            "image_pull": "Runner image could not be pulled",
            "oom_killed": "Runner container exceeded its memory limit",
            "evicted": "Runner pod was evicted",
            "unschedulable": "Runner pod could not be scheduled",
            "missing": "Runner Job disappeared before completion",
            "failed": "Runner Job failed",
        }
        message = labels.get(status.phase, "Runner Job failed")
        details = status.message or status.reason
        return f"{message}: {details}" if details else message

    def _finalize_terminal_failure(
        self,
        execution: Dict[str, object],
        status: KubernetesExecutionStatus,
    ) -> str:
        if status.phase == "succeeded":
            message = "Runner Job completed but neither callback nor S3 result could be recovered"
        else:
            message = self._failure_message(status)
        if not self._confirm_execution_cleanup(execution):
            return "cleanup_pending"
        self.orchestrator.finalize_k3s_failure(
            str(execution["id"]),
            message=message,
        )
        return status.phase

    def _timeout_reason(self, execution: Dict[str, object]) -> Optional[str]:
        run = self.repo.get_run(str(execution.get("run_id") or ""))
        if not run:
            return "Execution run record is missing"
        deadline = run.get("timeout_deadline")
        if deadline is None:
            return None
        try:
            expired = time.time() >= float(deadline)
        except (TypeError, ValueError):
            return "Run has an invalid timeout deadline"
        return "Run timed out before execution completed" if expired else None

    @staticmethod
    def _remembered_terminal_status(
        execution: Dict[str, object],
        observed: KubernetesExecutionStatus,
    ) -> KubernetesExecutionStatus:
        phase = str(execution.get("kubernetes_terminal_phase") or observed.phase)
        return KubernetesExecutionStatus(
            phase=phase,
            reason=str(execution.get("kubernetes_terminal_reason") or observed.reason or "") or None,
            message=str(execution.get("kubernetes_terminal_message") or observed.message or "") or None,
            pod_name=str(execution.get("kubernetes_terminal_pod_name") or observed.pod_name or "") or None,
            exit_code=observed.exit_code,
        )

    def _recover_terminal_result(
        self,
        execution: Dict[str, object],
        status: KubernetesExecutionStatus,
    ) -> tuple[str, KubernetesExecutionStatus]:
        execution_id = str(execution["id"])
        try:
            if self.orchestrator.recover_durable_result(execution_id):
                return "recovered", status
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Durable result recovery failed for %s: %s", execution_id, exc)

        remembered = self._remembered_terminal_status(execution, status)
        started_at = str(
            execution.get("result_recovery_started_at")
            or execution.get("job_succeeded_at")
            or ""
        )
        patch: Dict[str, object] = {}
        if not started_at:
            started_at = _utcnow()
            patch.update(
                {
                    "result_recovery_started_at": started_at,
                    "kubernetes_terminal_phase": status.phase,
                    "kubernetes_terminal_reason": status.reason,
                    "kubernetes_terminal_message": status.message,
                    "kubernetes_terminal_pod_name": status.pod_name,
                }
            )
            if status.phase == "succeeded":
                patch["job_succeeded_at"] = started_at
            remembered = status
        elif not execution.get("result_recovery_started_at"):
            patch["result_recovery_started_at"] = started_at

        if status.pod_name and not execution.get("kubernetes_terminal_log_captured_at"):
            runner_log = self._logs(status)
            if runner_log:
                try:
                    if self.orchestrator.attach_k3s_log(execution_id, runner_log):
                        patch["kubernetes_terminal_log_captured_at"] = _utcnow()
                except Exception as exc:  # noqa: BLE001
                    LOGGER.warning("Unable to persist runner log for %s: %s", execution_id, exc)
        if patch:
            self.repo.update_execution(execution_id, patch)

        try:
            observed_at = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
            age = datetime.now(timezone.utc).timestamp() - observed_at.timestamp()
        except (TypeError, ValueError):
            age = self.callback_grace_seconds
        if age < self.callback_grace_seconds:
            return "waiting", remembered
        return "expired", remembered

    def _reconcile_execution(self, execution: Dict[str, object]) -> str:
        execution_id = str(execution["id"])
        if execution.get("callback_state") in {"accepted", "finalized"}:
            self.orchestrator.finalize_durable_callback(
                execution_id,
                owner=self.owner,
                lease_seconds=max(self.lease_seconds * 4, 120),
            )
            return "callback"
        if execution.get("cancellation_requested_at"):
            self._cancel(execution)
            return "cancelled"
        if (
            execution.get("kubernetes_cleanup_requested_at")
            and execution.get("kubernetes_terminal_phase")
        ):
            remembered = self._remembered_terminal_status(
                execution,
                KubernetesExecutionStatus("failed"),
            )
            recovery, terminal_status = self._recover_terminal_result(execution, remembered)
            if recovery == "recovered":
                return "recovered"
            if recovery == "waiting":
                return "callback_grace"
            return self._finalize_terminal_failure(execution, terminal_status)
        timeout_reason = self._timeout_reason(execution)
        if timeout_reason:
            cancelled = self.repo.request_execution_cancellation(
                execution_id,
                reason=timeout_reason,
            )
            self._cancel(cancelled or execution)
            return "timed_out"
        job_name = str(execution.get("kubernetes_job_name") or "")
        if not job_name:
            self.orchestrator.finalize_k3s_failure(
                execution_id,
                message="Executing record has no Kubernetes Job identity",
            )
            return "failed"
        status = self.runner.status(job_name)
        if status.phase in {"active", "pending"}:
            if execution.get("result_recovery_started_at"):
                self.repo.update_execution(
                    execution_id,
                    {
                        "result_recovery_started_at": "",
                        "kubernetes_terminal_phase": "",
                        "kubernetes_terminal_reason": "",
                        "kubernetes_terminal_message": "",
                        "kubernetes_terminal_pod_name": "",
                        "kubernetes_terminal_log_captured_at": "",
                    },
                )
            self.repo.renew_execution_claim(
                execution_id,
                owner=str(execution.get("dispatch_claim_owner") or self.owner),
                generation=int(execution.get("dispatch_generation") or 0),
                lease_seconds=max(self.lease_seconds * 2, 60),
            )
            return status.phase
        if status.terminal:
            recovery, terminal_status = self._recover_terminal_result(execution, status)
            if recovery == "recovered":
                return "recovered"
            if recovery == "waiting":
                return "callback_grace"
            return self._finalize_terminal_failure(execution, terminal_status)
        self.orchestrator.finalize_k3s_failure(
            execution_id,
            message=self._failure_message(status),
            runner_log=self._logs(status),
        )
        return status.phase

    def _dispatch(self, execution: Dict[str, object]) -> bool:
        execution_id = str(execution["id"])
        timeout_reason = self._timeout_reason(execution)
        if timeout_reason:
            cancelled = self.repo.request_execution_cancellation(
                execution_id,
                reason=timeout_reason,
            )
            self._cancel(cancelled or execution)
            return False
        claimed = self.repo.claim_execution(
            execution_id,
            owner=self.owner,
            lease_seconds=max(self.lease_seconds * 2, 60),
        )
        if not claimed:
            return False
        job_name = str(claimed["kubernetes_job_name"])
        secret_name = str(claimed["kubernetes_secret_name"])
        generation = int(claimed["dispatch_generation"])
        if claimed.get("callback_token_sha256") and claimed.get("runner_spec_digest"):
            existing_status = self.runner.status(job_name)
            if existing_status.phase != "missing":
                return bool(
                    self.repo.mark_execution_dispatched(
                        execution_id,
                        owner=self.owner,
                        generation=generation,
                    )
                )
            if not self.runner.delete(job_name=job_name, secret_name=secret_name):
                return False
        prepared = self.orchestrator.prepare_k3s_dispatch(execution_id, owner=self.owner)
        runner_config = prepared["runner_config"]
        spec_digest = str(prepared["spec_digest"])
        secret = self.runner.build_secret(
            name=secret_name,
            execution_id=execution_id,
            generation=generation,
            runner_config=runner_config,
            callback_token=str(prepared["callback_token"]),
            spec_digest=spec_digest,
        )
        job = self.runner.build_job(
            name=job_name,
            secret_name=secret_name,
            execution_id=execution_id,
            run_id=str(claimed["run_id"]),
            generation=generation,
            callback_url=str(prepared["callback_url"]),
            timeout_seconds=int(prepared["timeout_seconds"]),
            spec_digest=spec_digest,
        )
        self.runner.create_or_adopt(secret=secret, job=job, spec_digest=spec_digest)
        dispatched = self.repo.mark_execution_dispatched(
            execution_id,
            owner=self.owner,
            generation=generation,
        )
        return dispatched is not None

    def _cleanup_terminal(self) -> int:
        cleaned = 0
        for execution in self.repo.list_cleanup_pending_executions(
            limit=100,
        ):
            try:
                job_name = str(execution.get("kubernetes_job_name") or "")
                status = (
                    self.runner.status(job_name)
                    if job_name
                    else KubernetesExecutionStatus("missing", "JobNotCreated")
                )
                runner_log = self._logs(status)
                if runner_log:
                    self.orchestrator.attach_k3s_log(str(execution["id"]), runner_log)
                cleanup_confirmed = self._confirm_execution_cleanup(execution)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Kubernetes cleanup failed for %s: %s", execution["id"], exc)
                continue
            if not cleanup_confirmed:
                continue
            cleaned += 1
        return cleaned

    def run_once(self) -> Dict[str, object]:
        stats: Dict[str, object] = {
            "leader": False,
            "owner": self.owner,
            "capabilities_ok": False,
            "dispatched": 0,
            "reconciled": 0,
            "callbacks_finalized": 0,
            "cleaned": 0,
        }
        if not self.repo.acquire_dispatcher_lease(
            self.owner,
            lease_seconds=self.lease_seconds,
        ):
            return stats
        stats["leader"] = True
        stats["capabilities_ok"] = self._publish_capabilities()
        if not stats["capabilities_ok"]:
            return stats
        for execution in self.repo.list_pending_callback_finalizations(limit=500):
            try:
                if self.orchestrator.finalize_durable_callback(
                    str(execution["id"]),
                    owner=self.owner,
                    lease_seconds=max(self.lease_seconds * 4, 120),
                ):
                    stats["callbacks_finalized"] = int(stats["callbacks_finalized"]) + 1
            except Exception:  # noqa: BLE001
                LOGGER.exception("Callback finalization failed for execution %s", execution["id"])
        active = self.repo.list_execution_records_bounded(
            statuses={"queued", "executing"},
            limit=500,
        )
        for execution in active:
            if (
                execution.get("status") == "executing"
                or execution.get("cancellation_requested_at")
                or self._timeout_reason(execution)
            ):
                self._reconcile_execution(execution)
                stats["reconciled"] = int(stats["reconciled"]) + 1
        refreshed = self.repo.list_execution_records_bounded(
            statuses={"executing"},
            limit=500,
        )
        available = max(0, self.max_concurrency - len(refreshed))
        for execution in self.repo.list_dispatchable_executions(limit=available):
            try:
                if self._dispatch(execution):
                    stats["dispatched"] = int(stats["dispatched"]) + 1
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("Dispatch failed for execution %s", execution["id"])
                self.repo.update_execution(
                    str(execution["id"]),
                    {
                        "message": f"Kubernetes dispatch attempt failed: {type(exc).__name__}: {exc}",
                        "dispatch_lease_expires_at": _utcnow(),
                    },
                )
        stats["cleaned"] = self._cleanup_terminal()
        return stats

    def run_forever(self, *, poll_seconds: float = 2.0, stop_event: Optional[threading.Event] = None) -> None:
        stop = stop_event or threading.Event()
        while not stop.is_set():
            try:
                stats = self.run_once()
                LOGGER.info("dispatcher_cycle %s", stats)
            except Exception:  # noqa: BLE001
                LOGGER.exception("Dispatcher reconciliation cycle failed")
            stop.wait(max(0.25, float(poll_seconds)))
        self.repo.release_dispatcher_lease(self.owner)


def main() -> int:
    logging.basicConfig(
        level=os.environ.get("SCENE_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    dispatcher = KubernetesDispatcher()
    stop = threading.Event()

    def request_stop(_signum, _frame) -> None:
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    dispatcher.run_forever(
        poll_seconds=float(os.environ.get("SCENE_DISPATCH_POLL_SECONDS", "2")),
        stop_event=stop,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
