from __future__ import annotations
import json
import logging
import os
import queue
import shutil
import threading
import time
import secrets
import subprocess
import urllib.error
import urllib.request
from collections import deque
import socket
from urllib.parse import urlparse
try:
    import docker  # type: ignore
    from docker.errors import APIError, ImageNotFound, NotFound
except ModuleNotFoundError:  # pragma: no cover - fallback when SDK unavailable
    docker = None  # type: ignore
    APIError = Exception  # type: ignore
    ImageNotFound = Exception  # type: ignore
    NotFound = Exception  # type: ignore
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from PIL import Image

from app.schemas import (
    ArtifactKind,
    BaselineStatus,
    ExecutionStatus,
    RunPurpose,
    RunStatus,
)
from app.services.artifacts import ArtifactStore, get_artifact_store
from app.services.runner_backend import (
    DEFAULT_RUNNER_IMAGE,
    SCENE_RUNNER_IMAGE_ENV,
    RunnerReadinessReport,
    load_runner_runtime_config,
    validate_runner_runtime_config,
)
from app.services.storage import SceneRepository, get_repository

LOGGER = logging.getLogger("scene.orchestrator")


def _utcnow() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())


def _clean_js(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped or stripped.lower() == "none":
            return None
        return stripped
    return value  # type: ignore[return-value]


def _clean_actions(value: Optional[object]) -> List[Dict[str, object]]:
    if not value:
        return []
    cleaned: List[Dict[str, object]] = []
    if not isinstance(value, list):
        return cleaned
    for item in value:
        if not isinstance(item, dict):
            continue
        action_type = item.get("type")
        if not isinstance(action_type, str) or not action_type.strip():
            continue
        cleaned.append({str(k): v for k, v in item.items()})
    return cleaned


RUNNER_SCRIPT_PATH = Path(__file__).resolve().parent / "runner_script.py"
try:
    RUNNER_SCRIPT = RUNNER_SCRIPT_PATH.read_text(encoding="utf-8")
except FileNotFoundError as exc:  # pragma: no cover - import-time guard
    raise RuntimeError(f"Runner script file missing: {RUNNER_SCRIPT_PATH}") from exc


@dataclass
class RunnerResult:
    success: bool
    screenshot: Optional[Path] = None
    reference: Optional[Path] = None
    trace: Optional[Path] = None
    video: Optional[Path] = None
    log: Optional[Path] = None
    message: Optional[str] = None
    timed_out: bool = False


@dataclass
class ExecutionContext:
    run_id: str
    execution_id: str
    container_handle: object
    token: str
    log_stop: threading.Event
    log_thread: threading.Thread
    started_at: float
    deadline: Optional[float]
    result_payload: Optional[Dict[str, object]] = None
    watchdog_marked: bool = False
    exit_code: Optional[int] = None


class DockerPlaywrightRunner:
    """Launch Playwright inside Docker and gather primary artifacts."""

    def __init__(
        self,
        image: Optional[str] = None,
        timeout: int = 180,
    ) -> None:
        self._image = image or os.environ.get(SCENE_RUNNER_IMAGE_ENV, DEFAULT_RUNNER_IMAGE)
        self._timeout = timeout

    @property
    def timeout(self) -> int:
        return self._timeout

    @property
    def image(self) -> str:
        return self._image

    def prepare_workspace(self, config: Dict[str, object], workdir: Path) -> None:
        workdir.mkdir(parents=True, exist_ok=True)
        script_path = workdir / "runner.py"
        config_path = workdir / "config.json"
        script_path.write_text(RUNNER_SCRIPT, encoding="utf-8")
        config_with_artifacts = dict(config)
        config_with_artifacts["artifacts_dir"] = "."
        config_path.write_text(json.dumps(config_with_artifacts), encoding="utf-8")


class DockerContainerHandleProtocol:
    id: str

    def logs(self, stream: bool = True, follow: bool = True):  # pragma: no cover - interface stub
        raise NotImplementedError

    def kill(self) -> None:  # pragma: no cover - interface stub
        raise NotImplementedError

    def remove(self, force: bool = False) -> None:  # pragma: no cover - interface stub
        raise NotImplementedError

    def status(self) -> str:  # pragma: no cover - interface stub
        raise NotImplementedError

    def exit_code(self) -> Optional[int]:  # pragma: no cover - interface stub
        raise NotImplementedError


class SDKContainerHandle(DockerContainerHandleProtocol):
    def __init__(self, container):
        self._container = container
        self.id = container.id

    def logs(self, stream: bool = True, follow: bool = True):
        return self._container.logs(stream=stream, follow=follow)

    def kill(self) -> None:
        try:
            self._container.kill()
        except APIError as exc:  # pragma: no cover - defensive
            LOGGER.warning("Failed to kill container %s: %s", self.id, exc)

    def remove(self, force: bool = False) -> None:
        try:
            self._container.remove(force=force)
        except APIError as exc:  # pragma: no cover - defensive
            LOGGER.warning("Failed to remove container %s: %s", self.id, exc)

    def status(self) -> str:
        try:
            self._container.reload()
            return getattr(self._container, "status", "unknown")
        except NotFound:  # pragma: no cover - container auto-removed
            return "not_found"
        except APIError as exc:  # pragma: no cover - defensive
            LOGGER.debug("Failed to poll container %s status: %s", self.id, exc)
            return "unknown"

    def exit_code(self) -> Optional[int]:
        try:
            self._container.reload()
            state = getattr(self._container, "attrs", {}).get("State", {})
            code = state.get("ExitCode")
            return int(code) if code is not None else None
        except (APIError, ValueError, TypeError) as exc:  # pragma: no cover - defensive
            LOGGER.debug("Failed to resolve exit code for container %s: %s", self.id, exc)
            return None


class CLIContainerHandle(DockerContainerHandleProtocol):
    def __init__(self, container_id: str):
        self.id = container_id

    def logs(self, stream: bool = True, follow: bool = True):
        args = ["docker", "logs"]
        if stream and follow:
            args.append("-f")
        args.append(self.id)
        if stream:
            proc = subprocess.Popen(
                args,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            try:
                if proc.stdout is None:
                    return
                for line in iter(proc.stdout.readline, b""):
                    if not line:
                        break
                    yield line
            finally:
                if proc.stdout:
                    proc.stdout.close()
                proc.wait()
        else:
            proc = subprocess.run(
                args,
                capture_output=True,
                text=False,
                check=False,
            )
            if proc.stdout:
                return proc.stdout
            return b""

    def kill(self) -> None:
        subprocess.run(
            ["docker", "kill", self.id],
            capture_output=True,
            text=True,
            check=False,
        )

    def remove(self, force: bool = False) -> None:
        cmd = ["docker", "rm"]
        if force:
            cmd.append("-f")
        cmd.append(self.id)
        subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )

    def status(self) -> str:
        proc = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Status}}", self.id],
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            stderr = (proc.stderr or "").lower()
            if "no such container" in stderr or "not found" in stderr:
                return "not_found"
            return "unknown"
        return proc.stdout.strip() or "unknown"

    def exit_code(self) -> Optional[int]:
        proc = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.ExitCode}}", self.id],
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            return None
        output = proc.stdout.strip()
        try:
            return int(output)
        except ValueError:
            return None


class DockerBackend:
    def run_container(
        self,
        image: str,
        command: List[str],
        *,
        environment: Dict[str, str],
        volumes: Dict[str, Dict[str, str]],
        working_dir: Optional[str],
        shm_size: Optional[str],
        name: Optional[str],
        auto_remove: bool,
        extra_hosts: Optional[Dict[str, str]],
    ) -> DockerContainerHandleProtocol:
        raise NotImplementedError

    def get_container(self, container_id: str) -> DockerContainerHandleProtocol:
        raise NotImplementedError


class DockerSDKBackend(DockerBackend):
    def __init__(self) -> None:
        self._client = docker.from_env()

    def run_container(
        self,
        image: str,
        command: List[str],
        *,
        environment: Dict[str, str],
        volumes: Dict[str, Dict[str, str]],
        working_dir: Optional[str],
        shm_size: Optional[str],
        name: Optional[str],
        auto_remove: bool,
        extra_hosts: Optional[Dict[str, str]],
    ) -> DockerContainerHandleProtocol:
        container = self._client.containers.run(
            image,
            command,
            detach=True,
            name=name,
            environment=environment,
            volumes=volumes,
            working_dir=working_dir,
            shm_size=shm_size,
            auto_remove=auto_remove,
            extra_hosts=extra_hosts or {},
        )
        return SDKContainerHandle(container)

    def get_container(self, container_id: str) -> DockerContainerHandleProtocol:
        container = self._client.containers.get(container_id)
        return SDKContainerHandle(container)


class DockerCLIBackend(DockerBackend):
    def run_container(
        self,
        image: str,
        command: List[str],
        *,
        environment: Dict[str, str],
        volumes: Dict[str, Dict[str, str]],
        working_dir: Optional[str],
        shm_size: Optional[str],
        name: Optional[str],
        auto_remove: bool,
        extra_hosts: Optional[Dict[str, str]],
    ) -> DockerContainerHandleProtocol:
        args = ["docker", "run", "-d"]
        # Do not use --rm so logs remain accessible after the container exits; cleanup happens elsewhere.
        if shm_size:
            args.extend(["--shm-size", shm_size])
        if name:
            args.extend(["--name", name])
        for host, value in (extra_hosts or {}).items():
            args.extend(["--add-host", f"{host}:{value}"])
        for host_path, spec in (volumes or {}).items():
            bind = spec.get("bind")
            mode = spec.get("mode", "rw")
            if bind:
                args.extend(["-v", f"{host_path}:{bind}:{mode}"])
        for key, value in environment.items():
            args.extend(["-e", f"{key}={value}"])
        if working_dir:
            args.extend(["-w", working_dir])
        args.append(image)
        args.extend(command)
        proc = subprocess.run(args, capture_output=True, text=True, check=False)
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip() or "docker run failed")
        container_id = proc.stdout.strip().splitlines()[-1]
        return CLIContainerHandle(container_id)

    def get_container(self, container_id: str) -> DockerContainerHandleProtocol:
        return CLIContainerHandle(container_id)



class DiffEngine:
    """Generate pixel diff overlays between baseline and observed screenshots."""

    def __init__(self, pixel_tolerance: int = 0) -> None:
        self._pixel_tolerance = self._normalize_tolerance(pixel_tolerance)

    @staticmethod
    def _normalize_tolerance(value: object) -> int:
        try:
            return max(0, min(255, int(value)))
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _dimensions(size: Tuple[int, int]) -> Dict[str, int]:
        return {"width": int(size[0]), "height": int(size[1])}

    @staticmethod
    def _has_transparency(image: Image.Image) -> bool:
        if image.mode in {"RGBA", "LA"}:
            alpha = image.getchannel("A")
            return alpha.getextrema()[0] < 255
        if image.mode == "P" and "transparency" in image.info:
            return True
        return False

    @staticmethod
    def _sanitize_transparent_pixels(image: Image.Image) -> Tuple[Image.Image, bool]:
        rgba = image.convert("RGBA")
        changed = False
        sanitized = []
        for red, green, blue, alpha in rgba.getdata():
            if alpha == 0 and (red or green or blue):
                sanitized.append((0, 0, 0, 0))
                changed = True
            else:
                sanitized.append((red, green, blue, alpha))
        if changed:
            rgba.putdata(sanitized)
        return rgba, changed

    def _normalize_image(
        self,
        image: Image.Image,
        target_size: Tuple[int, int],
    ) -> Tuple[Image.Image, str]:
        original_size = image.size
        has_transparency = self._has_transparency(image)
        normalized, sanitized = self._sanitize_transparent_pixels(image)
        actions: List[str] = []
        if sanitized:
            actions.append("sanitize_transparent_pixels")

        if normalized.size != target_size:
            fill = (0, 0, 0, 0)
            if not has_transparency and normalized.width > 0 and normalized.height > 0:
                fill = normalized.getpixel((normalized.width - 1, normalized.height - 1))
            canvas = Image.new("RGBA", target_size, fill)
            canvas.paste(normalized, (0, 0))
            normalized = canvas
            actions.append("pad_to_max_canvas")

        if not actions and normalized.size == original_size:
            actions.append("none")
        return normalized, "+".join(actions)

    def _normalize_pair(
        self,
        baseline_image: Image.Image,
        observed_image: Image.Image,
    ) -> Tuple[Image.Image, Image.Image, Dict[str, object]]:
        baseline_original = baseline_image.size
        observed_original = observed_image.size
        normalized_size = (
            max(baseline_original[0], observed_original[0]),
            max(baseline_original[1], observed_original[1]),
        )
        baseline_normalized, baseline_action = self._normalize_image(baseline_image, normalized_size)
        observed_normalized, observed_action = self._normalize_image(observed_image, normalized_size)
        if baseline_action == "none" and observed_action == "none":
            action = "none"
        else:
            action = "normalize_to_max_canvas"
        stats = {
            "baseline_original_dimensions": self._dimensions(baseline_original),
            "observed_original_dimensions": self._dimensions(observed_original),
            "normalized_dimensions": self._dimensions(normalized_size),
            "normalization_action": action,
            "baseline_normalization_action": baseline_action,
            "observed_normalization_action": observed_action,
        }
        return baseline_normalized, observed_normalized, stats

    def normalize_files(self, first_path: Path, second_path: Path) -> Dict[str, object]:
        with Image.open(first_path) as first_img, Image.open(second_path) as second_img:
            first_normalized, second_normalized, stats = self._normalize_pair(first_img, second_img)
        first_normalized.save(first_path)
        second_normalized.save(second_path)
        return stats

    def generate(
        self,
        baseline_path: Path,
        observed_path: Path,
        diff_path: Path,
        heatmap_path: Path,
        *,
        pixel_tolerance: Optional[int] = None,
    ) -> Dict[str, object]:
        tolerance = self._normalize_tolerance(
            self._pixel_tolerance if pixel_tolerance is None else pixel_tolerance
        )
        with Image.open(baseline_path) as base_img, Image.open(observed_path) as obs_img:
            base_norm, obs_norm, stats = self._normalize_pair(base_img, obs_img)
            delta_values: List[int] = []
            diff_pixels = 0
            for base_pixel, obs_pixel in zip(base_norm.getdata(), obs_norm.getdata()):
                delta = max(abs(int(base_pixel[index]) - int(obs_pixel[index])) for index in range(4))
                if delta <= tolerance:
                    delta_values.append(0)
                    continue
                adjusted = delta - tolerance
                delta_values.append(adjusted)
                diff_pixels += 1

            total_pixels = base_norm.width * base_norm.height
            percentage = (diff_pixels / total_pixels * 100.0) if total_pixels else 0.0

            alpha_values = [min(255, value * 4) for value in delta_values]
            diff_alpha = Image.new("L", base_norm.size)
            diff_alpha.putdata(alpha_values)
            diff_canvas = Image.new("RGBA", base_norm.size, (0, 0, 0, 255))
            diff_overlay = Image.new("RGBA", base_norm.size, (255, 193, 7, 0))
            diff_overlay.putalpha(diff_alpha)
            diff_visual = Image.alpha_composite(diff_canvas, diff_overlay)
            diff_visual.save(diff_path)

            heat_overlay = Image.new("RGBA", base_norm.size, (255, 64, 0, 0))
            heat_overlay.putalpha(diff_alpha)
            heatmap = Image.alpha_composite(obs_norm, heat_overlay)
            heatmap.save(heatmap_path)

        stats.update({
            "pixel_count": diff_pixels,
            "total_pixels": total_pixels,
            "percentage": round(percentage, 4),
            "pixel_tolerance": tolerance,
        })
        return stats


PROJECT_ROOT = Path(__file__).resolve().parents[2]


class RunOrchestrator:
    """Expand runs into Playwright executions and manage artifact persistence."""

    def __init__(
        self,
        repo: Optional[SceneRepository] = None,
        artifacts: Optional[ArtifactStore] = None,
        *,
        auto_start: bool = True,
        docker_backend: Optional[DockerBackend] = None,
    ) -> None:
        self._repo = repo or get_repository()
        self._artifacts = artifacts or get_artifact_store()
        self._queue: "queue.Queue[str]" = queue.Queue()
        self._inflight: set[str] = set()
        self._lock = threading.Lock()
        self._image_lock = threading.Lock()
        self._runner_image_verified = False
        self._cancelled_runs: set[str] = set()
        self._cancelled_executions: set[str] = set()
        config_defaults = self._repo.get_config()
        self._runner_runtime = load_runner_runtime_config(
            config_defaults,
            artifact_root=self._artifacts.root,
        )
        self._runner = DockerPlaywrightRunner(image=self._runner_runtime.image)
        self._docker_backend: Optional[DockerBackend]
        if self._runner_runtime.backend == "docker":
            if docker_backend is not None:
                self._docker_backend = docker_backend
                self._runner_image_verified = True
            elif docker is not None:
                self._docker_backend = DockerSDKBackend()
            else:
                LOGGER.info("Docker SDK not available; falling back to CLI backend")
                self._docker_backend = DockerCLIBackend()
        else:
            self._docker_backend = docker_backend
        self._execution_contexts: Dict[str, ExecutionContext] = {}
        self._execution_tokens: Dict[str, str] = {}
        self._completion_queues: Dict[str, "queue.Queue[Tuple[str, Dict[str, object]]]"] = {}
        self._diff_pixel_tolerance = DiffEngine._normalize_tolerance(
            config_defaults.get("diff_pixel_tolerance", 0)
        )
        self._diffs = DiffEngine(pixel_tolerance=self._diff_pixel_tolerance)
        self._scene_host_url = self._runner_runtime.callback_base_url
        self._max_concurrent = int(self._runner_runtime.max_concurrency)
        self._post_wait_ms = int(config_defaults.get("capture_post_wait_ms", 7000))
        interval_value = config_defaults.get("watchdog_interval_seconds", 5)
        try:
            self._watchdog_interval = max(1.0, float(interval_value))
        except (TypeError, ValueError):
            self._watchdog_interval = 5.0
        self._watchdog_stop = threading.Event()
        self._watchdog: Optional[threading.Thread] = None
        self._worker: Optional[threading.Thread] = None
        if auto_start:
            self._ensure_worker()
            self._ensure_watchdog()

    def _log_path(self, run_id: str, execution_id: str) -> Path:
        return self._artifacts.execution_dir(run_id, execution_id) / "runner.log"

    def _resolve_baseline_record(self, run: Dict[str, object]) -> Optional[Dict[str, object]]:
        baseline_id = run.get("baseline_id")
        if baseline_id:
            return self._repo.get_baseline(baseline_id)
        if run["purpose"] == RunPurpose.comparison.value:
            return self._repo.get_latest_baseline(run["batch_id"])
        return None

    def _find_video(self, execution_dir: Path) -> Optional[Path]:
        videos_dir = execution_dir / "videos"
        if not videos_dir.exists():
            return None
        for child in videos_dir.iterdir():
            if child.is_file():
                return child
        return None

    def _completion_queue(self, run_id: str) -> "queue.Queue[Tuple[str, Dict[str, object]]]":
        with self._lock:
            queue_ref = self._completion_queues.get(run_id)
            if queue_ref is None:
                queue_ref = queue.Queue()
                self._completion_queues[run_id] = queue_ref
            return queue_ref

    def _start_log_stream(
        self,
        run_id: str,
        execution_id: str,
        container: DockerContainerHandleProtocol,
    ) -> tuple[threading.Event, threading.Thread]:
        stop_event = threading.Event()
        log_path = self._log_path(run_id, execution_id)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        def _stream() -> None:
            try:
                for chunk in container.logs(stream=True, follow=True):
                    if chunk:
                        text = chunk.decode("utf-8", errors="ignore")
                        with log_path.open("a", encoding="utf-8") as handle:
                            handle.write(text)
                    if stop_event.is_set():
                        break
            except Exception as exc:
                LOGGER.debug("Log stream for execution %s interrupted: %s", execution_id, exc)

        thread = threading.Thread(target=_stream, name=f"scene-log-{execution_id[:8]}", daemon=True)
        thread.start()
        self._ensure_log_artifact(run_id, execution_id)
        return stop_event, thread

    def _harmonize_reference_dimensions(self, execution_dir: Path) -> None:
        observed_path = execution_dir / "observed.png"
        reference_path = execution_dir / "reference.png"
        if not observed_path.exists() or not reference_path.exists():
            return
        try:
            self._diffs.normalize_files(observed_path, reference_path)
        except Exception as exc:
            LOGGER.debug("Reference dimension normalization failed for %s: %s", execution_dir, exc)
            return

    def _stop_log_stream(self, context: ExecutionContext) -> None:
        context.log_stop.set()
        try:
            context.log_thread.join(timeout=5)
        except RuntimeError:
            pass

    def _cleanup_execution_context(self, execution_id: str) -> None:
        context = self._execution_contexts.pop(execution_id, None)
        if not context:
            return
        self._execution_tokens.pop(execution_id, None)
        try:
            self._stop_log_stream(context)
        except Exception:
            pass
        try:
            context.container_handle.remove(force=True)
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.debug("Container cleanup for %s failed: %s", execution_id, exc)

    def _enqueue_completion(self, run_id: str, execution_id: str, payload: Dict[str, object]) -> None:
        queue_ref = self._completion_queue(run_id)
        queue_ref.put((execution_id, payload))

    def _process_run(self, run_id: str) -> None:
        run = self._repo.get_run(run_id)
        if not run:
            LOGGER.warning("Run %s no longer exists; skipping", run_id)
            return

        LOGGER.info("Starting orchestration for run %s", run_id)

        with self._lock:
            if run_id in self._cancelled_runs:
                LOGGER.info("Run %s was cancelled before start", run_id)
                self._refresh_run_summary(run_id, force_status=RunStatus.cancelled.value)
                return

        batch = self._repo.get_batch(run["batch_id"])
        if not batch:
            self._repo.update_run(
                run_id,
                {
                    "status": RunStatus.failed.value,
                    "note": "Batch missing; cannot execute run.",
                },
            )
            return

        task_records: List[Tuple[Dict[str, object], Dict[str, object]]] = []
        for task_id in batch.get("task_ids", []):
            task = self._repo.get_task(task_id)
            if not task:
                LOGGER.warning("Task %s missing; skipping in run %s", task_id, run_id)
                continue
            page = self._repo.get_page(task.get("page_id"))
            if not page:
                LOGGER.warning("Page %s missing for task %s; skipping", task.get("page_id"), task_id)
                continue
            task_records.append((task, page))

        if not task_records:
            self._repo.update_run(
                run_id,
                {
                    "status": RunStatus.failed.value,
                    "note": "Run has no valid tasks to execute.",
                },
            )
            return

        executions = self._create_execution_matrix(run, task_records)
        config_defaults = self._repo.get_config()
        run_timeout_seconds = run.get("timeout_seconds")
        if run_timeout_seconds is None:
            run_timeout_seconds = int(config_defaults.get("run_timeout_seconds", self._runner.timeout))
        deadline = time.time() + run_timeout_seconds if run_timeout_seconds else None

        summary = {
            "executions_total": len(executions),
            "executions_finished": 0,
            "executions_failed": 0,
            "executions_cancelled": 0,
        }
        started_at = _utcnow()
        update_payload: Dict[str, Any] = {
            "status": RunStatus.executing.value,
            "summary": summary,
            "started_at": started_at,
        }
        if deadline is not None:
            update_payload["timeout_deadline"] = deadline
            update_payload["timeout_deadline_iso"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(deadline))
        self._repo.update_run(run_id, update_payload)
        run.update(update_payload)

        baseline_record = None
        if run["purpose"] == RunPurpose.baseline_recording.value:
            baseline_record = self._repo.create_baseline(
                {
                    "project_id": run["project_id"],
                    "batch_id": run["batch_id"],
                    "run_id": run_id,
                    "status": BaselineStatus.pending.value,
                    "items": [],
                }
            )
            self._repo.update_run(run_id, {"baseline_id": baseline_record["id"]})
        else:
            baseline_id = run.get("baseline_id")
            if baseline_id:
                baseline_record = self._repo.get_baseline(baseline_id)
            else:
                baseline_record = self._repo.get_latest_baseline(run["batch_id"])
                if baseline_record:
                    self._repo.update_run(run_id, {"baseline_id": baseline_record["id"]})

        completion_queue = self._completion_queue(run_id)
        pending: deque[Dict[str, object]] = deque(executions)
        active: Dict[str, ExecutionContext] = {}
        timeout_triggered = False

        while pending or active:
            while pending and len(active) < max(1, self._max_concurrent):
                execution = pending.popleft()
                task_meta = next((pair[0] for pair in task_records if pair[0]["id"] == execution["task_id"]), None)
                page_meta = next((pair[1] for pair in task_records if pair[0]["id"] == execution["task_id"]), None)
                if not task_meta or not page_meta:
                    LOGGER.warning("Execution %s missing task metadata; skipping", execution["id"])
                    continue
                context = self._launch_execution(
                    run=run,
                    execution=execution,
                    task=task_meta,
                    page=page_meta,
                    baseline=baseline_record,
                    deadline=deadline,
                    execution_timeout=run_timeout_seconds,
                )
                if context:
                    active[execution["id"]] = context

            if not active:
                break

            wait_timeout = 1.0
            if deadline is not None:
                wait_timeout = max(min(wait_timeout, deadline - time.time()), 0.0)
                if wait_timeout <= 0:
                    timeout_triggered = True
                    break

            try:
                execution_id, result_data = completion_queue.get(timeout=wait_timeout)
            except queue.Empty:
                continue

            context = active.pop(execution_id, None)
            if not context:
                continue
            payload = result_data if isinstance(result_data, dict) else {}
            self._finalize_from_callback(run, execution_id, payload, baseline_record)
            self._cleanup_execution_context(execution_id)

        if timeout_triggered:
            message = f"Execution timed out after {run_timeout_seconds} seconds"
            for execution_id, context in list(active.items()):
                context.log_stop.set()
                try:
                    context.container_handle.kill()
                except Exception as exc:
                    LOGGER.debug("Failed to kill container for %s during timeout: %s", execution_id, exc)
                timeout_payload = {"status": "timeout", "error": message}
                self._finalize_from_callback(run, execution_id, timeout_payload, baseline_record)
                self._cleanup_execution_context(execution_id)
            active.clear()

        self._completion_queues.pop(run_id, None)

        counts = self._repo.execution_status_counts(run_id)
        with self._lock:
            run_cancelled = run_id in self._cancelled_runs
        if run_cancelled or (
            counts[ExecutionStatus.cancelled.value] > 0
            and counts[ExecutionStatus.executing.value] == 0
            and counts[ExecutionStatus.queued.value] == 0
            and counts[ExecutionStatus.failed.value] == 0
        ):
            final_status = RunStatus.cancelled.value
        elif counts[ExecutionStatus.failed.value] > 0:
            final_status = RunStatus.failed.value
        elif timeout_triggered:
            final_status = RunStatus.cancelled.value
        else:
            final_status = RunStatus.finished.value

        counts = self._refresh_run_summary(run_id, force_status=final_status)

        if baseline_record:
            baseline_status = (
                BaselineStatus.completed.value
                if final_status == RunStatus.finished.value
                else BaselineStatus.failed.value
            )
            self._repo.update_baseline(
                baseline_record["id"],
                {"status": baseline_status},
            )

        with self._lock:
            self._cancelled_runs.discard(run_id)
            for execution in executions:
                self._cancelled_executions.discard(execution["id"])

        LOGGER.info(
            "Completed run %s: %s (finished=%s failed=%s cancelled=%s)",
            run_id,
            final_status,
            counts[ExecutionStatus.finished.value],
            counts[ExecutionStatus.failed.value],
            counts[ExecutionStatus.cancelled.value],
        )

    def _finalize_execution_success(
        self,
        *,
        run: Dict[str, object],
        execution: Dict[str, object],
        baseline: Optional[Dict[str, object]],
        execution_dir: Path,
        result: RunnerResult,
    ) -> None:
        execution_id = execution["id"]
        artifacts = self._artifact_payload(result, execution_dir)
        extra_artifacts: Dict[str, object] = {}

        if run["purpose"] == RunPurpose.baseline_recording.value and baseline:
            baseline_item = self._store_baseline_artifacts(
                baseline_id=baseline["id"],
                execution=execution,
                screenshot=result.screenshot,
            )
            extra_artifacts[ArtifactKind.baseline.value] = baseline_item["artifacts"][ArtifactKind.baseline.value]

        diff_level_value: Optional[float] = None
        if run["purpose"] == RunPurpose.comparison.value:
            diff_summary = self._compare_with_baseline(
                run=run,
                execution=execution,
                execution_dir=execution_dir,
                observed=result.screenshot,
                baseline_record=baseline,
            )
            diff_error = diff_summary.get("error")
            diff_info = diff_summary.get("diff")
            if diff_info:
                diff_percentage = diff_info.get("percentage")
                if diff_percentage is not None:
                    diff_level_value = float(diff_percentage)
                    diff_info["diff_level"] = diff_level_value
                self._repo.update_execution(execution_id, {"diff": diff_info})
            diff_artifacts = diff_summary.get("artifacts") or {}
            if diff_artifacts:
                extra_artifacts.update(diff_artifacts)
            if diff_error:
                artifacts.update(extra_artifacts)
                self._repo.update_execution(
                    execution_id,
                    {
                        "status": ExecutionStatus.failed.value,
                        "completed_at": _utcnow(),
                        "message": str(diff_error),
                        "artifacts": artifacts,
                    },
                )
                self._log_execution_transition(
                    event="execution_finalize",
                    run_id=run["id"],
                    execution_id=execution_id,
                    from_status=str(execution.get("status") or "unknown"),
                    to_status=ExecutionStatus.failed.value,
                    reason="comparison_baseline_unavailable",
                )
                self._append_log(run["id"], execution_id, f"Execution failed: {diff_error}")
                return
        else:
            diff_level_value = 0.0

        self._harmonize_reference_dimensions(execution_dir)
        artifacts = self._artifact_payload(result, execution_dir)
        artifacts.update(extra_artifacts)
        update_payload = {
            "status": ExecutionStatus.finished.value,
            "completed_at": _utcnow(),
            "artifacts": artifacts,
        }
        if diff_level_value is not None:
            update_payload["diff_level"] = diff_level_value
        self._repo.update_execution(execution_id, update_payload)
        self._log_execution_transition(
            event="execution_finalize",
            run_id=run["id"],
            execution_id=execution_id,
            from_status=str(execution.get("status") or "unknown"),
            to_status=ExecutionStatus.finished.value,
            reason="runner_success",
        )
        self._append_log(run["id"], execution_id, "Execution finished successfully")
        LOGGER.info(
            "Execution %s for run %s finalized successfully (artifacts=%s)",
            execution_id,
            run["id"],
            ", ".join(sorted(artifacts.keys())),
        )

    def _ensure_log_artifact(self, run_id: str, execution_id: str) -> None:
        log_path = self._log_path(run_id, execution_id)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        artifact = {
            "kind": ArtifactKind.log.value,
            "path": self._artifacts.relative(log_path),
            "url": self._artifacts.url(log_path),
            "label": "Execution Log",
            "content_type": "text/plain",
            "size_bytes": log_path.stat().st_size if log_path.exists() else 0,
        }
        self._repo.update_execution(
            execution_id,
            {
                "artifacts": {ArtifactKind.log.value: artifact},
            },
        )

    def _append_log(self, run_id: str, execution_id: str, message: str) -> None:
        log_path = self._log_path(run_id, execution_id)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        timestamp = _utcnow()
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(f"[{timestamp}] {message}\n")
        self._ensure_log_artifact(run_id, execution_id)

    def _ensure_worker(self) -> None:
        if self._worker and self._worker.is_alive():
            return
        self._worker = threading.Thread(target=self._run_loop, daemon=True, name="run-orchestrator")
        self._worker.start()

    def _ensure_watchdog(self) -> None:
        if self._watchdog and self._watchdog.is_alive():
            return
        if self._watchdog_stop.is_set():
            self._watchdog_stop.clear()
        self._watchdog = threading.Thread(
            target=self._watchdog_loop,
            daemon=True,
            name="run-orchestrator-watchdog",
        )
        self._watchdog.start()

    def _launch_execution(
        self,
        *,
        run: Dict[str, object],
        execution: Dict[str, object],
        task: Dict[str, object],
        page: Dict[str, object],
        baseline: Optional[Dict[str, object]],
        deadline: Optional[float],
        execution_timeout: Optional[int],
    ) -> Optional[ExecutionContext]:
        execution_id = execution["id"]
        run_id = run["id"]
        with self._lock:
            execution_cancelled = execution_id in self._cancelled_executions
            run_cancelled = run_id in self._cancelled_runs
        if execution_cancelled or run_cancelled:
            reason = "run cancellation" if run_cancelled else "execution cancellation"
            self._repo.update_execution(
                execution_id,
                {
                    "status": ExecutionStatus.cancelled.value,
                    "completed_at": _utcnow(),
                    "message": f"Execution cancelled before start ({reason}).",
                },
            )
            self._append_log(run_id, execution_id, f"Execution cancelled before start ({reason})")
            return None

        if not self._runner_runtime.supports_inline_launch:
            message = (
                f"Runner backend '{self._runner_runtime.backend}' is configured, so the "
                "FastAPI process will not launch Docker containers directly. Start the "
                "SCENE runner worker/k3s job launcher and run the runner readiness check "
                "from that runtime before accepting unattended runs."
            )
            self._repo.update_execution(
                execution_id,
                {
                    "status": ExecutionStatus.failed.value,
                    "completed_at": _utcnow(),
                    "message": message,
                },
            )
            self._append_log(run_id, execution_id, message)
            LOGGER.error("Execution %s cannot launch: %s", execution_id, message)
            return None

        execution_dir = self._artifacts.execution_dir(run_id, execution_id)
        config = {
            "url": page["url"],
            "browser": execution["browser"],
            "viewport": execution["viewport"],
            "preparatory_js": _clean_js(page.get("preparatory_js")),
            "preparatory_actions": _clean_actions(page.get("preparatory_actions")),
            "task_js": _clean_js(task.get("task_js")),
            "task_actions": _clean_actions(task.get("task_actions")),
            "post_wait_ms": self._post_wait_ms,
            "goto_timeout_ms": 60000,
        }
        if page.get("basic_auth_username"):
            config["http_credentials"] = {
                "username": page.get("basic_auth_username"),
                "password": page.get("basic_auth_password") or "",
            }
        if page.get("reference_url"):
            config["reference"] = {
                "url": page.get("reference_url"),
                "post_wait_ms": config.get("post_wait_ms", self._post_wait_ms),
            }

        self._runner.prepare_workspace(config, execution_dir)

        callback_url = self._scene_host_url.rstrip("/") + f"/api/executions/{execution_id}/complete"
        token = secrets.token_urlsafe(32)
        env = {
            "SCENE_CALLBACK_URL": callback_url,
            "SCENE_CALLBACK_TOKEN": token,
            "SCENE_EXECUTION_ID": execution_id,
        }
        container_name = f"scene-run-{execution_id}"
        log_path = self._log_path(run_id, execution_id)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        command = ["python", "runner.py", "config.json"]
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(f"[{_utcnow()}] COMMAND: docker run {getattr(self._runner, 'image', 'scene-playwright-runner:latest')} {' '.join(command)}\n")

        extra_hosts = {}
        if self._runner_runtime.add_host_gateway:
            extra_hosts["host.docker.internal"] = "host-gateway"
        target_hosts = set()
        parsed_primary = urlparse(str(config.get("url", "")))
        if parsed_primary.hostname:
            target_hosts.add(parsed_primary.hostname)
        reference_url = page.get("reference_url")
        if reference_url:
            parsed_ref = urlparse(str(reference_url))
            if parsed_ref.hostname:
                target_hosts.add(parsed_ref.hostname)
        for host in target_hosts:
            try:
                ip = socket.gethostbyname(host)
            except Exception:
                continue
            if ip:
                extra_hosts[host] = ip

        try:
            self._ensure_runner_image(run_id, execution_id)
            self._append_log(run_id, execution_id, "Container launch initiated")
            if self._docker_backend is None:
                raise RuntimeError("Docker runner backend is not available.")
            container_handle = self._docker_backend.run_container(
                getattr(self._runner, 'image', 'scene-playwright-runner:latest'),
                command,
                environment=env,
                volumes={str(execution_dir.resolve()): {"bind": "/workspace", "mode": "rw"}},
                working_dir="/workspace",
                shm_size=self._runner_runtime.shm_size,
                name=container_name,
                auto_remove=False,
                extra_hosts=extra_hosts,
            )
        except Exception as exc:
            LOGGER.error("Failed to launch container for execution %s: %s", execution_id, exc)
            self._repo.update_execution(
                execution_id,
                {
                    "status": ExecutionStatus.failed.value,
                    "completed_at": _utcnow(),
                    "message": f"Container launch failed: {exc}",
                },
            )
            self._append_log(run_id, execution_id, f"Container launch failed: {exc}")
            return None

        stop_event, log_thread = self._start_log_stream(run_id, execution_id, container_handle)

        started_ts = _utcnow()
        self._repo.update_execution(
            execution_id,
            {
                "status": ExecutionStatus.executing.value,
                "started_at": started_ts,
            },
        )
        self._append_log(run_id, execution_id, "Execution started")

        context = ExecutionContext(
            run_id=run_id,
            execution_id=execution_id,
            container_handle=container_handle,
            token=token,
            log_stop=stop_event,
            log_thread=log_thread,
            started_at=time.time(),
            deadline=(time.time() + execution_timeout) if execution_timeout else None,
        )
        self._execution_contexts[execution_id] = context
        self._execution_tokens[execution_id] = token
        return context

    def _ensure_runner_image(self, run_id: Optional[str], execution_id: Optional[str]) -> None:
        image = getattr(self._runner, "image", DEFAULT_RUNNER_IMAGE)
        with self._image_lock:
            if self._runner_image_verified:
                return
            if self._runner_runtime.backend != "docker":
                self._runner_image_verified = True
                return
            docker_cli = shutil.which("docker")
            if docker_cli:
                inspect_proc = subprocess.run(
                    [docker_cli, "image", "inspect", image],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                )
                if inspect_proc.returncode == 0:
                    self._runner_image_verified = True
                    return
            elif docker is not None:
                try:
                    docker.from_env().images.get(image)
                    self._runner_image_verified = True
                    return
                except ImageNotFound:
                    pass
                except APIError as exc:
                    raise RuntimeError(f"Failed to inspect runner image '{image}': {exc}") from exc

            if not self._runner_runtime.allow_image_build:
                raise RuntimeError(
                    f"Docker image '{image}' is not available. "
                    "Build or pull the pinned runner image before launching runs; "
                    "on-demand image builds are disabled for this runtime."
                )

            dockerfile_path = PROJECT_ROOT / "Dockerfile.playwright"
            if not docker_cli or not dockerfile_path.exists():
                raise RuntimeError(
                    f"Docker image '{image}' is not available. "
                    "Build or pull the pinned runner image before launching staging runs."
                )

            message = f"Runner image '{image}' not found; building from {dockerfile_path.name}"
            LOGGER.info(message)
            if run_id and execution_id:
                self._append_log(run_id, execution_id, message)
            build_cmd = [
                docker_cli,
                "build",
                "-f",
                str(dockerfile_path),
                "-t",
                image,
                str(PROJECT_ROOT),
            ]
            build_proc = subprocess.run(build_cmd, capture_output=True, text=True, check=False)
            if build_proc.returncode != 0:
                error_output = build_proc.stderr.strip() or build_proc.stdout.strip() or "docker build failed"
                raise RuntimeError(f"Failed to build runner image '{image}': {error_output}")
            self._runner_image_verified = True
            if run_id and execution_id:
                self._append_log(run_id, execution_id, "Runner image build complete")

    def enqueue(self, run_id: str) -> None:
        with self._lock:
            if run_id in self._inflight:
                return
            self._inflight.add(run_id)
        self._queue.put(run_id)
        self._ensure_worker()
        self._ensure_watchdog()

    def execute_now(self, run_id: str) -> None:
        """Execute a run immediately in the current thread (used by tests)."""
        with self._lock:
            self._inflight.add(run_id)
        try:
            self._process_run(run_id)
        finally:
            with self._lock:
                self._inflight.discard(run_id)

    def cancel_run(self, run_id: str) -> None:
        run = self._repo.get_run(run_id)
        if not run:
            return
        with self._lock:
            self._cancelled_runs.add(run_id)
        executions = self._repo.list_executions(run_id=run_id)
        for execution in executions:
            self._append_log(run_id, execution["id"], "Run cancellation requested")
        executions = self._repo.list_executions(run_id=run_id)
        for execution in executions:
            self.cancel_execution(
                execution["id"],
                refresh_run=False,
                reason="Run cancelled manually",
            )
        self._refresh_run_summary(run_id, force_status=RunStatus.cancelled.value)

    def cancel_execution(
        self,
        execution_id: str,
        *,
        refresh_run: bool = True,
        reason: Optional[str] = None,
    ) -> None:
        execution = self._repo.get_execution(execution_id)
        if not execution:
            return
        status = execution.get("status")
        if status not in {ExecutionStatus.queued.value, ExecutionStatus.executing.value}:
            return
        run_id = execution["run_id"]
        with self._lock:
            self._cancelled_executions.add(execution_id)
        log_message = reason or "Cancellation requested"
        self._append_log(run_id, execution_id, log_message)
        context = self._execution_contexts.get(execution_id)
        if context:
            context.log_stop.set()
            try:
                context.container_handle.kill()
            except Exception as exc:  # pragma: no cover - defensive
                LOGGER.debug("Failed to kill container for %s: %s", execution_id, exc)
            self._execution_tokens.pop(execution_id, None)
            payload = {"status": "cancelled", "error": log_message}
            self._enqueue_completion(run_id, execution_id, payload)
        else:
            update_payload = {
                "status": ExecutionStatus.cancelled.value,
                "completed_at": _utcnow(),
                "message": log_message,
            }
            self._repo.update_execution(execution_id, update_payload)
            with self._lock:
                self._cancelled_executions.discard(execution_id)
            if refresh_run:
                self._refresh_run_summary(run_id, force_status=None)

    def retry_execution(self, execution_id: str) -> Dict[str, Any]:
        execution = self._repo.get_execution(execution_id)
        if not execution:
            raise ValueError("Execution not found")
        run = self._repo.get_run(execution["run_id"])
        if not run:
            raise ValueError("Run not found")
        if run.get("status") == RunStatus.finished.value:
            raise ValueError("Cannot retry executions on a finished run.")
        task = self._repo.get_task(execution["task_id"])
        if not task:
            raise ValueError("Task not found")
        page = self._repo.get_page(task.get("page_id"))
        if not page:
            raise ValueError("Page not found")

        existing = self._repo.list_executions(run_id=run["id"])
        max_sequence = max((item.get("sequence") or 0 for item in existing), default=0)

        new_execution = self._repo.create_execution(
            {
                "run_id": run["id"],
                "project_id": run["project_id"],
                "batch_id": run["batch_id"],
                "task_id": execution["task_id"],
                "task_name": execution["task_name"],
                "page_id": execution.get("page_id"),
                "browser": execution["browser"],
                "viewport": execution["viewport"],
                "status": ExecutionStatus.queued.value,
                "sequence": max_sequence + 1,
            }
        )

        self._refresh_run_summary(run["id"], force_status=RunStatus.executing.value)
        self.enqueue(run["id"])
        return new_execution

    def _run_loop(self) -> None:
        while True:
            run_id = self._queue.get()
            try:
                self._process_run(run_id)
            except Exception:
                LOGGER.exception("Unhandled error while processing run %s", run_id)
            finally:
                with self._lock:
                    self._inflight.discard(run_id)
                self._queue.task_done()

    def _snapshot_execution_contexts(self) -> List[Tuple[str, ExecutionContext]]:
        with self._lock:
            return list(self._execution_contexts.items())

    def _mark_context_watchdog(self, execution_id: str) -> Optional[ExecutionContext]:
        with self._lock:
            context = self._execution_contexts.get(execution_id)
            if not context or context.watchdog_marked:
                return None
            context.watchdog_marked = True
            return context

    def _poll_container_status(self, context: ExecutionContext) -> str:
        try:
            status = context.container_handle.status()
            if status:
                return status
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.debug(
                "Watchdog failed to poll status for execution %s container %s: %s",
                context.execution_id,
                getattr(context.container_handle, "id", "<unknown>"),
                exc,
            )
        return "unknown"

    def _safe_exit_code(self, context: ExecutionContext) -> Optional[int]:
        try:
            code = context.container_handle.exit_code()
            context.exit_code = code
            return code
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.debug(
                "Watchdog failed to resolve exit code for execution %s container %s: %s",
                context.execution_id,
                getattr(context.container_handle, "id", "<unknown>"),
                exc,
            )
            return None

    def _log_execution_transition(
        self,
        *,
        event: str,
        run_id: str,
        execution_id: str,
        from_status: Optional[str],
        to_status: Optional[str],
        reason: str,
    ) -> None:
        LOGGER.info(
            "execution_transition event=%s run_id=%s execution_id=%s from_status=%s to_status=%s reason=%s",
            event,
            run_id,
            execution_id,
            from_status or "unknown",
            to_status or "unknown",
            reason,
        )

    @staticmethod
    def _coerce_result_payload(raw_payload: object) -> Optional[Dict[str, object]]:
        if not isinstance(raw_payload, dict):
            return None
        nested = raw_payload.get("result")
        if isinstance(nested, dict):
            return nested
        return {str(key): value for key, value in raw_payload.items()}

    def _read_result_json_payload(self, execution_dir: Path) -> Optional[Dict[str, object]]:
        result_path = execution_dir / "result.json"
        if not result_path.exists():
            return None
        try:
            with result_path.open("r", encoding="utf-8") as handle:
                return self._coerce_result_payload(json.load(handle))
        except Exception as exc:
            LOGGER.debug("Failed to read result.json at %s: %s", result_path, exc)
            return None

    def _read_runner_log_result_payload(self, log_path: Path) -> Optional[Dict[str, object]]:
        if not log_path.exists():
            return None
        try:
            lines = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except Exception as exc:
            LOGGER.debug("Failed to read runner log at %s: %s", log_path, exc)
            return None
        for line in reversed(lines):
            stripped = line.strip()
            if not stripped.startswith("{") or not stripped.endswith("}"):
                continue
            try:
                return self._coerce_result_payload(json.loads(stripped))
            except json.JSONDecodeError:
                continue
        return None

    def _enrich_payload_from_artifacts(
        self,
        execution_dir: Path,
        payload: Dict[str, object],
    ) -> Dict[str, object]:
        enriched = dict(payload)
        observed_path = execution_dir / "observed.png"
        reference_path = execution_dir / "reference.png"
        trace_path = execution_dir / "trace.zip"
        video_path = self._find_video(execution_dir)
        if observed_path.exists() and not isinstance(enriched.get("screenshot"), str):
            enriched["screenshot"] = observed_path.name
        if reference_path.exists() and not isinstance(enriched.get("reference"), str):
            enriched["reference"] = reference_path.name
        if trace_path.exists() and not isinstance(enriched.get("trace"), str):
            enriched["trace"] = trace_path.name
        if video_path and not isinstance(enriched.get("video"), str):
            try:
                enriched["video"] = str(video_path.relative_to(execution_dir))
            except ValueError:
                enriched["video"] = video_path.name
        return enriched

    def _infer_payload_from_artifacts(
        self,
        execution_dir: Path,
        log_path: Path,
        exit_code: Optional[int],
    ) -> Optional[Dict[str, object]]:
        observed_path = execution_dir / "observed.png"
        if not observed_path.exists():
            return None
        log_text = ""
        if log_path.exists():
            try:
                log_text = log_path.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                log_text = ""
        success_in_log = "Execution completed successfully" in log_text
        if exit_code == 0 and success_in_log:
            return self._enrich_payload_from_artifacts(execution_dir, {"status": "ok"})
        return None

    def _reconcile_payload_for_exit(
        self,
        context: ExecutionContext,
        *,
        status: str,
        exit_code: Optional[int],
    ) -> Dict[str, object]:
        execution_dir = self._artifacts.execution_dir(context.run_id, context.execution_id)
        log_path = self._log_path(context.run_id, context.execution_id)

        payload = self._read_result_json_payload(execution_dir)
        if payload is not None:
            self._log_execution_transition(
                event="watchdog_reconcile_result_json",
                run_id=context.run_id,
                execution_id=context.execution_id,
                from_status=ExecutionStatus.executing.value,
                to_status=str(payload.get("status") or "unknown"),
                reason=f"container_{status}",
            )
            return self._enrich_payload_from_artifacts(execution_dir, payload)

        payload = self._read_runner_log_result_payload(log_path)
        if payload is not None:
            self._log_execution_transition(
                event="watchdog_reconcile_runner_log",
                run_id=context.run_id,
                execution_id=context.execution_id,
                from_status=ExecutionStatus.executing.value,
                to_status=str(payload.get("status") or "unknown"),
                reason=f"container_{status}",
            )
            return self._enrich_payload_from_artifacts(execution_dir, payload)

        payload = self._infer_payload_from_artifacts(execution_dir, log_path, exit_code)
        if payload is not None:
            self._log_execution_transition(
                event="watchdog_reconcile_artifacts",
                run_id=context.run_id,
                execution_id=context.execution_id,
                from_status=ExecutionStatus.executing.value,
                to_status="ok",
                reason=f"container_{status}",
            )
            return payload

        message = "Execution watchdog detected container exit without callback"
        if exit_code is not None:
            message = f"{message} (exit_code={exit_code})"
        else:
            message = f"{message} (status={status})"
        self._log_execution_transition(
            event="watchdog_reconcile_failed",
            run_id=context.run_id,
            execution_id=context.execution_id,
            from_status=ExecutionStatus.executing.value,
            to_status=ExecutionStatus.failed.value,
            reason=message,
        )
        return {"status": "error", "error": message}

    def _handle_watchdog_timeout(self, execution_id: str, context: ExecutionContext) -> None:
        marked = self._mark_context_watchdog(execution_id)
        if not marked:
            return
        context = marked
        context.log_stop.set()
        try:
            context.container_handle.kill()
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.debug("Watchdog kill failed for execution %s: %s", execution_id, exc)
        message = "Execution watchdog triggered: deadline exceeded"
        self._log_execution_transition(
            event="watchdog_timeout",
            run_id=context.run_id,
            execution_id=execution_id,
            from_status=ExecutionStatus.executing.value,
            to_status=ExecutionStatus.cancelled.value,
            reason=message,
        )
        self._append_log(context.run_id, execution_id, message)
        payload = {"status": "timeout", "error": message}
        self._enqueue_completion(context.run_id, execution_id, payload)

    def _handle_watchdog_exit(
        self,
        execution_id: str,
        context: ExecutionContext,
        status: str,
    ) -> None:
        marked = self._mark_context_watchdog(execution_id)
        if not marked:
            return
        context = marked
        exit_code = self._safe_exit_code(context)
        self._stop_log_stream(context)
        message = "Execution watchdog detected container exit without callback"
        if exit_code is not None:
            message = f"{message} (exit_code={exit_code})"
        else:
            message = f"{message} (status={status})"
        self._append_log(context.run_id, execution_id, message)
        payload = context.result_payload or self._reconcile_payload_for_exit(
            context,
            status=status,
            exit_code=exit_code,
        )
        self._enqueue_completion(context.run_id, execution_id, payload)

    def _watchdog_loop(self) -> None:
        while not self._watchdog_stop.is_set():
            time.sleep(self._watchdog_interval)
            for execution_id, context in self._snapshot_execution_contexts():
                if context.result_payload is not None:
                    continue
                if context.watchdog_marked:
                    continue
                status = self._poll_container_status(context)
                if status in {"exited", "dead", "not_found"}:
                    self._handle_watchdog_exit(execution_id, context, status)
                    continue
                now = time.time()
                if context.deadline and now > context.deadline:
                    self._handle_watchdog_timeout(execution_id, context)
                    continue

    def _collect_logs(self, handle: DockerContainerHandleProtocol) -> str:
        try:
            logs = handle.logs(stream=False, follow=False)
            if isinstance(logs, bytes):
                return logs.decode("utf-8", errors="ignore")
            if isinstance(logs, str):
                return logs
            if logs is None:
                return ""
            chunks = []
            for chunk in logs:
                if isinstance(chunk, bytes):
                    chunks.append(chunk.decode("utf-8", errors="ignore"))
                else:
                    chunks.append(str(chunk))
            return "".join(chunks)
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.debug("Failed to gather logs for container %s: %s", getattr(handle, "id", "<unknown>"), exc)
            return ""

    def _wait_for_container(
        self,
        handle: DockerContainerHandleProtocol,
        *,
        timeout: int = 20,
    ) -> Optional[int]:
        deadline = time.time() + timeout
        while time.time() < deadline:
            status = "unknown"
            try:
                status = handle.status()
            except Exception as exc:  # pragma: no cover - defensive
                LOGGER.debug(
                    "Failed to poll container %s during wait: %s",
                    getattr(handle, "id", "<unknown>"),
                    exc,
                )
                break
            if status in {"exited", "dead"}:
                return handle.exit_code()
            time.sleep(0.5)
        return None

    def validate_callback_host(
        self,
        *,
        use_container: bool = True,
        timeout: int = 15,
    ) -> tuple[bool, str]:
        readiness = self.deployment_readiness()
        if not readiness.ok:
            error_messages = [
                issue.message for issue in readiness.issues if issue.level == "error"
            ]
            return False, "; ".join(error_messages)

        host_url = (self._scene_host_url or "").rstrip("/")
        if not host_url:
            return False, "Scene host URL is not configured."
        ping_url = f"{host_url}/api/orchestrator/ping"
        try:
            urllib.request.urlopen(ping_url, timeout=timeout)
        except urllib.error.URLError as exc:
            return False, f"Scene host unreachable from application process: {exc}"
        if not use_container:
            return True, "Scene host reachable from application process."
        if self._runner_runtime.backend != "docker":
            return (
                True,
                "Scene host reachable from application process; run scripts/runner_readiness.py "
                "from the configured runner pod/worker to prove runner-side reachability.",
            )
        if self._docker_backend is None:
            return False, "Docker runner backend is not available for callback probing."

        try:
            command = [
                "python",
                "-c",
                (
                    "import sys, urllib.request, urllib.error; "
                    "url = sys.argv[1]; "
                    "urllib.request.urlopen(url, timeout=10)"
                ),
                ping_url,
            ]
            handle = self._docker_backend.run_container(
                self._runner.image,
                command,
                environment={},
                volumes={},
                working_dir=None,
                shm_size=None,
                name=None,
                auto_remove=False,
                extra_hosts=(
                    {"host.docker.internal": "host-gateway"}
                    if self._runner_runtime.add_host_gateway
                    else {}
                ),
            )
        except Exception as exc:
            return False, f"Failed to launch callback probe container: {exc}"

        try:
            exit_code = self._wait_for_container(handle, timeout=timeout)
            if exit_code is None:
                try:
                    handle.kill()
                except Exception:
                    pass
                return False, "Callback probe container did not finish before timeout."
            if exit_code != 0:
                logs = self._collect_logs(handle).strip()
                if logs:
                    return False, f"Callback probe failed (exit_code={exit_code}): {logs}"
                return False, f"Callback probe failed (exit_code={exit_code})."
            return True, "Scene host reachable from runner containers."
        finally:
            try:
                handle.remove(force=True)
            except Exception:
                pass

    def deployment_readiness(self) -> RunnerReadinessReport:
        return validate_runner_runtime_config(self._runner_runtime)

    def reconcile(self) -> Dict[str, object]:
        result: Dict[str, object] = {
            "executions_reconciled": 0,
            "executions_cancelled": 0,
            "timed_out_runs": [],
        }
        timed_out_runs: set[str] = set()
        for execution_id, context in self._snapshot_execution_contexts():
            if context.result_payload is not None or context.watchdog_marked:
                continue

            status = self._poll_container_status(context)
            if status in {"exited", "dead", "not_found"}:
                self._handle_watchdog_exit(execution_id, context, status)
                result["executions_reconciled"] = int(result["executions_reconciled"]) + 1
                continue

            if context.deadline and time.time() > context.deadline:
                self._handle_watchdog_timeout(execution_id, context)
                result["executions_cancelled"] = int(result["executions_cancelled"]) + 1
                timed_out_runs.add(context.run_id)

        result["timed_out_runs"] = sorted(timed_out_runs)
        return result

    # ------------------------------------------------------------------ processing
    def _create_execution_matrix(
        self,
        run: Dict[str, object],
        task_records: List[Tuple[Dict[str, object], Dict[str, object]]],
    ) -> List[Dict[str, object]]:
        executions: List[Dict[str, object]] = []
        sequence = 0
        for task_meta, _page_meta in task_records:
            browsers = task_meta.get("browsers") or []
            if not browsers:
                config = self._repo.get_config()
                browsers = config.get("browsers", ["chromium"])

            raw_viewports = task_meta.get("viewports") or []
            if not raw_viewports:
                raw_viewports = [{"width": 1280, "height": 720}]

            for browser in browsers:
                for viewport in raw_viewports:
                    sequence += 1
                    execution = self._repo.create_execution(
                        {
                            "run_id": run["id"],
                            "project_id": run["project_id"],
                            "batch_id": run["batch_id"],
                            "task_id": task_meta["id"],
                            "task_name": task_meta["name"],
                            "page_id": task_meta["page_id"],
                            "browser": browser,
                            "viewport": {
                                "width": int(viewport["width"]),
                                "height": int(viewport["height"]),
                            },
                            "status": ExecutionStatus.queued.value,
                            "sequence": sequence,
                        }
                    )
                    self._append_log(run["id"], execution["id"], "Execution queued")
                    executions.append(execution)
        return executions

    def _compute_run_status(self, counts: Dict[str, int]) -> str:
        total = sum(counts.values())
        if counts[ExecutionStatus.executing.value] > 0:
            return RunStatus.executing.value
        if counts[ExecutionStatus.queued.value] > 0:
            return RunStatus.queued.value
        if counts[ExecutionStatus.failed.value] > 0:
            return RunStatus.failed.value
        if counts[ExecutionStatus.cancelled.value] > 0:
            if counts[ExecutionStatus.finished.value] == 0 or counts[ExecutionStatus.cancelled.value] + counts[ExecutionStatus.finished.value] == total:
                return RunStatus.cancelled.value
            if counts[ExecutionStatus.cancelled.value] == total:
                return RunStatus.cancelled.value
            return RunStatus.cancelled.value
        return RunStatus.finished.value if total > 0 else RunStatus.finished.value

    def _refresh_run_summary(self, run_id: str, *, force_status: Optional[str]) -> Dict[str, int]:
        counts = self._repo.execution_status_counts(run_id)
        diff_levels: List[float] = []
        for execution in self._repo.list_executions(run_id=run_id):
            level = execution.get("diff_level")
            if level is None:
                diff_info = execution.get("diff") or {}
                level = diff_info.get("percentage")
            if level is None:
                continue
            try:
                diff_levels.append(float(level))
            except (TypeError, ValueError):
                continue
        average_diff = sum(diff_levels) / len(diff_levels) if diff_levels else 0.0
        max_diff = max(diff_levels) if diff_levels else 0.0
        summary = {
            "executions_total": sum(counts.values()),
            "executions_finished": counts[ExecutionStatus.finished.value],
            "executions_failed": counts[ExecutionStatus.failed.value],
            "executions_cancelled": counts[ExecutionStatus.cancelled.value],
            "diff_average": round(average_diff, 4),
            "diff_maximum": round(max_diff, 4),
            "diff_samples": len(diff_levels),
        }
        status = force_status or self._compute_run_status(counts)
        self._repo.update_run(
            run_id,
            {
                "status": status,
                "summary": summary,
            },
        )
        return counts

    def _artifact_payload(self, result: RunnerResult, execution_dir: Path) -> Dict[str, Dict[str, object]]:
        payload: Dict[str, Dict[str, object]] = {}
        if result.screenshot:
            payload[ArtifactKind.observed.value] = self._build_artifact(
                result.screenshot,
                ArtifactKind.observed,
                "Observed Screenshot",
                "image/png",
            )
        if result.reference:
            payload[ArtifactKind.reference.value] = self._build_artifact(
                result.reference,
                ArtifactKind.reference,
                "Reference Screenshot",
                "image/png",
            )
        if result.trace:
            payload[ArtifactKind.trace.value] = self._build_artifact(
                result.trace,
                ArtifactKind.trace,
                "Playwright Trace",
                "application/zip",
            )
        if result.video:
            payload[ArtifactKind.video.value] = self._build_artifact(
                result.video,
                ArtifactKind.video,
                "Session Video",
                "video/webm",
            )
        if result.log:
            payload[ArtifactKind.log.value] = self._build_artifact(
                result.log,
                ArtifactKind.log,
                "Execution Log",
                "text/plain",
            )
        return payload

    def _runner_result_from_payload(
        self,
        run_id: str,
        execution_id: str,
        data: Dict[str, object],
    ) -> RunnerResult:
        execution_dir = self._artifacts.execution_dir(run_id, execution_id)
        screenshot = None
        screenshot_name = data.get("screenshot")
        if isinstance(screenshot_name, str):
            candidate = execution_dir / screenshot_name
            if candidate.exists():
                screenshot = candidate
        if not screenshot:
            candidate = execution_dir / "observed.png"
            if candidate.exists():
                screenshot = candidate

        reference = None
        reference_name = data.get("reference")
        if isinstance(reference_name, str):
            candidate = execution_dir / reference_name
            if candidate.exists():
                reference = candidate
        if not reference:
            candidate = execution_dir / "reference.png"
            if candidate.exists():
                reference = candidate

        trace = None
        trace_name = data.get("trace")
        if isinstance(trace_name, str):
            candidate = execution_dir / trace_name
            if candidate.exists():
                trace = candidate
        if not trace:
            candidate = execution_dir / "trace.zip"
            if candidate.exists():
                trace = candidate

        video = None
        video_name = data.get("video")
        if isinstance(video_name, str):
            candidate = execution_dir / video_name
            if not candidate.exists():
                candidate = execution_dir / "videos" / video_name
            if candidate.exists():
                video = candidate

        log_path = self._log_path(run_id, execution_id)
        success = data.get("status") == "ok"
        message = data.get("error") if isinstance(data.get("error"), str) else None
        return RunnerResult(
            success=success,
            screenshot=screenshot,
            reference=reference,
            trace=trace,
            video=video,
            log=log_path if log_path.exists() else None,
            message=message,
        )

    def _finalize_from_callback(
        self,
        run: Dict[str, object],
        execution_id: str,
        result_data: Dict[str, object],
        baseline_record: Optional[Dict[str, object]],
    ) -> None:
        execution = self._repo.get_execution(execution_id)
        if not execution:
            LOGGER.warning("Execution %s missing during finalization", execution_id)
            return
        runner_result = self._runner_result_from_payload(run["id"], execution_id, result_data)
        execution_dir = self._artifacts.execution_dir(run["id"], execution_id)
        log_path = self._log_path(run["id"], execution_id)
        if runner_result.log is None and log_path.exists():
            runner_result.log = log_path

        if runner_result.success and runner_result.screenshot:
            self._finalize_execution_success(
                run=run,
                execution=execution,
                baseline=baseline_record,
                execution_dir=execution_dir,
                result=runner_result,
            )
        else:
            if runner_result.success and not runner_result.screenshot:
                runner_result.message = "Runner reported success but observed screenshot artifact was not found"
                result_data = dict(result_data)
                result_data["status"] = "error"
                result_data["error"] = runner_result.message
            status_value = ExecutionStatus.failed.value
            status_token = result_data.get("status")
            if status_token in {"cancelled", "timeout"}:
                status_value = ExecutionStatus.cancelled.value
            message = runner_result.message or "Runner failure"
            artifacts = self._artifact_payload(runner_result, execution_dir)
            update_payload = {
                "status": status_value,
                "completed_at": _utcnow(),
                "message": message,
                "artifacts": artifacts,
            }
            self._repo.update_execution(execution_id, update_payload)
            self._log_execution_transition(
                event="execution_finalize",
                run_id=run["id"],
                execution_id=execution_id,
                from_status=str(execution.get("status") or "unknown"),
                to_status=status_value,
                reason=str(status_token or "error"),
            )
            LOGGER.error(
                "Execution %s for run %s failed (status=%s): %s",
                execution_id,
                run["id"],
                status_token or "error",
                message or "<no message>",
            )
            error_payload = result_data.get("error")
            if isinstance(error_payload, str) and error_payload.strip():
                snippet = error_payload.strip()
                if len(snippet) > 4000:
                    snippet = snippet[:4000] + "…"
                LOGGER.debug("Execution %s error detail:\\n%s", execution_id, snippet)
            self._append_log(
                run["id"],
                execution_id,
                f"Execution {status_token or 'failed'}: {message}",
            )

        with self._lock:
            self._cancelled_executions.discard(execution_id)

        self._refresh_run_summary(run["id"], force_status=None)

    def _build_artifact(
        self,
        path: Path,
        kind: ArtifactKind,
        label: str,
        content_type: str,
    ) -> Dict[str, object]:
        return {
            "kind": kind.value,
            "path": self._artifacts.relative(path),
            "url": self._artifacts.url(path),
            "label": label,
            "content_type": content_type,
            "size_bytes": path.stat().st_size if path.exists() else 0,
        }

    def _store_baseline_artifacts(
        self,
        *,
        baseline_id: str,
        execution: Dict[str, object],
        screenshot: Path,
    ) -> Dict[str, object]:
        baseline_path = self._artifacts.copy_to_baseline(screenshot, baseline_id, f"{execution['id']}.png")
        artifact = self._build_artifact(
            baseline_path,
            ArtifactKind.baseline,
            "Baseline Screenshot",
            "image/png",
        )
        item = {
            "task_id": execution["task_id"],
            "task_name": execution["task_name"],
            "browser": execution["browser"],
            "viewport": execution["viewport"],
            "captured_at": _utcnow(),
            "execution_id": execution["id"],
            "artifacts": {
                ArtifactKind.baseline.value: artifact,
            },
        }
        self._repo.append_baseline_item(baseline_id, item)
        return item

    def _compare_with_baseline(
        self,
        *,
        run: Dict[str, object],
        execution: Dict[str, object],
        execution_dir: Path,
        observed: Path,
        baseline_record: Optional[Dict[str, object]],
    ) -> Dict[str, object]:
        if not baseline_record:
            message = f"No baseline available for comparison run {run['id']}"
            LOGGER.warning(message)
            return {"artifacts": {}, "diff": None, "error": message}

        matching_item = None
        for item in baseline_record.get("items", []):
            if (
                item.get("task_id") == execution["task_id"]
                and item.get("browser") == execution["browser"]
                and item.get("viewport", {}).get("width") == execution["viewport"]["width"]
                and item.get("viewport", {}).get("height") == execution["viewport"]["height"]
            ):
                matching_item = item
                break

        if not matching_item:
            message = (
                f"Baseline {baseline_record['id']} missing match for execution {execution['id']} "
                f"(task={execution['task_id']} browser={execution['browser']} "
                f"viewport={execution['viewport']})"
            )
            LOGGER.warning(message)
            return {"artifacts": {}, "diff": None, "error": message}

        baseline_artifact = matching_item["artifacts"].get(ArtifactKind.baseline.value)
        if not baseline_artifact:
            message = f"Baseline {baseline_record['id']} match for execution {execution['id']} has no artifact"
            LOGGER.warning(message)
            return {"artifacts": {}, "diff": None, "error": message}

        baseline_path = self._artifacts.root / Path(baseline_artifact["path"])
        if not baseline_path.exists():
            message = f"Baseline artifact missing on disk: {baseline_path}"
            LOGGER.warning(message)
            return {"artifacts": {}, "diff": None, "error": message}

        LOGGER.debug(
            "Baseline match found for execution %s (task=%s browser=%s viewport=%sx%s)",
            execution["id"],
            execution["task_id"],
            execution["browser"],
            execution["viewport"]["width"],
            execution["viewport"]["height"],
        )

        baseline_copy = execution_dir / "baseline.png"
        if not baseline_copy.exists():
            shutil.copy2(baseline_path, baseline_copy)

        diff_path = execution_dir / "diff.png"
        heatmap_path = execution_dir / "heatmap.png"
        stats = self._diffs.generate(
            baseline_copy,
            observed,
            diff_path,
            heatmap_path,
            pixel_tolerance=self._diff_pixel_tolerance,
        )

        artifacts = {
            ArtifactKind.baseline.value: self._build_artifact(
                baseline_copy,
                ArtifactKind.baseline,
                "Baseline Screenshot",
                "image/png",
            ),
            ArtifactKind.diff.value: self._build_artifact(
                diff_path,
                ArtifactKind.diff,
                "Pixel Diff",
                "image/png",
            ),
            ArtifactKind.heatmap.value: self._build_artifact(
                heatmap_path,
                ArtifactKind.heatmap,
                "Heatmap Overlay",
                "image/png",
            ),
        }
        return {
            "artifacts": artifacts,
            "diff": stats,
        }

    def update_concurrency(self, max_workers: int) -> None:
        with self._lock:
            self._max_concurrent = max_workers
        LOGGER.info("Updated orchestrator concurrency limit to %s", max_workers)

    def update_scene_host(self, host_url: str) -> None:
        callback_url = host_url
        if self._runner_runtime.backend == "k3s" and self._runner_runtime.k3s_service_url:
            callback_url = self._runner_runtime.k3s_service_url
        with self._lock:
            self._scene_host_url = callback_url
        LOGGER.info("Updated scene host callback URL to %s", callback_url)

    def update_capture_delay(self, milliseconds: int) -> None:
        with self._lock:
            self._post_wait_ms = max(0, int(milliseconds))
        LOGGER.info("Updated capture stabilization delay to %sms", milliseconds)

    def update_diff_pixel_tolerance(self, tolerance: int) -> None:
        normalized = DiffEngine._normalize_tolerance(tolerance)
        with self._lock:
            self._diff_pixel_tolerance = normalized
            self._diffs = DiffEngine(pixel_tolerance=normalized)
        LOGGER.info("Updated diff pixel tolerance to %s", normalized)

    def handle_execution_callback(self, execution_id: str, payload: Dict[str, object]) -> bool:
        token = payload.get("token") if isinstance(payload, dict) else None
        result_data = payload.get("result") if isinstance(payload, dict) else None
        if not isinstance(result_data, dict):
            result_data = {}
        expected = self._execution_tokens.get(execution_id)
        if not expected or token != expected:
            LOGGER.warning("Rejected callback for execution %s due to invalid token", execution_id)
            return False
        context = self._execution_contexts.get(execution_id)
        if not context:
            LOGGER.warning("Received callback for unknown execution %s", execution_id)
            return False
        context.result_payload = result_data
        context.log_stop.set()
        self._execution_tokens.pop(execution_id, None)
        self._enqueue_completion(context.run_id, execution_id, result_data)
        return True


_orchestrator: Optional[RunOrchestrator] = None


def get_orchestrator() -> RunOrchestrator:
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = RunOrchestrator()
    return _orchestrator
