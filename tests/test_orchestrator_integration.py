from __future__ import annotations

import json
import socket
import threading
from functools import partial
from http.server import BaseHTTPRequestHandler, SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Tuple
import shutil
import uuid
import subprocess

import pytest

from app.schemas import BaselineStatus, ExecutionStatus, RunPurpose, RunStatus
from app.services.artifacts import ArtifactStore
from app.services.orchestrator import RunOrchestrator
from app.services.storage import LocalDynamoStorage, SceneRepository


def _start_http_server(root: Path) -> Tuple[ThreadingHTTPServer, threading.Thread, int]:
    handler = partial(SimpleHTTPRequestHandler, directory=str(root))

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]

    server = ThreadingHTTPServer(("0.0.0.0", port), handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, port


class CallbackHandler(BaseHTTPRequestHandler):
    orchestrator: RunOrchestrator | None = None

    def _write(self, status: int, payload: dict) -> None:
        body = json.dumps(payload)
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body.encode("utf-8"))))
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))

    def do_POST(self) -> None:  # noqa: N802 - mandated by BaseHTTPRequestHandler
        orchestrator = self.__class__.orchestrator
        if orchestrator is None:
            self._write(503, {"error": "orchestrator unavailable"})
            return
        length = int(self.headers.get("Content-Length", "0"))
        data = self.rfile.read(length) if length else b""
        try:
            payload = json.loads(data.decode("utf-8")) if data else {}
        except json.JSONDecodeError:
            self._write(400, {"error": "invalid json"})
            return

        parts = self.path.strip("/").split("/")
        if len(parts) != 4 or parts[0] != "api" or parts[1] != "executions" or parts[3] != "complete":
            self._write(404, {"error": "unknown path"})
            return

        execution_id = parts[2]
        try:
            accepted = orchestrator.handle_execution_callback(execution_id, payload)  # type: ignore[arg-type]
        except Exception as exc:  # pragma: no cover - defensive guard
            self._write(500, {"error": str(exc)})
            return

        if not accepted:
            self._write(403, {"error": "callback rejected"})
            return

        self._write(200, {"status": "ok"})


def _start_callback_server(orchestrator: RunOrchestrator) -> Tuple[ThreadingHTTPServer, threading.Thread, int]:
    handler = CallbackHandler
    handler.orchestrator = orchestrator

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]

    server = ThreadingHTTPServer(("0.0.0.0", port), handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, port


@pytest.mark.integration
def test_playwright_baseline_and_comparison(tmp_path: Path) -> None:
    ensure_runner_image()
    html = tmp_path / "index.html"
    html.write_text(
        "<html><body style='background:white'><h1 id='headline'>Baseline</h1></body></html>",
        encoding="utf-8",
    )

    server, thread, port = _start_http_server(tmp_path)

    storage = LocalDynamoStorage(tmp_path / "db.json")
    repo = SceneRepository(storage)
    artifact_root = Path.cwd() / "tmp_artifacts" / uuid.uuid4().hex
    artifact_store = ArtifactStore(root=artifact_root)
    orchestrator = RunOrchestrator(repo=repo, artifacts=artifact_store, auto_start=False)
    callback_server, callback_thread, callback_port = _start_callback_server(orchestrator)
    repo.set_scene_host_url(f"http://host.docker.internal:{callback_port}")
    orchestrator.update_scene_host(f"http://host.docker.internal:{callback_port}")

    try:
        project = repo.create_project({"name": "Demo", "slug": "demo"})
        page = repo.create_page(
            {
                "project_id": project["id"],
                "name": "Landing",
                "url": f"http://host.docker.internal:{port}/index.html",
                "preparatory_js": "document.body.style.padding='20px';",
            }
        )
        task = repo.create_task(
            {
                "project_id": project["id"],
                "page_id": page["id"],
                "name": "Landing baseline",
                "browsers": ["chromium"],
                "viewports": [{"width": 800, "height": 600}],
                "task_js": "document.getElementById('headline').textContent = 'Baseline Ready';",
            }
        )
        batch = repo.create_batch(
            {
                "project_id": project["id"],
                "name": "Smoke",
                "task_ids": [task["id"]],
            }
        )

        baseline_run = repo.create_run(
            {
                "project_id": project["id"],
                "batch_id": batch["id"],
                "purpose": RunPurpose.baseline_recording.value,
                "requested_by": "integration",
            }
        )
        orchestrator.execute_now(baseline_run["id"])

        refreshed_baseline_run = repo.get_run(baseline_run["id"])
        assert refreshed_baseline_run["status"] == RunStatus.finished.value
        assert refreshed_baseline_run["baseline_id"]

        baseline_record = repo.get_baseline(refreshed_baseline_run["baseline_id"])
        assert baseline_record
        assert baseline_record["status"] == BaselineStatus.completed.value
        assert len(baseline_record["items"]) == 1

        baseline_artifact = baseline_record["items"][0]["artifacts"]["baseline"]
        baseline_path = artifact_store.root / baseline_artifact["path"]
        assert baseline_path.exists()

        # Change the page to introduce a visual difference.
        html.write_text(
            "<html><body style='background:#222;color:#fff'><h1 id='headline'>Updated</h1></body></html>",
            encoding="utf-8",
        )

        comparison_run = repo.create_run(
            {
                "project_id": project["id"],
                "batch_id": batch["id"],
                "purpose": RunPurpose.comparison.value,
                "baseline_id": baseline_record["id"],
                "requested_by": "integration",
            }
        )
        orchestrator.execute_now(comparison_run["id"])

        refreshed_comparison_run = repo.get_run(comparison_run["id"])
        assert refreshed_comparison_run["status"] == RunStatus.finished.value
        executions = repo.list_executions(run_id=comparison_run["id"])
        assert len(executions) == 1

        execution = executions[0]
        assert execution["status"] == ExecutionStatus.finished.value
        assert execution.get("diff")
        assert execution["diff"]["percentage"] > 0.0

        artifacts = execution["artifacts"]
        observed_artifact = artifacts["observed"]
        diff_artifact = artifacts["diff"]
        heatmap_artifact = artifacts["heatmap"]
        baseline_copy = artifacts["baseline"]
        trace_artifact = artifacts["trace"]
        log_artifact = artifacts["log"]

        for art in [observed_artifact, diff_artifact, heatmap_artifact, baseline_copy, trace_artifact, log_artifact]:
            path = artifact_store.root / art["path"]
            assert path.exists(), f"Missing artifact {art['path']}"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        callback_server.shutdown()
        callback_thread.join(timeout=5)
        shutil.rmtree(artifact_root, ignore_errors=True)


def ensure_runner_image() -> None:
    image = "scene-playwright-runner:latest"
    inspect = subprocess.run(
        ["docker", "image", "inspect", image],
        capture_output=True,
        text=True,
    )
    if inspect.returncode != 0:
        stderr = (inspect.stderr or "").lower()
        if "permission denied" in stderr or "cannot connect to the docker daemon" in stderr:
            pytest.skip("Docker daemon unavailable; skipping integration test")
        try:
            subprocess.run(
                ["docker", "build", "-t", image, "-f", "Dockerfile.playwright", "."],
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as exc:
            error_text = ((exc.stderr or "") + (exc.stdout or "")).lower()
            if "permission denied" in error_text or "cannot connect" in error_text:
                pytest.skip("Docker daemon unavailable; skipping integration test")
            raise
