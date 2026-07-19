#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib import error, request


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the SCENE Linux Docker staging smoke check."
    )
    parser.add_argument(
        "--base-url",
        default=os.environ.get("SCENE_HOST_URL", "http://host.docker.internal:8010"),
        help="SCENE URL reachable from the staging host and runner containers.",
    )
    parser.add_argument(
        "--runner-image",
        default=os.environ.get("SCENE_RUNNER_IMAGE", "scene-playwright-runner:1.47.0-jammy"),
        help="Pinned Playwright runner image to verify and use for callback probing.",
    )
    parser.add_argument(
        "--artifact-root",
        default=os.environ.get("SCENE_ARTIFACT_ROOT"),
        help="Persistent artifact root on the staging host.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=int(os.environ.get("SCENE_SMOKE_TIMEOUT_SECONDS", "600")),
        help="Maximum seconds to wait for each baseline/comparison run.",
    )
    parser.add_argument(
        "--keep-project",
        action="store_true",
        help="Leave the smoke project and generated run records in SCENE.",
    )
    return parser.parse_args()


class SmokeError(RuntimeError):
    pass


def api_url(base_url: str, path: str) -> str:
    return base_url.rstrip("/") + path


def request_json(
    base_url: str,
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
    *,
    timeout: int = 30,
) -> Any:
    body = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = request.Request(
        api_url(base_url, path),
        data=body,
        headers=headers,
        method=method,
    )
    try:
        with request.urlopen(req, timeout=timeout) as response:
            raw = response.read()
    except error.HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        raise SmokeError(f"{method} {path} failed: HTTP {exc.code} {details}") from exc
    except error.URLError as exc:
        raise SmokeError(f"{method} {path} failed: {exc}") from exc
    if not raw:
        return None
    return json.loads(raw.decode("utf-8"))


def fetch(base_url: str, path_or_url: str, *, timeout: int = 30) -> bytes:
    url = path_or_url if path_or_url.startswith("http") else api_url(base_url, path_or_url)
    try:
        with request.urlopen(url, timeout=timeout) as response:
            return response.read()
    except error.URLError as exc:
        raise SmokeError(f"GET {url} failed: {exc}") from exc


def ensure_docker_image(image: str) -> None:
    docker = shutil.which("docker")
    if not docker:
        raise SmokeError("docker CLI is required on the staging host for smoke checks.")
    inspect = subprocess.run(
        [docker, "image", "inspect", image],
        capture_output=True,
        text=True,
        check=False,
    )
    if inspect.returncode != 0:
        raise SmokeError(
            f"Runner image '{image}' is not present. "
            "Build it with the runner-image compose profile or pull it explicitly first."
        )


def verify_orchestrator_readiness(base_url: str) -> dict[str, Any]:
    readiness = request_json(base_url, "GET", "/api/orchestrator/readiness")
    if not readiness.get("ok"):
        raise SmokeError(
            "Orchestrator readiness failed: "
            + json.dumps(readiness.get("issues", []), sort_keys=True)
        )
    return readiness


def verify_callback_from_runner(base_url: str, image: str) -> None:
    docker = shutil.which("docker")
    assert docker is not None
    ping_url = api_url(base_url, "/api/orchestrator/ping")
    code = (
        "import sys, urllib.request; "
        "urllib.request.urlopen(sys.argv[1], timeout=15).read(); "
        "print('callback-ok')"
    )
    probe = subprocess.run(
        [
            docker,
            "run",
            "--rm",
            "--add-host",
            "host.docker.internal:host-gateway",
            image,
            "python",
            "-c",
            code,
            ping_url,
        ],
        capture_output=True,
        text=True,
        timeout=45,
        check=False,
    )
    if probe.returncode != 0:
        output = (probe.stderr or probe.stdout or "").strip()
        raise SmokeError(f"Runner container cannot reach {ping_url}: {output}")


def verify_artifact_write_read(base_url: str, artifact_root: Path) -> str:
    token = uuid.uuid4().hex
    relative = Path("smoke") / f"{token}.txt"
    target = artifact_root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    expected = f"scene staging smoke {token}\n".encode("utf-8")
    target.write_bytes(expected)
    observed = fetch(base_url, "/artifacts/" + relative.as_posix())
    if observed != expected:
        raise SmokeError("Artifact readback did not match the file written to the host volume.")
    return "/artifacts/" + relative.as_posix()


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("0.0.0.0", 0))
        return int(sock.getsockname()[1])


def start_fixture_server(root: Path) -> tuple[ThreadingHTTPServer, threading.Thread, int]:
    port = free_port()
    handler = partial(SimpleHTTPRequestHandler, directory=str(root))
    server = ThreadingHTTPServer(("0.0.0.0", port), handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, port


def wait_for_run(base_url: str, run_id: str, timeout: int) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_status = "queued"
    while time.monotonic() < deadline:
        run = request_json(base_url, "GET", f"/api/runs/{run_id}")
        last_status = str(run.get("status"))
        if last_status in {"finished", "failed", "cancelled"}:
            return run
        time.sleep(2)
    raise SmokeError(f"Run {run_id} did not finish within {timeout}s; last status={last_status}.")


def assert_finished(run: dict[str, Any], label: str) -> None:
    if run.get("status") != "finished":
        raise SmokeError(f"{label} run {run.get('id')} ended with status={run.get('status')}.")


def artifact_url(artifact: dict[str, Any]) -> str:
    url = artifact.get("url")
    if not isinstance(url, str) or not url:
        path = artifact.get("path")
        if not isinstance(path, str) or not path:
            raise SmokeError(f"Artifact missing url/path: {artifact}")
        return "/artifacts/" + path
    return url


def verify_execution_artifacts(base_url: str, run_id: str, required: set[str]) -> list[dict[str, Any]]:
    executions = request_json(base_url, "GET", f"/api/runs/{run_id}/executions")
    if not executions:
        raise SmokeError(f"Run {run_id} has no executions.")
    for execution in executions:
        if execution.get("status") != "finished":
            raise SmokeError(
                f"Execution {execution.get('id')} ended with status={execution.get('status')}: "
                f"{execution.get('message')}"
            )
        artifacts = execution.get("artifacts") or {}
        missing = sorted(required - set(artifacts))
        if missing:
            raise SmokeError(f"Execution {execution.get('id')} missing artifacts: {missing}")
        for kind in required:
            fetch(base_url, artifact_url(artifacts[kind]))
    return executions


def run_baseline_comparison(base_url: str, timeout: int, *, keep_project: bool) -> dict[str, Any]:
    smoke_id = uuid.uuid4().hex[:8]
    with tempfile.TemporaryDirectory(prefix="scene-smoke-") as tmp:
        fixture_root = Path(tmp)
        index = fixture_root / "index.html"
        index.write_text(
            "<html><body style='font-family:sans-serif;background:white;color:#111'>"
            "<h1>SCENE staging baseline</h1><main style='width:360px;height:180px'>A</main>"
            "</body></html>",
            encoding="utf-8",
        )
        server, thread, fixture_port = start_fixture_server(fixture_root)
        project_id = None
        try:
            page_url = f"http://host.docker.internal:{fixture_port}/index.html"
            project = request_json(
                base_url,
                "POST",
                "/api/projects",
                {
                    "name": f"SCENE staging smoke {smoke_id}",
                    "slug": f"scene-smoke-{smoke_id}",
                    "description": "Generated by scripts/staging_smoke.py",
                },
            )
            project_id = project["id"]
            page = request_json(
                base_url,
                "POST",
                "/api/pages",
                {
                    "project_id": project_id,
                    "name": "Fixture",
                    "url": page_url,
                    "preparatory_actions": [{"type": "disable_animations"}],
                },
            )
            task = request_json(
                base_url,
                "POST",
                "/api/tasks",
                {
                    "project_id": project_id,
                    "page_id": page["id"],
                    "name": "Chromium and Firefox fixture capture",
                    "browsers": ["chromium", "firefox"],
                    "viewports": [{"width": 800, "height": 600}],
                },
            )
            batch = request_json(
                base_url,
                "POST",
                "/api/batches",
                {
                    "project_id": project_id,
                    "name": "Linux Docker staging smoke",
                    "description": "Exercises callback, artifacts, Chromium, Firefox, and diffing.",
                    "task_ids": [task["id"]],
                    "spm_ticket": "SCENE-10",
                    "run_diff_threshold": 0,
                    "execution_diff_threshold": 0,
                },
            )
            baseline_run = request_json(
                base_url,
                "POST",
                "/api/runs",
                {
                    "project_id": project_id,
                    "batch_id": batch["id"],
                    "purpose": "baseline_recording",
                    "requested_by": "staging-smoke",
                },
            )
            baseline_run = wait_for_run(base_url, baseline_run["id"], timeout)
            assert_finished(baseline_run, "Baseline")
            verify_execution_artifacts(base_url, baseline_run["id"], {"observed", "log", "trace"})
            baseline_id = baseline_run.get("baseline_id")
            if not baseline_id:
                raise SmokeError("Baseline run finished without a baseline_id.")

            index.write_text(
                "<html><body style='font-family:sans-serif;background:#123;color:white'>"
                "<h1>SCENE staging comparison</h1><main style='width:360px;height:180px'>B</main>"
                "</body></html>",
                encoding="utf-8",
            )
            comparison_run = request_json(
                base_url,
                "POST",
                "/api/runs",
                {
                    "project_id": project_id,
                    "batch_id": batch["id"],
                    "purpose": "comparison",
                    "baseline_id": baseline_id,
                    "requested_by": "staging-smoke",
                },
            )
            comparison_run = wait_for_run(base_url, comparison_run["id"], timeout)
            assert_finished(comparison_run, "Comparison")
            comparison_executions = verify_execution_artifacts(
                base_url,
                comparison_run["id"],
                {"observed", "baseline", "diff", "heatmap", "log", "trace"},
            )
            if not any((execution.get("diff") or {}).get("percentage", 0) > 0 for execution in comparison_executions):
                raise SmokeError("Comparison completed but did not record a positive diff percentage.")

            return {
                "project_id": project_id,
                "batch_id": batch["id"],
                "baseline_run_id": baseline_run["id"],
                "baseline_id": baseline_id,
                "comparison_run_id": comparison_run["id"],
                "execution_count": len(comparison_executions),
                "diff_percentages": [
                    (execution.get("diff") or {}).get("percentage", 0)
                    for execution in comparison_executions
                ],
            }
        finally:
            server.shutdown()
            thread.join(timeout=5)
            if project_id and not keep_project:
                try:
                    request_json(base_url, "DELETE", f"/api/projects/{project_id}")
                except SmokeError as exc:
                    print(f"warning: failed to delete smoke project {project_id}: {exc}", file=sys.stderr)


def main() -> int:
    args = parse_args()
    base_url = args.base_url.rstrip("/")
    if not args.artifact_root:
        raise SmokeError("--artifact-root or SCENE_ARTIFACT_ROOT is required.")
    artifact_root = Path(args.artifact_root).expanduser().resolve()
    if not artifact_root.exists():
        raise SmokeError(f"Artifact root does not exist: {artifact_root}")

    request_json(base_url, "GET", "/api/orchestrator/ping")
    readiness = verify_orchestrator_readiness(base_url)
    ensure_docker_image(args.runner_image)
    verify_callback_from_runner(base_url, args.runner_image)
    smoke_artifact_url = verify_artifact_write_read(base_url, artifact_root)
    run_result = run_baseline_comparison(
        base_url,
        args.timeout,
        keep_project=args.keep_project,
    )

    print(json.dumps({
        "status": "ok",
        "base_url": base_url,
        "runner_image": args.runner_image,
        "runner_backend": readiness.get("config", {}).get("backend"),
        "artifact_readback_url": smoke_artifact_url,
        **run_result,
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SmokeError as exc:
        print(f"staging smoke failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
