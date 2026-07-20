#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import shutil
import socket
import subprocess
import sys
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]
QUALITY_ROOT = REPO_ROOT / ".scene" / "quality-gate"


def _python() -> str:
    for environment in (".venv", "venv"):
        venv_python = REPO_ROOT / environment / "bin" / "python"
        if venv_python.exists():
            return str(venv_python)
    return sys.executable


def _docker_compose_command() -> list[str]:
    if shutil.which("docker-compose"):
        return ["docker-compose"]
    return ["docker", "compose"]


def _base_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("SCENE_STATE_PATH", str(QUALITY_ROOT / "state" / "dev.dynamodb.json"))
    env.setdefault("SCENE_ARTIFACT_ROOT", str(QUALITY_ROOT / "artifacts"))
    env.setdefault("PLAYWRIGHT_HTML_REPORT", str(QUALITY_ROOT / "frontend-playwright-report"))
    return env


def _frontend_env(env: dict[str, str]) -> dict[str, str]:
    frontend_env = env.copy()
    base_url = frontend_env.get("BASE_URL")
    if not base_url:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.bind(("127.0.0.1", 0))
            frontend_port = listener.getsockname()[1]
        base_url = f"http://127.0.0.1:{frontend_port}"
        frontend_env.setdefault(
            "PW_WEB_SERVER_COMMAND",
            (
                f"cd {REPO_ROOT} && {_python()} -m uvicorn app.main:app "
                f"--host 127.0.0.1 --port {frontend_port}"
            ),
        )
    frontend_env.setdefault("BASE_URL", base_url)
    frontend_env.setdefault("API_BASE_URL", base_url.rstrip("/") + "/api")
    # UI tests need a stable terminal execution but do not exercise Docker or k3s.
    frontend_env.setdefault("SCENE_RUNNER_BACKEND", "worker")
    frontend_env.setdefault("SCENE_RUNNER_IMAGE", "scene-playwright-runner:1.47.0-jammy")
    frontend_env.setdefault("SCENE_RUNNER_IMAGE_AUTOBUILD", "false")
    frontend_env.setdefault("SCENE_RUNNER_CALLBACK_BASE_URL", base_url)
    frontend_env.setdefault("SCENE_K3S_SERVICE_URL", base_url)
    frontend_env.setdefault("SCENE_ARTIFACT_STORAGE", "pvc")
    frontend_env.setdefault("SCENE_ARTIFACT_PVC_CLAIM", "scene-quality-gate")
    frontend_env.setdefault("PW_REUSE_EXISTING_SERVER", "false")
    return frontend_env


def _integration_env(env: dict[str, str]) -> dict[str, str]:
    integration_env = env.copy()
    if integration_env.get("DOCKER_HOST") or not shutil.which("docker"):
        return integration_env
    context = subprocess.run(
        ["docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    endpoint = context.stdout.strip()
    if context.returncode == 0 and endpoint:
        integration_env["DOCKER_HOST"] = endpoint
    return integration_env


def _staging_env() -> dict[str, str]:
    # Compose should validate its own defaults, not paths injected for pytest.
    return os.environ.copy()


def _commands(group: str) -> list[list[str]]:
    python = _python()
    if group == "lint":
        return [[python, "-m", "ruff", "check", "app", "scene_mcp", "scripts", "tests"]]
    if group == "unit":
        return [[python, "-m", "pytest", "-m", "not integration", "-q"]]
    if group == "integration":
        return [[python, "-m", "pytest", "-m", "integration", "-q"]]
    if group == "frontend":
        return [
            [
                "npm",
                "--prefix",
                "frontend",
                "run",
                "test",
                "--",
                "--output=../.scene/quality-gate/frontend-test-results",
                "--workers=1",
            ]
        ]
    if group == "staging-config":
        return [_docker_compose_command() + ["-f", "docker-compose.staging.yml", "config"]]
    raise ValueError(f"Unknown quality gate group: {group}")


def _expand_groups(groups: Iterable[str]) -> list[str]:
    expanded: list[str] = []
    for group in groups:
        if group == "all":
            expanded.extend(["lint", "unit", "integration", "frontend", "staging-config"])
        else:
            expanded.append(group)
    return expanded


def _run(command: list[str], env: dict[str, str], *, dry_run: bool) -> int:
    printable = " ".join(command)
    if dry_run:
        print(printable)
        return 0
    print(f"\n$ {printable}", flush=True)
    return subprocess.run(command, cwd=REPO_ROOT, env=env, check=False).returncode


def _prepare_group(group: str, *, dry_run: bool) -> None:
    if dry_run or group != "frontend":
        return
    for child in [
        QUALITY_ROOT / "state",
        QUALITY_ROOT / "artifacts",
        QUALITY_ROOT / "frontend-test-results",
        QUALITY_ROOT / "frontend-playwright-report",
    ]:
        shutil.rmtree(child, ignore_errors=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the SCENE deterministic quality gate.")
    parser.add_argument(
        "groups",
        nargs="*",
        default=["unit"],
        choices=["lint", "unit", "integration", "frontend", "staging-config", "all"],
        help=(
            "Quality gate group(s) to run. Use 'all' for lint, unit, integration, "
            "frontend, and staging config."
        ),
    )
    parser.add_argument("--dry-run", action="store_true", help="Print commands without running them.")
    args = parser.parse_args()

    env = _base_env()
    QUALITY_ROOT.mkdir(parents=True, exist_ok=True)
    for group in _expand_groups(args.groups):
        _prepare_group(group, dry_run=args.dry_run)
        if group == "frontend":
            group_env = _frontend_env(env)
        elif group == "integration":
            group_env = _integration_env(env)
        elif group == "staging-config":
            group_env = _staging_env()
        else:
            group_env = env
        for command in _commands(group):
            result = _run(command, group_env, dry_run=args.dry_run)
            if result != 0:
                return result
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
