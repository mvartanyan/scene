#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from pathlib import Path
from typing import Any
from urllib import error, request
from urllib.parse import urlparse


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify SCENE runner callback and artifact assumptions from inside a runner runtime."
    )
    parser.add_argument(
        "--callback-url",
        default=(
            os.environ.get("SCENE_CALLBACK_URL")
            or os.environ.get("SCENE_K3S_SERVICE_URL")
            or os.environ.get("SCENE_HOST_URL")
        ),
        help="SCENE callback base URL or /api/orchestrator/ping URL reachable from the runner.",
    )
    parser.add_argument(
        "--artifact-dir",
        default=os.environ.get("SCENE_ARTIFACT_ROOT") or os.environ.get("SCENE_ARTIFACT_DIR"),
        help="Artifact mount path visible to the runner container/pod.",
    )
    parser.add_argument(
        "--expected-storage",
        choices=["filesystem", "pvc", "object"],
        default=os.environ.get("SCENE_ARTIFACT_STORAGE", "filesystem"),
        help="Expected artifact storage class for this runtime.",
    )
    parser.add_argument(
        "--allow-host-docker-internal",
        action="store_true",
        help="Allow host.docker.internal callbacks for local Docker smoke checks.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON.",
    )
    return parser.parse_args()


def ping_url(callback_url: str) -> str:
    cleaned = callback_url.rstrip("/")
    if cleaned.endswith("/api/orchestrator/ping"):
        return cleaned
    return cleaned + "/api/orchestrator/ping"


def check_callback(callback_url: str, *, allow_host_docker_internal: bool) -> dict[str, Any]:
    parsed = urlparse(callback_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return {
            "name": "callback",
            "ok": False,
            "message": "Callback URL must be an absolute http(s) URL.",
        }
    if parsed.hostname == "host.docker.internal" and not allow_host_docker_internal:
        return {
            "name": "callback",
            "ok": False,
            "message": "host.docker.internal is not valid for k3s runner pods.",
        }
    target = ping_url(callback_url)
    try:
        with request.urlopen(target, timeout=15) as response:
            response.read()
    except error.URLError as exc:
        return {
            "name": "callback",
            "ok": False,
            "message": f"Runner cannot reach {target}: {exc}",
        }
    return {
        "name": "callback",
        "ok": True,
        "message": f"Runner reached {target}.",
    }


def check_artifact_dir(
    artifact_dir: str | None,
    *,
    expected_storage: str,
) -> dict[str, Any]:
    if expected_storage == "object":
        return {
            "name": "artifact_write",
            "ok": False,
            "message": "Object storage is declared, but this runner readiness script only verifies mounted filesystem/PVC writes.",
        }
    if not artifact_dir:
        return {
            "name": "artifact_write",
            "ok": False,
            "message": "Artifact directory is required for filesystem/PVC runner readiness.",
        }
    root = Path(artifact_dir).expanduser()
    try:
        root.mkdir(parents=True, exist_ok=True)
        marker = root / f".scene-runner-readiness-{uuid.uuid4().hex}.tmp"
        payload = f"scene runner readiness {uuid.uuid4().hex}\n"
        marker.write_text(payload, encoding="utf-8")
        observed = marker.read_text(encoding="utf-8")
        marker.unlink(missing_ok=True)
    except Exception as exc:
        return {
            "name": "artifact_write",
            "ok": False,
            "message": f"Runner cannot write/read artifact directory {root}: {exc}",
        }
    if observed != payload:
        return {
            "name": "artifact_write",
            "ok": False,
            "message": f"Artifact readback mismatch in {root}.",
        }
    return {
        "name": "artifact_write",
        "ok": True,
        "message": f"Runner can write/read artifacts in {root}.",
    }


def main() -> int:
    args = parse_args()
    checks: list[dict[str, Any]] = []
    if not args.callback_url:
        checks.append({
            "name": "callback",
            "ok": False,
            "message": "Callback URL is required.",
        })
    else:
        checks.append(
            check_callback(
                args.callback_url,
                allow_host_docker_internal=args.allow_host_docker_internal,
            )
        )
    checks.append(
        check_artifact_dir(
            args.artifact_dir,
            expected_storage=args.expected_storage,
        )
    )
    ok = all(check["ok"] for check in checks)
    payload = {
        "status": "ok" if ok else "failed",
        "checks": checks,
    }
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        for check in checks:
            marker = "OK" if check["ok"] else "FAIL"
            print(f"{marker} {check['name']}: {check['message']}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
