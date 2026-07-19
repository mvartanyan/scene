#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict

from app.services.config_transfer import (
    ConfigImportValidationError,
    export_configuration,
    import_configuration,
)
from app.services.storage import get_repository


def _write_private_json(path: Path, document: Dict[str, Any]) -> None:
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(document, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _read_json(path: Path) -> Dict[str, Any]:
    with path.expanduser().open("r", encoding="utf-8") as handle:
        document = json.load(handle)
    if not isinstance(document, dict):
        raise ValueError("Configuration document must be a JSON object")
    return document


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export or import SCENE configuration without runtime history."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    export = commands.add_parser("export", help="Write a private config-only JSON export.")
    export.add_argument("--output", required=True, type=Path)
    importer = commands.add_parser("import", help="Validate or apply a config-only JSON import.")
    importer.add_argument("--input", required=True, type=Path)
    importer.add_argument(
        "--apply",
        action="store_true",
        help="Apply after validation. Without this flag, import is a dry run.",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    repo = get_repository()
    if args.command == "export":
        _write_private_json(args.output, export_configuration(repo))
        print(
            json.dumps(
                {
                    "ok": True,
                    "output": str(args.output.expanduser().resolve()),
                    "mode": "0600",
                    "backend": repo.backend_info(),
                },
                sort_keys=True,
            )
        )
        return 0

    try:
        report = import_configuration(
            repo,
            _read_json(args.input),
            dry_run=not args.apply,
        )
    except (ConfigImportValidationError, ValueError, json.JSONDecodeError) as exc:
        if isinstance(exc, ConfigImportValidationError):
            report = exc.report
        else:
            report = {"valid": False, "errors": [str(exc)]}
        print(json.dumps(report, indent=2, sort_keys=True))
        return 2
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
