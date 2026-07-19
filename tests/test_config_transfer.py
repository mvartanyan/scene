from __future__ import annotations

import json
import stat
from pathlib import Path

import pytest

from app.services.config_transfer import (
    ConfigImportValidationError,
    export_configuration,
    import_configuration,
)
from app.services.storage import LocalDynamoStorage, SceneRepository
from scripts.scene_config import _write_private_json


def _repository(path: Path) -> SceneRepository:
    return SceneRepository(LocalDynamoStorage(path))


def _seed_configuration(repo: SceneRepository) -> dict[str, dict]:
    project = repo.create_project(
        {"name": "Dotint", "slug": "dotint", "description": "Baseline suite"}
    )
    page = repo.create_page(
        {
            "project_id": project["id"],
            "name": "Careers",
            "url": "https://dotint.careers/",
            "reference_url": "https://dotint.careers/",
            "basic_auth_username": "agent",
            "basic_auth_password": "never-print-me",
            "preparatory_actions": [{"type": "disable_animations"}],
        }
    )
    task = repo.create_task(
        {
            "project_id": project["id"],
            "page_id": page["id"],
            "name": "Desktop",
            "browsers": ["chromium", "firefox"],
            "viewports": [{"width": 1280, "height": 720}],
            "task_actions": [{"type": "wait", "wait_ms": 250}],
        }
    )
    batch = repo.create_batch(
        {
            "project_id": project["id"],
            "name": "Cross-browser",
            "task_ids": [task["id"]],
            "spm_ticket": "SCENE-19",
            "run_diff_threshold": 0.5,
            "execution_diff_threshold": 1.0,
        }
    )
    repo.create_run(
        {
            "project_id": project["id"],
            "batch_id": batch["id"],
            "purpose": "baseline_recording",
        }
    )
    return {"project": project, "page": page, "task": task, "batch": batch}


def test_config_export_import_round_trip_preserves_ids_and_credentials_only(
    tmp_path: Path,
) -> None:
    source = _repository(tmp_path / "source.json")
    seeded = _seed_configuration(source)
    document = export_configuration(source)
    document["runs"] = {"historic-run": {"id": "historic-run"}}
    document["executions"] = [{"id": "historic-execution"}]
    document["baselines"] = {"historic-baseline": {"id": "historic-baseline"}}
    target = _repository(tmp_path / "target.json")

    dry_run = import_configuration(target, document, dry_run=True)
    assert dry_run["valid"] is True
    assert dry_run["counts"]["projects"]["create"] == 1
    assert dry_run["excluded"] == {
        "runs": 1,
        "executions": 1,
        "baselines": 1,
        "webhook_deliveries": 0,
    }

    applied = import_configuration(target, document, dry_run=False)
    assert applied["applied"] is True
    assert target.get_project(seeded["project"]["id"])["slug"] == "dotint"  # type: ignore[index]
    imported_page = target.get_page(seeded["page"]["id"])
    assert imported_page is not None
    assert imported_page["basic_auth_username"] == "agent"
    assert imported_page["basic_auth_password"] == "never-print-me"
    assert target.list_runs() == []
    assert target.list_baselines() == []
    assert target.list_executions() == []

    replay = import_configuration(target, document, dry_run=False)
    for collection in ("projects", "pages", "tasks", "batches"):
        assert replay["counts"][collection]["skip"] == 1
        assert replay["counts"][collection]["create"] == 0
        assert replay["counts"][collection]["update"] == 0

    round_trip = export_configuration(target)
    for key in ("config", "projects", "pages", "tasks", "batches"):
        assert round_trip[key] == document[key]


def test_import_accepts_legacy_map_shape_and_rejects_broken_graph_without_writes(
    tmp_path: Path,
) -> None:
    source = _repository(tmp_path / "source.json")
    _seed_configuration(source)
    exported = export_configuration(source)
    legacy = {
        "config": exported["config"],
        **{
            collection: {record["id"]: record for record in exported[collection]}
            for collection in ("projects", "pages", "tasks", "batches")
        },
    }
    legacy["tasks"][next(iter(legacy["tasks"]))]["page_id"] = "missing-page"
    target = _repository(tmp_path / "target.json")

    with pytest.raises(ConfigImportValidationError) as raised:
        import_configuration(target, legacy, dry_run=False)

    report_text = json.dumps(raised.value.report)
    assert "unknown page missing-page" in report_text
    assert "never-print-me" not in report_text
    assert raised.value.report["counts"]["tasks"]["error"] == 1
    assert target.list_projects() == []


def test_private_export_writer_uses_owner_only_permissions(tmp_path: Path) -> None:
    output = tmp_path / "scene-config.json"
    _write_private_json(output, {"format": "scene-config-v1", "secret": "value"})

    assert stat.S_IMODE(output.stat().st_mode) == 0o600
    assert json.loads(output.read_text(encoding="utf-8"))["secret"] == "value"
