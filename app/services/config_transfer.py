from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Tuple, Type

from pydantic import BaseModel, ValidationError

from app.schemas import Batch, Page, Project, SceneConfig, Task
from app.services.storage import CONFIG_COLLECTIONS, SceneRepository


CONFIG_EXPORT_FORMAT = "scene-config-v1"
RUNTIME_COLLECTIONS = ("runs", "executions", "baselines", "webhook_deliveries")
MODELS: Dict[str, Type[BaseModel]] = {
    "projects": Project,
    "pages": Page,
    "tasks": Task,
    "batches": Batch,
}


class ConfigImportValidationError(ValueError):
    def __init__(self, report: Dict[str, Any]) -> None:
        super().__init__("SCENE configuration import validation failed")
        self.report = report


def _utcnow() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _without_internal_fields(record: Mapping[str, Any]) -> Dict[str, Any]:
    clean = deepcopy(dict(record))
    clean.pop("_version", None)
    clean.pop("jira_issue", None)
    return clean


def export_configuration(repo: SceneRepository) -> Dict[str, Any]:
    records = repo.configuration_records()
    return {
        "format": CONFIG_EXPORT_FORMAT,
        "exported_at": _utcnow(),
        "config": repo.get_config(),
        **{
            collection: [
                _without_internal_fields(record)
                for record in sorted(
                    records[collection],
                    key=lambda item: (str(item.get("created_at") or ""), str(item.get("id") or "")),
                )
            ]
            for collection in CONFIG_COLLECTIONS
        },
    }


def _collection_values(document: Mapping[str, Any], collection: str) -> List[Any]:
    value = document.get(collection, [])
    if isinstance(value, dict):
        return list(value.values())
    if isinstance(value, list):
        return value
    raise ValueError(f"'{collection}' must be an object or array")


def _safe_validation_messages(
    collection: str,
    index: int,
    error: ValidationError,
) -> List[str]:
    messages = []
    for issue in error.errors(include_url=False, include_context=False, include_input=False):
        path = ".".join(str(part) for part in issue.get("loc", ())) or "record"
        messages.append(
            f"{collection}[{index}].{path}: {issue.get('msg', 'invalid value')}"
        )
    return messages


def _normalized_records(
    document: Mapping[str, Any],
    collection: str,
    errors: List[str],
) -> List[Dict[str, Any]]:
    try:
        values = _collection_values(document, collection)
    except ValueError as exc:
        errors.append(str(exc))
        return []
    model = MODELS[collection]
    records: List[Dict[str, Any]] = []
    for index, value in enumerate(values):
        if not isinstance(value, dict):
            errors.append(f"{collection}[{index}] must be an object")
            continue
        try:
            normalized = model.model_validate(value).model_dump(
                mode="json",
                exclude_unset=True,
            )
        except ValidationError as exc:
            errors.extend(_safe_validation_messages(collection, index, exc))
            continue
        records.append(_without_internal_fields(normalized))
    return records


def _duplicate_values(records: Iterable[Dict[str, Any]], field: str) -> List[str]:
    seen = set()
    duplicates = set()
    for record in records:
        value = str(record.get(field) or "").strip()
        if not value:
            continue
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    return sorted(duplicates)


def _record_count(value: Any) -> int:
    if isinstance(value, (dict, list)):
        return len(value)
    return 0


def _normalize_document(
    repo: SceneRepository,
    document: Mapping[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, List[Dict[str, Any]]], List[str]]:
    errors: List[str] = []
    source_format = document.get("format")
    if source_format not in {None, CONFIG_EXPORT_FORMAT}:
        errors.append(f"Unsupported configuration format: {source_format}")

    source_config = document.get("config") or {}
    if not isinstance(source_config, dict):
        errors.append("'config' must be an object")
        source_config = {}
    known_config = set(SceneConfig.model_fields)
    merged_config = repo.get_config()
    merged_config.update(
        {key: value for key, value in source_config.items() if key in known_config}
    )
    try:
        config = SceneConfig.model_validate(merged_config).model_dump(mode="json")
    except ValidationError as exc:
        errors.extend(_safe_validation_messages("config", 0, exc))
        config = repo.get_config()

    records = {
        collection: _normalized_records(document, collection, errors)
        for collection in CONFIG_COLLECTIONS
    }

    for collection, values in records.items():
        duplicate_ids = _duplicate_values(values, "id")
        if duplicate_ids:
            errors.append(
                f"{collection} contains duplicate ids: " + ", ".join(duplicate_ids)
            )
    duplicate_slugs = _duplicate_values(records["projects"], "slug")
    if duplicate_slugs:
        errors.append("projects contains duplicate slugs: " + ", ".join(duplicate_slugs))

    projects = {record["id"]: record for record in records["projects"]}
    pages = {record["id"]: record for record in records["pages"]}
    tasks = {record["id"]: record for record in records["tasks"]}
    configured_browsers = set(config["browsers"])
    configured_viewports = set(config["viewports"])

    for page in records["pages"]:
        if page["project_id"] not in projects:
            errors.append(
                f"page {page['id']} references unknown project {page['project_id']}"
            )
    for task in records["tasks"]:
        project = projects.get(task["project_id"])
        page = pages.get(task["page_id"])
        if project is None:
            errors.append(
                f"task {task['id']} references unknown project {task['project_id']}"
            )
        if page is None:
            errors.append(f"task {task['id']} references unknown page {task['page_id']}")
        elif page["project_id"] != task["project_id"]:
            errors.append(f"task {task['id']} references a page from another project")
        unknown_browsers = sorted(set(task.get("browsers") or []) - configured_browsers)
        if unknown_browsers:
            errors.append(
                f"task {task['id']} uses browsers absent from config: "
                + ", ".join(unknown_browsers)
            )
        task_viewports = {
            f"{viewport['width']}x{viewport['height']}"
            for viewport in task.get("viewports") or []
        }
        unknown_viewports = sorted(task_viewports - configured_viewports)
        if unknown_viewports:
            errors.append(
                f"task {task['id']} uses viewports absent from config: "
                + ", ".join(unknown_viewports)
            )
    for batch in records["batches"]:
        if batch["project_id"] not in projects:
            errors.append(
                f"batch {batch['id']} references unknown project {batch['project_id']}"
            )
        task_ids = list(batch.get("task_ids") or [])
        if len(task_ids) != len(set(task_ids)):
            errors.append(f"batch {batch['id']} contains duplicate task ids")
        for task_id in task_ids:
            task = tasks.get(task_id)
            if task is None:
                errors.append(f"batch {batch['id']} references unknown task {task_id}")
            elif task["project_id"] != batch["project_id"]:
                errors.append(
                    f"batch {batch['id']} references a task from another project"
                )
    return config, records, errors


def import_configuration(
    repo: SceneRepository,
    document: Mapping[str, Any],
    *,
    dry_run: bool = True,
) -> Dict[str, Any]:
    if not isinstance(document, Mapping):
        raise ConfigImportValidationError(
            {
                "format": CONFIG_EXPORT_FORMAT,
                "dry_run": dry_run,
                "valid": False,
                "errors": ["Configuration document must be a JSON object"],
            }
        )

    config, records, errors = _normalize_document(repo, document)
    counts: Dict[str, Dict[str, int]] = {
        collection: {"create": 0, "update": 0, "skip": 0, "error": 0}
        for collection in CONFIG_COLLECTIONS
    }
    for collection, values in records.items():
        for record in values:
            current = repo.get_configuration_record(collection, record["id"])
            if current is None:
                counts[collection]["create"] += 1
            elif _without_internal_fields(current) == record:
                counts[collection]["skip"] += 1
            else:
                counts[collection]["update"] += 1

    report: Dict[str, Any] = {
        "format": CONFIG_EXPORT_FORMAT,
        "source_format": document.get("format") or "legacy-local-state",
        "dry_run": dry_run,
        "valid": not errors,
        "counts": counts,
        "config": {
            "update": int(repo.get_config() != config),
            "skip": int(repo.get_config() == config),
        },
        "excluded": {
            collection: _record_count(document.get(collection))
            for collection in RUNTIME_COLLECTIONS
        },
        "errors": errors,
    }
    if errors:
        prefixes = {
            "projects": ("project ", "projects"),
            "pages": ("page ", "pages"),
            "tasks": ("task ", "tasks"),
            "batches": ("batch ", "batches"),
        }
        for collection, collection_prefixes in prefixes.items():
            counts[collection]["error"] = sum(
                1 for error in errors if error.startswith(collection_prefixes)
            )
        raise ConfigImportValidationError(report)

    if dry_run:
        return report

    with repo.transaction():
        for collection in CONFIG_COLLECTIONS:
            for record in records[collection]:
                current = repo.get_configuration_record(collection, record["id"])
                if current is not None and _without_internal_fields(current) == record:
                    continue
                repo.put_configuration_record(collection, record)
        if repo.get_config() != config:
            repo.replace_config(config)
    report["dry_run"] = False
    report["applied"] = True
    return report
