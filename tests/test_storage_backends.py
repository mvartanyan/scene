from __future__ import annotations

from contextlib import AbstractContextManager
from copy import deepcopy
from pathlib import Path
from threading import Barrier, Lock, Thread
from types import SimpleNamespace
from typing import Any, Dict, Optional

import pytest
from botocore.exceptions import ClientError

from app.services.dynamodb_storage import DynamoDBStorage
from app.services.storage import LocalDynamoStorage, SceneRepository, _build_storage_backend
from app.services.storage_types import InvalidStorageCursorError, StorageConflictError


class _FakeBatchWriter(AbstractContextManager):
    def __init__(self, table: "_FakeDynamoTable") -> None:
        self.table = table

    def __enter__(self) -> "_FakeBatchWriter":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def delete_item(self, *, Key: Dict[str, Any]) -> None:
        self.table.delete_item(Key=Key)


class _FakeDynamoClient:
    def __init__(self, *, invalid_index: Optional[str] = None) -> None:
        self.invalid_index = invalid_index

    def describe_table(self, *, TableName: str) -> Dict[str, Any]:
        indexes = []
        for number in (1, 2, 3):
            name = f"gsi{number}"
            hash_key = f"{name}pk"
            range_key = f"{name}sk"
            if self.invalid_index == name:
                hash_key = "wrong"
            indexes.append(
                {
                    "IndexName": name,
                    "KeySchema": [
                        {"AttributeName": hash_key, "KeyType": "HASH"},
                        {"AttributeName": range_key, "KeyType": "RANGE"},
                    ],
                }
            )
        return {
            "Table": {
                "TableName": TableName,
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"},
                ],
                "GlobalSecondaryIndexes": indexes,
            }
        }


class _FakeDynamoTable:
    def __init__(self, *, invalid_index: Optional[str] = None) -> None:
        self.items: Dict[tuple[str, str], Dict[str, Any]] = {}
        self.meta = SimpleNamespace(client=_FakeDynamoClient(invalid_index=invalid_index))
        self.queries: list[Dict[str, Any]] = []

    @staticmethod
    def _key(item: Dict[str, Any]) -> tuple[str, str]:
        return str(item["pk"]), str(item["sk"])

    @staticmethod
    def _conditional_failure() -> ClientError:
        return ClientError(
            {
                "Error": {
                    "Code": "ConditionalCheckFailedException",
                    "Message": "condition failed",
                }
            },
            "PutItem",
        )

    def put_item(self, **request: Any) -> Dict[str, Any]:
        item = deepcopy(request["Item"])
        key = self._key(item)
        current = self.items.get(key)
        condition = request.get("ConditionExpression", "")
        expected = request.get("ExpressionAttributeValues", {}).get(":expected")
        if condition.startswith("attribute_not_exists"):
            if current is not None:
                raise self._conditional_failure()
        elif expected == 0:
            if current is None or int(current.get("version", 0)) != 0:
                raise self._conditional_failure()
        elif current is None or int(current.get("version", 0)) != int(expected):
            raise self._conditional_failure()
        self.items[key] = item
        return {}

    def get_item(self, *, Key: Dict[str, Any], **_kwargs: Any) -> Dict[str, Any]:
        item = self.items.get(self._key(Key))
        return {"Item": deepcopy(item)} if item is not None else {}

    def delete_item(self, *, Key: Dict[str, Any]) -> Dict[str, Any]:
        self.items.pop(self._key(Key), None)
        return {}

    def batch_writer(self) -> _FakeBatchWriter:
        return _FakeBatchWriter(self)

    def query(self, **request: Any) -> Dict[str, Any]:
        self.queries.append(dict(request))
        expression = request["KeyConditionExpression"]
        attribute = expression._values[0].name
        expected = expression._values[1]
        sort_attribute = (
            f"{request['IndexName']}sk" if request.get("IndexName") else "sk"
        )
        matches = [
            deepcopy(item)
            for item in self.items.values()
            if item.get(attribute) == expected
        ]
        matches.sort(
            key=lambda item: str(item.get(sort_attribute) or ""),
            reverse=not request.get("ScanIndexForward", True),
        )
        start = 0
        exclusive = request.get("ExclusiveStartKey")
        if exclusive:
            exclusive_key = self._key(exclusive)
            for index, item in enumerate(matches):
                if self._key(item) == exclusive_key:
                    start = index + 1
                    break
        limit = int(request.get("Limit") or len(matches) or 1)
        page = matches[start : start + limit]
        response: Dict[str, Any] = (
            {"Count": len(page)} if request.get("Select") == "COUNT" else {"Items": page}
        )
        if start + len(page) < len(matches):
            last = page[-1]
            response["LastEvaluatedKey"] = {
                "pk": last["pk"],
                "sk": last["sk"],
                attribute: last[attribute],
                sort_attribute: last[sort_attribute],
            }
        return response


def _dynamo_storage(table: _FakeDynamoTable, *, validate_table: bool = True) -> DynamoDBStorage:
    return DynamoDBStorage(
        table_name="scene-test",
        region_name="eu-central-1",
        table=table,
        validate_table=validate_table,
        default_config_factory=lambda: {"browsers": ["chromium"]},
        config_override=lambda config: dict(config),
    )


def test_local_storage_detects_stale_writes_and_pages(tmp_path: Path) -> None:
    storage = LocalDynamoStorage(tmp_path / "state.json")
    first = storage.upsert(
        "projects",
        "project-1",
        {"id": "project-1", "name": "One", "created_at": "2026-01-01"},
    )
    stale = dict(first)
    first["name"] = "First writer"
    saved = storage.upsert("projects", "project-1", first)
    stale["name"] = "Stale writer"

    with pytest.raises(StorageConflictError):
        storage.upsert("projects", "project-1", stale)

    assert saved["_version"] == 2
    storage.upsert(
        "projects",
        "project-2",
        {"id": "project-2", "name": "Two", "created_at": "2026-01-02"},
    )
    page, cursor = storage.query_page("projects", limit=1)
    assert [item["id"] for item in page] == ["project-1"]
    assert cursor is not None
    next_page, next_cursor = storage.query_page("projects", limit=1, cursor=cursor)
    assert [item["id"] for item in next_page] == ["project-2"]
    assert next_cursor is None
    with pytest.raises(InvalidStorageCursorError):
        storage.query_page("projects", cursor="not-a-cursor")


def test_repository_retries_a_conflicting_update_without_losing_fields(tmp_path: Path) -> None:
    class RacingStorage(LocalDynamoStorage):
        armed = False

        def upsert(self, collection: str, item_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
            if self.armed and collection == "projects":
                self.armed = False
                concurrent = self.get(collection, item_id)
                assert concurrent is not None
                concurrent["description"] = "concurrent change"
                super().upsert(collection, item_id, concurrent)
            return super().upsert(collection, item_id, payload)

    storage = RacingStorage(tmp_path / "state.json")
    repo = SceneRepository(storage)
    project = repo.create_project({"name": "Before", "slug": "before"})
    storage.armed = True

    updated = repo.update_project(project["id"], {"name": "After"})

    assert updated is not None
    assert updated["name"] == "After"
    assert updated["description"] == "concurrent change"


def test_repository_idempotency_keys_replay_same_creation_and_reject_reuse(
    tmp_path: Path,
) -> None:
    repo = SceneRepository(LocalDynamoStorage(tmp_path / "state.json"))
    project = repo.create_project({"name": "Project", "slug": "project"})
    batch = repo.create_batch(
        {"project_id": project["id"], "name": "Batch", "task_ids": []}
    )
    payload = {
        "project_id": project["id"],
        "batch_id": batch["id"],
        "purpose": "comparison",
        "idempotency_key": "spm-invocation-1",
    }

    first = repo.create_run(payload)
    replay = repo.create_run(payload)
    assert replay["id"] == first["id"]
    assert len(repo.list_runs()) == 1

    with pytest.raises(StorageConflictError):
        repo.create_run({**payload, "purpose": "baseline_recording"})


def test_cancel_and_complete_race_converges_on_one_terminal_execution_state(
    tmp_path: Path,
) -> None:
    barrier = Barrier(2)
    gate_lock = Lock()

    class RacingReadStorage(LocalDynamoStorage):
        synchronize = False
        gated_reads = 0

        def get(self, collection: str, item_id: str) -> Optional[Dict[str, Any]]:
            record = super().get(collection, item_id)
            should_wait = False
            if self.synchronize and collection == "executions":
                with gate_lock:
                    if self.gated_reads < 2:
                        self.gated_reads += 1
                        should_wait = True
            if should_wait:
                barrier.wait(timeout=2)
            return record

    storage = RacingReadStorage(tmp_path / "state.json")
    repo = SceneRepository(storage)
    execution = repo.create_execution(
        {
            "run_id": "run-1",
            "project_id": "project-1",
            "batch_id": "batch-1",
            "task_id": "task-1",
            "task_name": "Task",
            "browser": "chromium",
            "viewport": {"width": 1280, "height": 720},
            "status": "executing",
        }
    )
    storage.synchronize = True
    errors: list[Exception] = []

    def transition(status: str) -> None:
        try:
            repo.update_execution(execution["id"], {"status": status})
        except Exception as exc:  # pragma: no cover - assertion captures thread failures
            errors.append(exc)

    threads = [Thread(target=transition, args=(status,)) for status in ("finished", "cancelled")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=3)

    assert errors == []
    assert all(not thread.is_alive() for thread in threads)
    final = repo.get_execution(execution["id"])
    assert final is not None
    assert final["status"] in {"finished", "cancelled"}
    losing_status = "cancelled" if final["status"] == "finished" else "finished"
    replay = repo.update_execution(execution["id"], {"status": losing_status})
    assert replay is not None
    assert replay["status"] == final["status"]


def test_dynamodb_backend_uses_indexes_conditional_versions_and_opaque_pages() -> None:
    table = _FakeDynamoTable()
    storage = _dynamo_storage(table)
    first = storage.upsert(
        "runs",
        "run-1",
        {
            "id": "run-1",
            "project_id": "project-1",
            "batch_id": "batch-1",
            "created_at": "2026-01-01T00:00:00+00:00",
            "score": 1.25,
        },
    )
    second = storage.upsert(
        "runs",
        "run-2",
        {
            "id": "run-2",
            "project_id": "project-1",
            "batch_id": "batch-1",
            "created_at": "2026-01-02T00:00:00+00:00",
        },
    )
    assert storage.get("runs", "run-1")["score"] == 1.25  # type: ignore[index]

    stale = dict(first)
    first["status"] = "executing"
    storage.upsert("runs", "run-1", first)
    with pytest.raises(StorageConflictError):
        storage.upsert("runs", "run-1", stale)

    by_project = storage.filter("runs", key="project_id", value="project-1")
    assert {run["id"] for run in by_project} == {"run-1", "run-2"}
    assert table.queries[-1]["IndexName"] == "gsi1"
    first_page, cursor = storage.query_page(
        "runs", key="batch_id", value="batch-1", limit=1, descending=True
    )
    assert [item["id"] for item in first_page] == [second["id"]]
    assert cursor and not cursor.isdigit()
    second_page, next_cursor = storage.query_page(
        "runs",
        key="batch_id",
        value="batch-1",
        limit=1,
        cursor=cursor,
        descending=True,
    )
    assert [item["id"] for item in second_page] == [first["id"]]
    assert next_cursor is None
    assert table.queries[-1]["IndexName"] == "gsi2"
    assert storage.count("runs", key="project_id", value="project-1") == 2
    assert table.queries[-1]["Select"] == "COUNT"
    with pytest.raises(InvalidStorageCursorError):
        storage.query_page("runs", cursor="%%%")

    assert storage.probe()["write_read_delete"] is True
    assert not any(key[0] == "COLLECTION#probes" for key in table.items)


def test_dynamodb_backend_fails_closed_on_wrong_index_schema() -> None:
    with pytest.raises(RuntimeError, match="incorrectly keyed indexes: gsi2"):
        _dynamo_storage(_FakeDynamoTable(invalid_index="gsi2"))


def test_storage_backend_selector_rejects_invalid_or_incomplete_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("SCENE_STATE_BACKEND", "json")
    monkeypatch.setenv("SCENE_STATE_PATH", str(tmp_path / "state.json"))
    assert isinstance(_build_storage_backend(), LocalDynamoStorage)

    monkeypatch.setenv("SCENE_STATE_BACKEND", "dynamodb")
    monkeypatch.delenv("SCENE_DYNAMODB_TABLE", raising=False)
    with pytest.raises(RuntimeError, match="SCENE_DYNAMODB_TABLE is required"):
        _build_storage_backend()

    monkeypatch.setenv("SCENE_STATE_BACKEND", "postgres")
    with pytest.raises(RuntimeError, match="Unsupported SCENE_STATE_BACKEND"):
        _build_storage_backend()
