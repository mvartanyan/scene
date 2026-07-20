from __future__ import annotations

from contextlib import AbstractContextManager
from copy import deepcopy
from pathlib import Path
from threading import Barrier, Event, Lock, Thread, current_thread
from types import SimpleNamespace
from typing import Any, Dict, Optional

import pytest
from botocore.exceptions import ClientError

from app.services import storage as storage_module
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
        self.put_requests: list[Dict[str, Any]] = []
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
        self.put_requests.append(deepcopy(request))
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


def test_accepted_callback_wins_stale_infrastructure_failure_race(
    tmp_path: Path,
) -> None:
    failure_read = Event()
    callback_saved = Event()

    class RacingStorage(LocalDynamoStorage):
        synchronize = False

        def get(self, collection: str, item_id: str) -> Optional[Dict[str, Any]]:
            record = super().get(collection, item_id)
            if (
                self.synchronize
                and collection == "executions"
                and current_thread().name == "failure-finalizer"
                and not failure_read.is_set()
            ):
                failure_read.set()
                assert callback_saved.wait(timeout=2)
            return record

    storage = RacingStorage(tmp_path / "state.json")
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
    token_sha256 = "a" * 64
    repo.update_execution(
        execution["id"],
        {
            "dispatch_generation": 1,
            "callback_token_sha256": token_sha256,
            "callback_expires_at": "2999-01-01T00:00:00+00:00",
            "callback_state": "pending",
        },
    )
    storage.synchronize = True
    failures: list[Optional[Dict[str, Any]]] = []

    thread = Thread(
        name="failure-finalizer",
        target=lambda: failures.append(
            repo.finalize_execution_infrastructure_failure(
                execution["id"],
                status="failed",
                completed_at="2026-07-20T12:00:00+00:00",
                message="Kubernetes Job failed",
            )
        ),
    )
    thread.start()
    assert failure_read.wait(timeout=2)
    try:
        outcome, accepted = repo.accept_execution_callback(
            execution["id"],
            run_id="run-1",
            generation=1,
            token_sha256=token_sha256,
            result={"status": "ok"},
        )
    finally:
        callback_saved.set()
    thread.join(timeout=3)

    assert not thread.is_alive()
    assert outcome == "accepted"
    assert accepted is not None
    assert failures == [None]
    final = repo.get_execution(execution["id"])
    assert final is not None
    assert final["status"] == "executing"
    assert final["callback_state"] == "accepted"
    assert final.get("message") != "Kubernetes Job failed"


def test_task_delete_preserves_run_execution_baseline_and_artifact_history(
    tmp_path: Path,
) -> None:
    repo = SceneRepository(LocalDynamoStorage(tmp_path / "state.json"))
    project = repo.create_project({"name": "Project", "slug": "project"})
    page = repo.create_page(
        {"project_id": project["id"], "name": "Page", "url": "https://example.test"}
    )
    task = repo.create_task(
        {
            "project_id": project["id"],
            "page_id": page["id"],
            "name": "Task",
            "browsers": ["chromium"],
            "viewports": [{"width": 1280, "height": 720}],
        }
    )
    batch = repo.create_batch(
        {"project_id": project["id"], "name": "Batch", "task_ids": [task["id"]]}
    )
    run = repo.create_run(
        {
            "project_id": project["id"],
            "batch_id": batch["id"],
            "purpose": "comparison",
            "status": "executing",
            "task_ids": [task["id"]],
        }
    )
    execution = repo.create_execution(
        {
            "run_id": run["id"],
            "project_id": project["id"],
            "batch_id": batch["id"],
            "task_id": task["id"],
            "task_name": task["name"],
            "page_id": page["id"],
            "browser": "chromium",
            "viewport": {"width": 1280, "height": 720},
            "status": "executing",
            "artifacts": {
                "observed": {"kind": "observed", "key": "runs/observed.png"}
            },
        }
    )
    repo.update_execution(
        execution["id"],
        {"artifact_transfer": {"outputs": {"result": {"key": "runs/result.json"}}}},
    )
    baseline = repo.create_baseline(
        {
            "project_id": project["id"],
            "batch_id": batch["id"],
            "status": "completed",
            "items": [
                {
                    "task_id": task["id"],
                    "artifact": {
                        "kind": "baseline",
                        "key": "baselines/reference.png",
                    },
                }
            ],
        }
    )

    repo.delete_task(task["id"])

    assert repo.get_task(task["id"]) is None
    assert repo.get_batch(batch["id"])["task_ids"] == []
    assert repo.get_run(run["id"])["task_ids"] == [task["id"]]
    preserved_execution = repo.get_execution(execution["id"])
    assert preserved_execution is not None
    assert preserved_execution["status"] == "executing"
    assert preserved_execution["artifacts"]["observed"]["key"] == "runs/observed.png"
    assert preserved_execution["artifact_transfer"]["outputs"]["result"]["key"] == "runs/result.json"
    assert repo.get_baseline(baseline["id"])["items"][0]["task_id"] == task["id"]


def test_latest_baseline_uses_strong_read_and_skips_incomplete_records(
    tmp_path: Path,
) -> None:
    class StaleBaselineIndexStorage(LocalDynamoStorage):
        def filter(self, collection: str, *, key: str, value: Any) -> list[Dict[str, Any]]:
            if collection == "baselines":
                raise AssertionError("baseline selection must not use the batch GSI")
            return super().filter(collection, key=key, value=value)

    repo = SceneRepository(StaleBaselineIndexStorage(tmp_path / "state.json"))
    completed_old = repo.create_baseline(
        {
            "project_id": "project-1",
            "batch_id": "batch-1",
            "status": "completed",
        }
    )
    completed_new = repo.create_baseline(
        {
            "project_id": "project-1",
            "batch_id": "batch-1",
            "status": "completed",
        }
    )
    pending = repo.create_baseline(
        {
            "project_id": "project-1",
            "batch_id": "batch-1",
            "status": "pending",
        }
    )
    failed = repo.create_baseline(
        {
            "project_id": "project-1",
            "batch_id": "batch-1",
            "status": "failed",
        }
    )
    repo.update_baseline(completed_old["id"], {"created_at": "2026-01-01T00:00:00+00:00"})
    repo.update_baseline(completed_new["id"], {"created_at": "2026-01-02T00:00:00+00:00"})
    repo.update_baseline(pending["id"], {"created_at": "2026-01-04T00:00:00+00:00"})
    repo.update_baseline(failed["id"], {"created_at": "2026-01-03T00:00:00+00:00"})

    latest = repo.get_latest_baseline("batch-1")

    assert latest is not None
    assert latest["id"] == completed_new["id"]


def test_project_delete_purges_terminal_run_and_baseline_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeArtifactStore:
        def __init__(self) -> None:
            self.runs: list[tuple[str, list[Dict[str, Any]]]] = []
            self.baselines: list[tuple[str, list[Dict[str, Any]]]] = []

        def purge_run(
            self,
            run_id: str,
            artifacts: list[Dict[str, Any]],
        ) -> None:
            self.runs.append((run_id, artifacts))

        def purge_baseline(
            self,
            baseline_id: str,
            artifacts: list[Dict[str, Any]],
        ) -> None:
            self.baselines.append((baseline_id, artifacts))

    artifact_store = FakeArtifactStore()
    monkeypatch.setattr(storage_module, "get_artifact_store", lambda: artifact_store)
    repo = SceneRepository(LocalDynamoStorage(tmp_path / "state.json"))
    project = repo.create_project({"name": "Project", "slug": "project"})
    batch = repo.create_batch(
        {"project_id": project["id"], "name": "Batch", "task_ids": []}
    )
    run = repo.create_run(
        {
            "project_id": project["id"],
            "batch_id": batch["id"],
            "purpose": "comparison",
        }
    )
    execution = repo.create_execution(
        {
            "run_id": run["id"],
            "project_id": project["id"],
            "batch_id": batch["id"],
            "task_id": "task-1",
            "task_name": "Task",
            "browser": "chromium",
            "viewport": {"width": 1280, "height": 720},
            "status": "finished",
            "artifacts": {
                "observed": {"kind": "observed", "key": "run/observed.png"}
            },
        }
    )
    repo.update_execution(
        execution["id"],
        {
            "artifact_transfer": {
                "outputs": {
                    "result": {
                        "key": "run/result.json",
                        "content_type": "application/json",
                    }
                }
            }
        },
    )
    baseline = repo.create_baseline(
        {
            "project_id": project["id"],
            "batch_id": batch["id"],
            "run_id": run["id"],
            "status": "completed",
            "items": [
                {
                    "artifact": {
                        "kind": "baseline",
                        "key": "baseline/reference.png",
                    }
                }
            ],
        }
    )
    repo.update_run(run["id"], {"status": "finished", "baseline_id": baseline["id"]})

    repo.delete_project(project["id"])

    assert artifact_store.runs == [
        (
            run["id"],
            [
                {"kind": "observed", "key": "run/observed.png"},
                {"key": "run/result.json", "content_type": "application/json"},
            ],
        )
    ]
    assert artifact_store.baselines == [
        (
            baseline["id"],
            [{"kind": "baseline", "key": "baseline/reference.png"}],
        )
    ]
    assert repo.get_project(project["id"]) is None
    assert repo.get_batch(batch["id"]) is None
    assert repo.get_run(run["id"]) is None
    assert repo.get_execution(execution["id"]) is None
    assert repo.get_baseline(baseline["id"]) is None


def test_run_delete_waits_for_kubernetes_cleanup(tmp_path: Path) -> None:
    repo = SceneRepository(LocalDynamoStorage(tmp_path / "state.json"))
    project = repo.create_project({"name": "Project", "slug": "project"})
    batch = repo.create_batch(
        {"project_id": project["id"], "name": "Batch", "task_ids": []}
    )
    run = repo.create_run(
        {
            "project_id": project["id"],
            "batch_id": batch["id"],
            "purpose": "comparison",
        }
    )
    execution = repo.create_execution(
        {
            "run_id": run["id"],
            "project_id": project["id"],
            "batch_id": batch["id"],
            "task_id": "task-1",
            "task_name": "Task",
            "browser": "chromium",
            "viewport": {"width": 1280, "height": 720},
            "status": "failed",
        }
    )
    repo.update_execution(
        execution["id"],
        {
            "kubernetes_job_name": "scene-exec-1",
            "kubernetes_secret_name": "scene-exec-1",
        },
    )
    repo.update_run(run["id"], {"status": "failed"})

    with pytest.raises(ValueError, match="Kubernetes cleanup is pending"):
        repo.delete_run(run["id"])
    assert repo.get_run(run["id"]) is not None

    repo.update_execution(execution["id"], {"kubernetes_cleaned_at": "2026-01-01T00:00:00Z"})
    repo.delete_run(run["id"])
    assert repo.get_run(run["id"]) is None


def test_run_delete_waits_for_presigned_upload_expiry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RecordingStore:
        def __init__(self) -> None:
            self.purged: list[str] = []

        def purge_run(self, run_id: str, artifacts: list[Dict[str, Any]]) -> None:
            self.purged.append(run_id)

        def purge_baseline(self, baseline_id: str, artifacts: list[Dict[str, Any]]) -> None:
            return None

    store = RecordingStore()
    monkeypatch.setattr(storage_module, "get_artifact_store", lambda: store)
    repo = SceneRepository(LocalDynamoStorage(tmp_path / "state.json"))
    project = repo.create_project({"name": "Project", "slug": "project"})
    batch = repo.create_batch(
        {"project_id": project["id"], "name": "Batch", "task_ids": []}
    )
    run = repo.create_run(
        {
            "project_id": project["id"],
            "batch_id": batch["id"],
            "purpose": "comparison",
            "status": "failed",
        }
    )
    execution = repo.create_execution(
        {
            "run_id": run["id"],
            "project_id": project["id"],
            "batch_id": batch["id"],
            "task_id": "task-1",
            "task_name": "Task",
            "browser": "chromium",
            "viewport": {"width": 1280, "height": 720},
            "status": "failed",
        }
    )
    repo.update_execution(
        execution["id"],
        {
            "artifact_transfer": {
                "expires_at": "2999-01-01T00:00:00Z",
                "outputs": {"result": {"key": "runs/result.json"}},
            }
        },
    )

    with pytest.raises(ValueError, match=r"upload URL is still valid; retry after"):
        repo.delete_run(run["id"])

    assert store.purged == []
    assert repo.get_run(run["id"]) is not None
    assert repo.get_execution(execution["id"]) is not None

    repo.update_execution(
        execution["id"],
        {
            "artifact_transfer": {
                "expires_at": "2000-01-01T00:00:00Z",
                "outputs": {"result": {"key": "runs/result.json"}},
            }
        },
    )
    repo.delete_run(run["id"])

    assert store.purged == [run["id"]]
    assert repo.get_run(run["id"]) is None
    assert repo.get_execution(execution["id"]) is None


def test_project_delete_uses_consistent_collection_reads_when_indexes_are_stale(
    tmp_path: Path,
) -> None:
    class StaleIndexStorage(LocalDynamoStorage):
        def filter(self, collection: str, *, key: str, value: Any) -> list[Dict[str, Any]]:
            return []

    repo = SceneRepository(StaleIndexStorage(tmp_path / "state.json"))
    project = repo.create_project({"name": "Project", "slug": "project"})
    page = repo.create_page(
        {"project_id": project["id"], "name": "Page", "url": "https://example.test"}
    )
    task = repo.create_task(
        {
            "project_id": project["id"],
            "page_id": page["id"],
            "name": "Task",
            "browsers": ["chromium"],
            "viewports": [{"width": 800, "height": 600}],
        }
    )
    batch = repo.create_batch(
        {"project_id": project["id"], "name": "Batch", "task_ids": [task["id"]]}
    )
    run = repo.create_run(
        {
            "project_id": project["id"],
            "batch_id": batch["id"],
            "purpose": "baseline_recording",
            "status": "finished",
        }
    )
    execution = repo.create_execution(
        {
            "run_id": run["id"],
            "project_id": project["id"],
            "batch_id": batch["id"],
            "task_id": task["id"],
            "task_name": task["name"],
            "browser": "chromium",
            "viewport": {"width": 800, "height": 600},
            "status": "finished",
        }
    )
    baseline = repo.create_baseline(
        {
            "project_id": project["id"],
            "batch_id": batch["id"],
            "run_id": run["id"],
            "status": "completed",
            "items": [],
        }
    )
    repo.update_run(run["id"], {"baseline_id": baseline["id"]})

    repo.delete_project(project["id"])

    assert repo.get_project(project["id"]) is None
    assert repo.get_page(page["id"]) is None
    assert repo.get_task(task["id"]) is None
    assert repo.get_batch(batch["id"]) is None
    assert repo.get_run(run["id"]) is None
    assert repo.get_execution(execution["id"]) is None
    assert repo.get_baseline(baseline["id"]) is None


def test_parent_delete_preflights_every_run_before_purging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RecordingStore:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str]] = []

        def purge_run(self, run_id: str, artifacts: list[Dict[str, Any]]) -> None:
            self.calls.append(("run", run_id))

        def purge_baseline(self, baseline_id: str, artifacts: list[Dict[str, Any]]) -> None:
            self.calls.append(("baseline", baseline_id))

    store = RecordingStore()
    monkeypatch.setattr(storage_module, "get_artifact_store", lambda: store)
    repo = SceneRepository(LocalDynamoStorage(tmp_path / "state.json"))
    project = repo.create_project({"name": "Project", "slug": "project"})
    batch = repo.create_batch(
        {"project_id": project["id"], "name": "Batch", "task_ids": []}
    )
    runs = [
        repo.create_run(
            {
                "project_id": project["id"],
                "batch_id": batch["id"],
                "purpose": "comparison",
                "status": "failed",
            }
        )
        for _ in range(2)
    ]
    first = repo.create_execution(
        {
            "run_id": runs[0]["id"],
            "project_id": project["id"],
            "batch_id": batch["id"],
            "task_id": "task-1",
            "task_name": "Task",
            "browser": "chromium",
            "viewport": {"width": 800, "height": 600},
            "status": "failed",
        }
    )
    second = repo.create_execution(
        {
            "run_id": runs[1]["id"],
            "project_id": project["id"],
            "batch_id": batch["id"],
            "task_id": "task-2",
            "task_name": "Task",
            "browser": "chromium",
            "viewport": {"width": 800, "height": 600},
            "status": "failed",
        }
    )
    repo.update_execution(
        second["id"],
        {"kubernetes_job_name": "scene-exec-pending"},
    )

    with pytest.raises(ValueError, match="cleanup is pending"):
        repo.delete_project(project["id"])

    assert store.calls == []
    assert repo.get_project(project["id"]) is not None
    assert repo.get_run(runs[0]["id"]) is not None
    assert repo.get_execution(first["id"]) is not None


def test_batch_delete_uses_consistent_baseline_reads_when_index_is_stale(
    tmp_path: Path,
) -> None:
    class StaleIndexStorage(LocalDynamoStorage):
        def filter(self, collection: str, *, key: str, value: Any) -> list[Dict[str, Any]]:
            return []

    repo = SceneRepository(StaleIndexStorage(tmp_path / "state.json"))
    project = repo.create_project({"name": "Project", "slug": "project"})
    batch = repo.create_batch(
        {"project_id": project["id"], "name": "Batch", "task_ids": []}
    )
    run = repo.create_run(
        {
            "project_id": project["id"],
            "batch_id": batch["id"],
            "purpose": "baseline_recording",
            "status": "finished",
        }
    )
    baseline = repo.create_baseline(
        {
            "project_id": project["id"],
            "batch_id": batch["id"],
            "run_id": run["id"],
            "status": "completed",
            "items": [],
        }
    )

    repo.delete_batch(batch["id"])

    assert repo.get_batch(batch["id"]) is None
    assert repo.get_run(run["id"]) is None
    assert repo.get_baseline(baseline["id"]) is None


def test_deleting_recording_run_preserves_baseline_used_by_comparison(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RecordingStore:
        def __init__(self) -> None:
            self.baselines: list[str] = []

        def purge_run(self, run_id: str, artifacts: list[Dict[str, Any]]) -> None:
            return None

        def purge_baseline(self, baseline_id: str, artifacts: list[Dict[str, Any]]) -> None:
            self.baselines.append(baseline_id)

    store = RecordingStore()
    monkeypatch.setattr(storage_module, "get_artifact_store", lambda: store)
    repo = SceneRepository(LocalDynamoStorage(tmp_path / "state.json"))
    project = repo.create_project({"name": "Project", "slug": "project"})
    batch = repo.create_batch(
        {"project_id": project["id"], "name": "Batch", "task_ids": []}
    )
    recording = repo.create_run(
        {
            "project_id": project["id"],
            "batch_id": batch["id"],
            "purpose": "baseline_recording",
            "status": "finished",
        }
    )
    baseline = repo.create_baseline(
        {
            "project_id": project["id"],
            "batch_id": batch["id"],
            "run_id": recording["id"],
            "status": "completed",
            "items": [],
        }
    )
    repo.update_run(recording["id"], {"baseline_id": baseline["id"]})
    comparison = repo.create_run(
        {
            "project_id": project["id"],
            "batch_id": batch["id"],
            "baseline_id": baseline["id"],
            "purpose": "comparison",
            "status": "finished",
        }
    )

    repo.delete_run(recording["id"], cascade_baseline=True)

    assert repo.get_run(recording["id"]) is None
    assert repo.get_run(comparison["id"]) is not None
    assert repo.get_baseline(baseline["id"]) is not None
    assert store.baselines == []


def test_direct_baseline_delete_rejects_run_references(
    tmp_path: Path,
) -> None:
    repo = SceneRepository(LocalDynamoStorage(tmp_path / "state.json"))
    project = repo.create_project({"name": "Project", "slug": "project"})
    batch = repo.create_batch(
        {"project_id": project["id"], "name": "Batch", "task_ids": []}
    )
    baseline = repo.create_baseline(
        {
            "project_id": project["id"],
            "batch_id": batch["id"],
            "status": "completed",
            "items": [],
        }
    )
    run = repo.create_run(
        {
            "project_id": project["id"],
            "batch_id": batch["id"],
            "baseline_id": baseline["id"],
            "purpose": "comparison",
            "status": "finished",
        }
    )

    with pytest.raises(ValueError, match="referenced by a run"):
        repo.delete_baseline(baseline["id"])

    assert repo.get_baseline(baseline["id"]) is not None
    assert repo.get_run(run["id"]) is not None


def test_baseline_purge_failure_preserves_run_and_execution_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailingStore:
        def purge_run(self, run_id: str, artifacts: list[Dict[str, Any]]) -> None:
            return None

        def purge_baseline(self, baseline_id: str, artifacts: list[Dict[str, Any]]) -> None:
            raise RuntimeError("S3 artifact deletion failed: AccessDenied")

    monkeypatch.setattr(storage_module, "get_artifact_store", lambda: FailingStore())
    repo = SceneRepository(LocalDynamoStorage(tmp_path / "state.json"))
    project = repo.create_project({"name": "Project", "slug": "project"})
    batch = repo.create_batch(
        {"project_id": project["id"], "name": "Batch", "task_ids": []}
    )
    run = repo.create_run(
        {
            "project_id": project["id"],
            "batch_id": batch["id"],
            "purpose": "baseline_recording",
            "status": "failed",
        }
    )
    execution = repo.create_execution(
        {
            "run_id": run["id"],
            "project_id": project["id"],
            "batch_id": batch["id"],
            "task_id": "task",
            "task_name": "Task",
            "browser": "chromium",
            "viewport": {"width": 800, "height": 600},
            "status": "failed",
        }
    )
    baseline = repo.create_baseline(
        {
            "project_id": project["id"],
            "batch_id": batch["id"],
            "run_id": run["id"],
            "status": "failed",
            "items": [],
        }
    )
    repo.update_run(run["id"], {"baseline_id": baseline["id"]})

    with pytest.raises(RuntimeError, match="AccessDenied"):
        repo.delete_run(run["id"], cascade_baseline=True)

    assert repo.get_run(run["id"]) is not None
    assert repo.get_execution(execution["id"]) is not None
    assert repo.get_baseline(baseline["id"]) is not None


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
    assert table.put_requests[-1]["ExpressionAttributeNames"] == {
        "#pk": "pk",
        "#sk": "sk",
    }
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
    assert table.put_requests[-1]["ExpressionAttributeNames"] == {
        "#pk": "pk",
        "#version": "version",
    }
    with pytest.raises(StorageConflictError):
        storage.upsert("runs", "run-1", stale)

    assert {run["id"] for run in storage.list("runs")} == {"run-1", "run-2"}
    assert table.queries[-1]["ConsistentRead"] is True
    assert "IndexName" not in table.queries[-1]

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


def test_dynamodb_backend_legacy_version_write_uses_only_referenced_aliases() -> None:
    table = _FakeDynamoTable()
    storage = _dynamo_storage(table)
    table.items[("COLLECTION#runs", "run-legacy")] = {
        "pk": "COLLECTION#runs",
        "sk": "run-legacy",
        "id": "run-legacy",
    }

    storage.upsert("runs", "run-legacy", {"id": "run-legacy", "_version": 0})

    assert table.put_requests[-1]["ExpressionAttributeNames"] == {
        "#pk": "pk",
        "#version": "version",
    }


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
