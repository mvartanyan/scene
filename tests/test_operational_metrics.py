from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

import pytest

from app.services.operational_metrics import (
    backend_metrics_snapshot,
    collect_repository_metrics,
    observe_backend_operation,
    reset_backend_metrics_for_tests,
)


class _Repository:
    def __init__(self) -> None:
        now = datetime.now(timezone.utc)
        self.records: Dict[str, list[Dict[str, object]]] = {
            "runs": [
                {
                    "id": "run-finished",
                    "status": "finished",
                    "created_at": (now - timedelta(seconds=20)).isoformat(),
                    "completed_at": now.isoformat(),
                },
                {
                    "id": "run-queued",
                    "status": "queued",
                    "created_at": (now - timedelta(seconds=10)).isoformat(),
                },
            ],
            "executions": [
                {
                    "id": "execution-finished",
                    "status": "finished",
                    "created_at": (now - timedelta(seconds=8)).isoformat(),
                    "started_at": (now - timedelta(seconds=5)).isoformat(),
                    "completed_at": now.isoformat(),
                    "artifacts": {
                        "observed": {
                            "storage": "s3",
                            "key": "runs/one/observed.png",
                            "version_id": "v1",
                            "size_bytes": 100,
                        },
                        "duplicate": {
                            "storage": "s3",
                            "key": "runs/one/observed.png",
                            "version_id": "v1",
                            "size_bytes": 100,
                        },
                    },
                },
                {
                    "id": "execution-queued",
                    "status": "queued",
                    "created_at": (now - timedelta(seconds=30)).isoformat(),
                },
            ],
            "baselines": [],
        }
        self.dispatcher = {
            "metrics": {
                "counters": {"cycles_total": 7},
                "job_terminal": {"succeeded": 2},
            }
        }

    def count(self, collection: str) -> int:
        return len(self.records[collection])

    def query_page(
        self,
        collection: str,
        *,
        limit: int,
        cursor: Optional[str] = None,
        descending: bool = False,
    ):
        records = sorted(
            self.records[collection],
            key=lambda record: str(record.get("created_at") or ""),
            reverse=descending,
        )
        offset = int(cursor or 0)
        page = records[offset : offset + limit]
        next_offset = offset + len(page)
        return page, str(next_offset) if next_offset < len(records) else None

    def operational_metrics(self):
        return {"counters": {"callback_accepted_total": 3}}

    def dispatcher_status(self):
        return self.dispatcher


@pytest.mark.unit
def test_repository_metrics_are_bounded_and_do_not_duplicate_artifacts() -> None:
    snapshot = collect_repository_metrics(_Repository(), limit=1)

    assert snapshot["runs"]["total"] == 2
    assert snapshot["runs"]["truncated"] is True
    assert sum(snapshot["runs"]["statuses"].values()) == 1
    assert snapshot["executions"]["total"] == 2
    assert snapshot["executions"]["truncated"] is True
    assert snapshot["artifacts"] == {
        "count": 1,
        "size_bytes": 100,
        "truncated": True,
    }
    assert snapshot["counters"]["callback_accepted_total"] == 3
    assert snapshot["dispatcher"]["counters"]["cycles_total"] == 7
    assert snapshot["record_limit"] == 1


@pytest.mark.unit
def test_backend_registry_counts_success_error_latency_and_bounds_labels() -> None:
    reset_backend_metrics_for_tests()
    with observe_backend_operation("dynamodb", "read"):
        pass
    with pytest.raises(RuntimeError):
        with observe_backend_operation("customer-id-must-not-be-a-label", "secret-url"):
            raise RuntimeError("failure")

    snapshot = backend_metrics_snapshot()

    assert snapshot["counters"][("dynamodb", "read", "success")] == 1
    assert snapshot["counters"][("other", "other", "error")] == 1
    assert snapshot["histograms"][("dynamodb", "read")]["count"] == 1
    assert snapshot["histograms"][("other", "other")]["count"] == 1
    assert "customer-id-must-not-be-a-label" not in str(snapshot)
    assert "secret-url" not in str(snapshot)
