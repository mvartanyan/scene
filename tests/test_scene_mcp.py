from __future__ import annotations

from typing import Any, Callable, Dict

import pytest

from scene_mcp.client import SceneAgentClient, SceneClientError
from scene_mcp.server import TOOL_NAMES, register_tools


class _FakeMCP:
    def __init__(self) -> None:
        self.tools: Dict[str, Callable[..., Any]] = {}

    def tool(self):
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self.tools[func.__name__] = func
            return func

        return decorator


class _FakeClient:
    def get_manifest(self) -> dict:
        return {"name": "SCENE Agent Control Plane"}

    def apply_setup(self, setup: dict) -> dict:
        return {"setup": setup}

    def list_projects(self) -> list:
        return [{"id": "p1"}]

    def list_batches(self, project_id=None) -> dict:
        return {"project_id": project_id}

    def run_batch(self, batch_id: str, **kwargs) -> dict:
        return {"batch_id": batch_id, **kwargs}

    def record_baseline(self, project_id: str, batch_id: str, **kwargs) -> dict:
        return {"project_id": project_id, "batch_id": batch_id, **kwargs}

    def get_run_status(self, run_id: str) -> dict:
        return {"status": "executing", "run_id": run_id}

    def get_run_result(self, run_id: str) -> dict:
        return {"status": "finished", "run_id": run_id}

    def get_artifacts(self, run_id: str) -> dict:
        return {"run_id": run_id, "executions": []}

    def cancel_run(self, run_id: str) -> dict:
        return {"run_id": run_id, "status": "cancelled"}

    def retry_execution(self, execution_id: str) -> dict:
        return {"execution_id": execution_id, "status": "queued"}


def test_mcp_tools_register_and_forward_to_rest_client() -> None:
    fake_mcp = _FakeMCP()
    fake_client = _FakeClient()

    register_tools(fake_mcp, client_factory=lambda: fake_client)  # type: ignore[arg-type]

    assert sorted(fake_mcp.tools) == sorted(TOOL_NAMES)
    assert fake_mcp.tools["scene_get_manifest"]()["name"] == "SCENE Agent Control Plane"
    assert fake_mcp.tools["scene_apply_setup"]({"project": {"slug": "demo"}})["setup"] == {
        "project": {"slug": "demo"}
    }
    assert fake_mcp.tools["scene_list_batches"]("p1") == {"project_id": "p1"}
    baseline = fake_mcp.tools["scene_record_baseline"](
        "p1",
        "b1",
        note="baseline",
        spm_ticket="SCENE-12",
    )
    assert baseline["note"] == "baseline"
    assert baseline["spm_ticket"] == "SCENE-12"
    comparison = fake_mcp.tools["scene_run_batch"](
        "b1",
        note="from mcp",
        spm_ticket="SCENE-12",
    )
    assert comparison["note"] == "from mcp"
    assert comparison["spm_ticket"] == "SCENE-12"
    assert fake_mcp.tools["scene_get_run_status"]("r1")["status"] == "executing"
    assert fake_mcp.tools["scene_get_run_result"]("r1")["status"] == "finished"
    assert fake_mcp.tools["scene_get_artifacts"]("r1")["executions"] == []
    assert fake_mcp.tools["scene_cancel_run"]("r1")["status"] == "cancelled"
    assert fake_mcp.tools["scene_retry_execution"]("e1")["status"] == "queued"


def test_scene_client_adds_bearer_token_and_surfaces_errors(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []

    class _Response:
        def __init__(self, status_code: int, payload: dict) -> None:
            self.status_code = status_code
            self._payload = payload
            self.content = b"{}"
            self.text = str(payload)

        def json(self) -> dict:
            return self._payload

    class _HTTPClient:
        def __init__(self, timeout: float) -> None:
            self.timeout = timeout

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def request(self, method, url, headers, json=None, params=None):
            calls.append(
                {
                    "method": method,
                    "url": url,
                    "headers": headers,
                    "json": json,
                    "params": params,
                }
            )
            if url.endswith("/api/projects"):
                return _Response(500, {"detail": "boom"})
            return _Response(200, {"ok": True})

    monkeypatch.setattr("scene_mcp.client.httpx.Client", _HTTPClient)
    client = SceneAgentClient(
        base_url="https://scene.example.test",
        api_token="secret",
        timeout_seconds=1,
    )

    assert client.get_manifest() == {"ok": True}
    assert calls[0]["headers"]["Authorization"] == "Bearer secret"
    assert client.run_batch("batch-1", spm_ticket="SCENE-12") == {"ok": True}
    assert calls[1]["json"]["spm_ticket"] == "SCENE-12"
    assert "jira_issue" not in calls[1]["json"]

    with pytest.raises(SceneClientError, match="GET /api/projects failed with 500"):
        client.list_projects()
