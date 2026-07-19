from __future__ import annotations

import os
from typing import Any, Dict, Optional

import httpx

SCENE_BASE_URL_ENV = "SCENE_BASE_URL"
SCENE_API_TOKEN_ENV = "SCENE_API_TOKEN"
SCENE_MCP_TIMEOUT_ENV = "SCENE_MCP_TIMEOUT_SECONDS"


class SceneClientError(RuntimeError):
    pass


class SceneAgentClient:
    def __init__(
        self,
        *,
        base_url: Optional[str] = None,
        api_token: Optional[str] = None,
        timeout_seconds: Optional[float] = None,
    ) -> None:
        self.base_url = (base_url or os.environ.get(SCENE_BASE_URL_ENV) or "http://127.0.0.1:8000").rstrip("/")
        self.api_token = api_token if api_token is not None else os.environ.get(SCENE_API_TOKEN_ENV)
        if timeout_seconds is None:
            timeout_seconds = float(os.environ.get(SCENE_MCP_TIMEOUT_ENV, "30"))
        self.timeout_seconds = timeout_seconds

    def _headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"
        return headers

    def request(
        self,
        method: str,
        path: str,
        *,
        json: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        url = f"{self.base_url}{path}"
        with httpx.Client(timeout=self.timeout_seconds) as client:
            response = client.request(
                method,
                url,
                headers=self._headers(),
                json=json,
                params={k: v for k, v in (params or {}).items() if v is not None},
            )
        if response.status_code >= 400:
            detail: Any
            try:
                detail = response.json()
            except ValueError:
                detail = response.text
            raise SceneClientError(f"{method} {path} failed with {response.status_code}: {detail}")
        if response.status_code == 204:
            return {"status": "deleted"}
        if not response.content:
            return {}
        return response.json()

    def get_manifest(self) -> Dict[str, Any]:
        return self.request("GET", "/api/agent/manifest")

    def apply_setup(self, setup: Dict[str, Any]) -> Dict[str, Any]:
        return self.request("POST", "/api/agent/setup", json=setup)

    def list_projects(self) -> Any:
        return self.request("GET", "/api/projects")

    def list_batches(self, project_id: Optional[str] = None) -> Any:
        return self.request("GET", "/api/batches", params={"project_id": project_id})

    def run_batch(
        self,
        batch_id: str,
        *,
        baseline_id: Optional[str] = None,
        requested_by: Optional[str] = "mcp",
        note: Optional[str] = None,
        spm_ticket: Optional[str] = None,
        jira_issue: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
    ) -> Dict[str, Any]:
        ticket = spm_ticket if spm_ticket is not None else jira_issue
        return self.request(
            "POST",
            f"/api/batches/{batch_id}/comparison-runs",
            json={
                "baseline_id": baseline_id,
                "requested_by": requested_by,
                "note": note,
                "spm_ticket": ticket,
                "timeout_seconds": timeout_seconds,
            },
        )

    def record_baseline(
        self,
        project_id: str,
        batch_id: str,
        *,
        requested_by: Optional[str] = "mcp",
        note: Optional[str] = None,
        spm_ticket: Optional[str] = None,
        jira_issue: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
    ) -> Dict[str, Any]:
        ticket = spm_ticket if spm_ticket is not None else jira_issue
        return self.request(
            "POST",
            "/api/runs",
            json={
                "project_id": project_id,
                "batch_id": batch_id,
                "purpose": "baseline_recording",
                "requested_by": requested_by,
                "note": note,
                "spm_ticket": ticket,
                "timeout_seconds": timeout_seconds,
            },
        )

    def get_run_status(self, run_id: str) -> Dict[str, Any]:
        return self.request("GET", f"/api/runs/{run_id}/detail")

    def get_run_result(self, run_id: str) -> Dict[str, Any]:
        return self.request("GET", f"/api/runs/{run_id}/result")

    def get_artifacts(self, run_id: str) -> Dict[str, Any]:
        return self.request("GET", f"/api/runs/{run_id}/artifacts")

    def cancel_run(self, run_id: str) -> Dict[str, Any]:
        return self.request("POST", f"/api/runs/{run_id}/cancel")

    def retry_execution(self, execution_id: str) -> Dict[str, Any]:
        return self.request("POST", f"/api/executions/{execution_id}/retry")
