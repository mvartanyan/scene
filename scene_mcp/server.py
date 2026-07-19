from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from scene_mcp.client import SceneAgentClient

try:  # MCP Python SDK 1.x common path.
    from mcp.server.fastmcp import FastMCP as _MCPServer
except ModuleNotFoundError:  # pragma: no cover - depends on installed SDK version
    try:
        from mcp.server import MCPServer as _MCPServer  # type: ignore
    except ModuleNotFoundError:  # pragma: no cover - helpful runtime error
        _MCPServer = None  # type: ignore

TOOL_NAMES = [
    "scene_get_manifest",
    "scene_apply_setup",
    "scene_list_projects",
    "scene_list_batches",
    "scene_record_baseline",
    "scene_run_batch",
    "scene_get_run_status",
    "scene_get_run_result",
    "scene_get_artifacts",
    "scene_cancel_run",
    "scene_retry_execution",
]


def default_client() -> SceneAgentClient:
    return SceneAgentClient()


def register_tools(mcp: Any, client_factory: Callable[[], SceneAgentClient] = default_client) -> Any:
    @mcp.tool()
    def scene_get_manifest() -> Dict[str, Any]:
        """Read SCENE agent capabilities, auth, REST docs, and MCP metadata."""
        return client_factory().get_manifest()

    @mcp.tool()
    def scene_apply_setup(setup: Dict[str, Any]) -> Dict[str, Any]:
        """Idempotently create or update a SCENE project graph."""
        return client_factory().apply_setup(setup)

    @mcp.tool()
    def scene_list_projects() -> Any:
        """List SCENE projects."""
        return client_factory().list_projects()

    @mcp.tool()
    def scene_list_batches(project_id: Optional[str] = None) -> Any:
        """List SCENE batches, optionally filtered by project."""
        return client_factory().list_batches(project_id=project_id)

    @mcp.tool()
    def scene_record_baseline(
        project_id: str,
        batch_id: str,
        requested_by: Optional[str] = "mcp",
        note: Optional[str] = None,
        spm_ticket: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        task_ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Launch a baseline-recording run for a SCENE batch."""
        return client_factory().record_baseline(
            project_id,
            batch_id,
            requested_by=requested_by,
            note=note,
            spm_ticket=spm_ticket,
            timeout_seconds=timeout_seconds,
            task_ids=task_ids,
        )

    @mcp.tool()
    def scene_run_batch(
        batch_id: str,
        baseline_id: Optional[str] = None,
        requested_by: Optional[str] = "mcp",
        note: Optional[str] = None,
        spm_ticket: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        task_ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Launch an unattended comparison run for a SCENE batch."""
        return client_factory().run_batch(
            batch_id,
            baseline_id=baseline_id,
            requested_by=requested_by,
            note=note,
            spm_ticket=spm_ticket,
            timeout_seconds=timeout_seconds,
            task_ids=task_ids,
        )

    @mcp.tool()
    def scene_get_run_status(run_id: str) -> Dict[str, Any]:
        """Read structured run status, counts, baseline, and execution state."""
        return client_factory().get_run_status(run_id)

    @mcp.tool()
    def scene_get_run_result(run_id: str) -> Dict[str, Any]:
        """Read SPM-friendly run metrics, thresholds, failures, and top artifact links."""
        return client_factory().get_run_result(run_id)

    @mcp.tool()
    def scene_get_artifacts(run_id: str) -> Dict[str, Any]:
        """Read artifact metadata and viewer/log links for a run."""
        return client_factory().get_artifacts(run_id)

    @mcp.tool()
    def scene_cancel_run(run_id: str) -> Dict[str, Any]:
        """Cancel a queued or executing SCENE run."""
        return client_factory().cancel_run(run_id)

    @mcp.tool()
    def scene_retry_execution(execution_id: str) -> Dict[str, Any]:
        """Retry one failed or cancelled execution while its run is still modifiable."""
        return client_factory().retry_execution(execution_id)

    return mcp


def create_server(client_factory: Callable[[], SceneAgentClient] = default_client) -> Any:
    if _MCPServer is None:
        raise RuntimeError(
            "The MCP Python SDK is not installed. Install SCENE with the mcp dependency "
            "or run `pip install mcp` in this environment."
        )
    return register_tools(_MCPServer("SCENE"), client_factory=client_factory)


def main() -> None:
    create_server().run()


if __name__ == "__main__":
    main()
