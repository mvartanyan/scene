from __future__ import annotations

from typing import Dict, Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, RedirectResponse, Response

from app.services.artifacts import get_artifact_store
from app.services.storage import RepositoryDep, SceneRepository

router = APIRouter(tags=["artifacts"])


def _artifact_for_key(repo: SceneRepository, key: str) -> Optional[Dict[str, object]]:
    parts = key.split("/")
    if "runs" in parts and "executions" in parts:
        try:
            run_id = parts[parts.index("runs") + 1]
            execution_id = parts[parts.index("executions") + 1]
        except IndexError:
            return None
        execution = repo.get_execution(execution_id)
        if not execution or str(execution.get("run_id")) != run_id:
            return None
        for artifact in (execution.get("artifacts") or {}).values():
            if isinstance(artifact, dict) and str(artifact.get("key") or artifact.get("path")) == key:
                return artifact
        return None
    if "baselines" in parts:
        try:
            baseline_id = parts[parts.index("baselines") + 1]
        except IndexError:
            return None
        baseline = repo.get_baseline(baseline_id)
        if not baseline:
            return None
        for item in baseline.get("items") or []:
            for artifact in (item.get("artifacts") or {}).values():
                if isinstance(artifact, dict) and str(artifact.get("key") or artifact.get("path")) == key:
                    return artifact
    return None


@router.get("/artifacts/{artifact_path:path}")
async def read_artifact(
    artifact_path: str,
    repo: SceneRepository = RepositoryDep,
) -> Response:
    store = get_artifact_store()
    if store.backend == "s3":
        artifact = _artifact_for_key(repo, artifact_path)
        if not artifact:
            raise HTTPException(status_code=404, detail="Artifact not found")
        url = store.download_url(artifact)
        if not url:
            raise HTTPException(status_code=404, detail="Artifact not found")
        return RedirectResponse(url=url, status_code=307)

    root = store.root.resolve()
    target = (root / artifact_path).resolve()

    try:
        target.relative_to(root)
    except ValueError:
        raise HTTPException(status_code=404, detail="Artifact not found")
    if not target.exists() or not target.is_file():
        raise HTTPException(status_code=404, detail="Artifact not found")

    return FileResponse(path=target)
