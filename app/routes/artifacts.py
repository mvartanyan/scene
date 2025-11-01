from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from app.services.artifacts import get_artifact_store

router = APIRouter(tags=["artifacts"])


@router.get("/artifacts/{artifact_path:path}")
async def read_artifact(artifact_path: str) -> FileResponse:
    store = get_artifact_store()
    root = store.root.resolve()
    target = (root / artifact_path).resolve()

    if not str(target).startswith(str(root)):
        raise HTTPException(status_code=404, detail="Artifact not found")
    if not target.exists() or not target.is_file():
        raise HTTPException(status_code=404, detail="Artifact not found")

    return FileResponse(path=target)
