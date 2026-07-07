from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Optional

SCENE_ARTIFACT_ROOT_ENV = "SCENE_ARTIFACT_ROOT"
SCENE_ARTIFACT_BASE_URL_ENV = "SCENE_ARTIFACT_BASE_URL"
DEFAULT_LOCAL_ROOT = Path(".scene")
DEFAULT_ARTIFACT_ROOT = DEFAULT_LOCAL_ROOT / "artifacts"


def resolve_artifact_root() -> Path:
    configured = os.environ.get(SCENE_ARTIFACT_ROOT_ENV)
    if configured:
        return Path(configured).expanduser()
    return DEFAULT_ARTIFACT_ROOT


class ArtifactStore:
    """Manage on-disk locations for run and baseline artifacts."""

    def __init__(self, root: Optional[Path] = None, base_url: str = "/artifacts") -> None:
        resolved_root = root or resolve_artifact_root()
        self._root = resolved_root.resolve()
        self._base_url = base_url.rstrip("/")
        self._root.mkdir(parents=True, exist_ok=True)

    @property
    def root(self) -> Path:
        return self._root

    def _ensure_dir(self, path: Path) -> Path:
        path.mkdir(parents=True, exist_ok=True)
        return path

    def run_dir(self, run_id: str) -> Path:
        return self._ensure_dir(self._root / "runs" / run_id)

    def execution_dir(self, run_id: str, execution_id: str) -> Path:
        return self._ensure_dir(self.run_dir(run_id) / execution_id)

    def baseline_dir(self, baseline_id: str) -> Path:
        return self._ensure_dir(self._root / "baselines" / baseline_id)

    def relative(self, path: Path) -> str:
        cleaned = path.resolve()
        root = self._root.resolve()
        return str(cleaned.relative_to(root))

    def url(self, path: Path) -> str:
        return f"{self._base_url}/{self.relative(path)}"

    def copy_to_baseline(self, source: Path, baseline_id: str, filename: Optional[str] = None) -> Path:
        destination_dir = self.baseline_dir(baseline_id)
        target_name = filename or source.name
        destination = destination_dir / target_name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        return destination

    def purge_run(self, run_id: str) -> None:
        """Remove all artifacts associated with a run."""
        target = self._root / "runs" / run_id
        if target.exists():
            shutil.rmtree(target, ignore_errors=True)

    def purge_baseline(self, baseline_id: str) -> None:
        """Remove all artifacts associated with a baseline."""
        target = self._root / "baselines" / baseline_id
        if target.exists():
            shutil.rmtree(target, ignore_errors=True)


_artifact_store: Optional[ArtifactStore] = None


def get_artifact_store() -> ArtifactStore:
    global _artifact_store
    if _artifact_store is None:
        _artifact_store = ArtifactStore(
            base_url=os.environ.get(SCENE_ARTIFACT_BASE_URL_ENV, "/artifacts")
        )
    return _artifact_store
