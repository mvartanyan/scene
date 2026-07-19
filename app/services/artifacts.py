from __future__ import annotations

import hashlib
import mimetypes
import os
import shutil
from pathlib import Path
from typing import Dict, Iterable, Mapping, Optional

SCENE_ARTIFACT_ROOT_ENV = "SCENE_ARTIFACT_ROOT"
SCENE_ARTIFACT_BASE_URL_ENV = "SCENE_ARTIFACT_BASE_URL"
SCENE_ARTIFACT_STORAGE_ENV = "SCENE_ARTIFACT_STORAGE"
SCENE_ARTIFACT_TEMP_ROOT_ENV = "SCENE_ARTIFACT_TEMP_ROOT"
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
    def backend(self) -> str:
        return "filesystem"

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

    def object_url(self, key: str) -> str:
        return f"{self._base_url}/{key.lstrip('/')}"

    @staticmethod
    def checksum(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def deterministic_key(
        self,
        *,
        project_id: str,
        batch_id: str,
        kind: str,
        filename: str,
        run_id: Optional[str] = None,
        execution_id: Optional[str] = None,
        baseline_id: Optional[str] = None,
    ) -> str:
        if baseline_id:
            parts = [
                "projects",
                project_id,
                "batches",
                batch_id,
                "baselines",
                baseline_id,
            ]
            if execution_id:
                parts.extend(["executions", execution_id])
        elif run_id and execution_id:
            parts = [
                "projects",
                project_id,
                "batches",
                batch_id,
                "runs",
                run_id,
                "executions",
                execution_id,
            ]
        else:
            raise ValueError("Artifact keys require a baseline or run/execution scope.")
        parts.extend([kind, Path(filename).name])
        return "/".join(str(part).strip("/") for part in parts if str(part).strip("/"))

    def persist(
        self,
        source: Path,
        *,
        project_id: str,
        batch_id: str,
        kind: str,
        label: str,
        content_type: Optional[str] = None,
        run_id: Optional[str] = None,
        execution_id: Optional[str] = None,
        baseline_id: Optional[str] = None,
        filename: Optional[str] = None,
    ) -> Dict[str, object]:
        """Record a local artifact and return portable persistence metadata."""

        if not source.exists() or not source.is_file():
            raise FileNotFoundError(source)
        relative = self.relative(source)
        resolved_type = content_type or mimetypes.guess_type(source.name)[0] or "application/octet-stream"
        return {
            "kind": kind,
            "storage": self.backend,
            "key": relative,
            "path": relative,
            "url": self.url(source),
            "label": label,
            "content_type": resolved_type,
            "size_bytes": source.stat().st_size,
            "sha256": self.checksum(source),
        }

    def materialize(self, artifact: Mapping[str, object], destination: Optional[Path] = None) -> Path:
        relative = str(artifact.get("path") or artifact.get("key") or "")
        if not relative:
            raise FileNotFoundError("Artifact metadata has no path.")
        source = (self._root / relative).resolve()
        try:
            source.relative_to(self._root)
        except ValueError as exc:
            raise FileNotFoundError("Artifact path is outside the configured root.") from exc
        if not source.exists() or not source.is_file():
            raise FileNotFoundError(source)
        if destination is None or destination.resolve() == source:
            return source
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        return destination

    def read_bytes(self, artifact: Mapping[str, object], *, max_bytes: Optional[int] = None) -> bytes:
        path = self.materialize(artifact)
        if max_bytes is not None and path.stat().st_size > max_bytes:
            raise ValueError(f"Artifact exceeds the {max_bytes}-byte read limit.")
        return path.read_bytes()

    def read_text(
        self,
        artifact: Mapping[str, object],
        *,
        max_bytes: int = 10 * 1024 * 1024,
    ) -> str:
        return self.read_bytes(artifact, max_bytes=max_bytes).decode("utf-8", errors="replace")

    def download_url(self, artifact: Mapping[str, object]) -> Optional[str]:
        value = artifact.get("url")
        return str(value) if value else None

    def delete_artifacts(self, artifacts: Iterable[Mapping[str, object]]) -> None:
        for artifact in artifacts:
            try:
                target = self.materialize(artifact)
            except FileNotFoundError:
                continue
            target.unlink(missing_ok=True)

    def probe(self) -> Dict[str, object]:
        probe_dir = self._ensure_dir(self._root / ".probe")
        probe_path = probe_dir / "artifact-store"
        probe_path.write_bytes(b"scene")
        value = probe_path.read_bytes()
        probe_path.unlink(missing_ok=True)
        if value != b"scene":
            raise RuntimeError("Artifact store probe read did not match its write.")
        return {"backend": self.backend, "root": str(self._root)}

    def copy_to_baseline(self, source: Path, baseline_id: str, filename: Optional[str] = None) -> Path:
        destination_dir = self.baseline_dir(baseline_id)
        target_name = filename or source.name
        destination = destination_dir / target_name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        return destination

    def purge_run(
        self,
        run_id: str,
        artifacts: Optional[Iterable[Mapping[str, object]]] = None,
    ) -> None:
        """Remove all artifacts associated with a run."""
        target = self._root / "runs" / run_id
        if target.exists():
            shutil.rmtree(target, ignore_errors=True)

    def purge_baseline(
        self,
        baseline_id: str,
        artifacts: Optional[Iterable[Mapping[str, object]]] = None,
    ) -> None:
        """Remove all artifacts associated with a baseline."""
        target = self._root / "baselines" / baseline_id
        if target.exists():
            shutil.rmtree(target, ignore_errors=True)


_artifact_store: Optional[ArtifactStore] = None


def get_artifact_store() -> ArtifactStore:
    global _artifact_store
    if _artifact_store is None:
        backend = os.environ.get(SCENE_ARTIFACT_STORAGE_ENV, "filesystem").strip().lower()
        base_url = os.environ.get(SCENE_ARTIFACT_BASE_URL_ENV, "/artifacts")
        if backend in {"s3", "object"}:
            from app.services.s3_artifacts import S3ArtifactStore

            _artifact_store = S3ArtifactStore.from_environment(base_url=base_url)
        else:
            _artifact_store = ArtifactStore(base_url=base_url)
    return _artifact_store
