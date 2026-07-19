from __future__ import annotations

from typing import Any, Dict, Iterable, Iterator, List, Optional, Protocol, Tuple


class StorageConflictError(RuntimeError):
    """Raised when a conditional state write races with another writer."""


class InvalidStorageCursorError(ValueError):
    """Raised when a caller supplies an invalid backend pagination cursor."""


class StorageBackend(Protocol):
    def get_config(self) -> Dict[str, Any]: ...

    def update_config(self, **updates: Any) -> Dict[str, Any]: ...

    def transaction(self) -> Iterator[None]: ...

    def upsert(
        self,
        collection: str,
        item_id: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]: ...

    def get(self, collection: str, item_id: str) -> Optional[Dict[str, Any]]: ...

    def delete(self, collection: str, item_id: str) -> None: ...

    def list(self, collection: str) -> List[Dict[str, Any]]: ...

    def filter(
        self,
        collection: str,
        *,
        key: str,
        value: Any,
    ) -> List[Dict[str, Any]]: ...

    def bulk_delete(self, collection: str, item_ids: Iterable[str]) -> None: ...

    def query_page(
        self,
        collection: str,
        *,
        key: Optional[str] = None,
        value: Any = None,
        limit: int = 25,
        cursor: Optional[str] = None,
        descending: bool = False,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]: ...

    def count(
        self,
        collection: str,
        *,
        key: Optional[str] = None,
        value: Any = None,
    ) -> int: ...

    def backend_info(self) -> Dict[str, Any]: ...

    def probe(self) -> Dict[str, Any]: ...
