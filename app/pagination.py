from __future__ import annotations

from math import ceil
from typing import Any, Dict, List, Sequence, Tuple, TypeVar


T = TypeVar("T")

DEFAULT_PAGE_SIZE = 25
MAX_PAGE_SIZE = 100


def paginate(
    items: Sequence[T],
    *,
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> Tuple[List[T], Dict[str, Any]]:
    """Return a bounded page plus template-friendly pagination metadata."""

    resolved_size = max(1, min(int(page_size or DEFAULT_PAGE_SIZE), MAX_PAGE_SIZE))
    total = len(items)
    total_pages = max(1, ceil(total / resolved_size))
    resolved_page = max(1, min(int(page or 1), total_pages))
    offset = (resolved_page - 1) * resolved_size
    end_offset = min(offset + resolved_size, total)
    return list(items[offset:end_offset]), {
        "page": resolved_page,
        "page_size": resolved_size,
        "total": total,
        "total_pages": total_pages,
        "start": offset + 1 if total else 0,
        "end": end_offset,
        "has_previous": resolved_page > 1,
        "has_next": resolved_page < total_pages,
        "previous_page": resolved_page - 1 if resolved_page > 1 else None,
        "next_page": resolved_page + 1 if resolved_page < total_pages else None,
    }
