from datetime import datetime, timezone
from typing import Any

from fastapi.templating import Jinja2Templates

from app.services.storage import get_repository


def _format_timestamp(value: Any) -> str:
    if value in (None, ""):
        return "—"
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value))
        except ValueError:
            return str(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    repo = get_repository()
    config = repo.get_config()
    preference = config.get("display_timezone", "utc")
    if preference == "local":
        dt = dt.astimezone()
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%d %b %y %H:%M:%S")

def _format_time(value: Any) -> str:
    if value in (None, ""):
        return "—"
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value))
        except ValueError:
            return str(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    repo = get_repository()
    config = repo.get_config()
    preference = config.get("display_timezone", "utc")
    if preference == "local":
        dt = dt.astimezone()
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%H:%M:%S")


templates = Jinja2Templates(directory="app/templates")
templates.env.globals.update(
    {
        "hasattr": hasattr,
        "getattr": getattr,
        "len": len,
    }
)
templates.env.filters["format_ts"] = _format_timestamp
templates.env.filters["format_time"] = _format_time
