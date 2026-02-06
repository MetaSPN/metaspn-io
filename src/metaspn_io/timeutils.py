from __future__ import annotations

from datetime import datetime, timezone


class TimestampError(ValueError):
    pass


def parse_timestamp(value: str | datetime) -> tuple[datetime, str | None]:
    """Parse timestamp value and normalize to UTC.

    Returns: (utc_datetime, original_timezone)
    """
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        raw = value.strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise TimestampError(f"invalid timestamp: {value}") from exc
    else:
        raise TimestampError(f"unsupported timestamp type: {type(value)!r}")

    original_tz: str | None
    if dt.tzinfo is None:
        original_tz = None
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        original_tz = dt.tzname() or dt.tzinfo.tzname(dt)

    return dt.astimezone(timezone.utc), original_tz


def in_range(ts: datetime, since: datetime | None, until: datetime | None) -> bool:
    if since and ts < since:
        return False
    if until and ts > until:
        return False
    return True
