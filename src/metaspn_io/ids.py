from __future__ import annotations

import hashlib
from datetime import datetime, timezone


def stable_signal_id(source: str, timestamp: datetime, key: str) -> str:
    """Generate stable deterministic signal id from canonical identity fields."""
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    canonical = f"{source.strip().lower()}|{timestamp.astimezone(timezone.utc).isoformat()}|{key.strip()}"
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:24]
    return f"s_{digest}"
