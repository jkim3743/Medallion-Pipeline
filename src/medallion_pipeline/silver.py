from __future__ import annotations

from datetime import datetime
from typing import Optional, Dict, Any


# ServiceNow CSV sample format:
# 6/5/2025 13:10  (M/D/YYYY H:MM)
# 2/15/2025 3:21  (M/D/YYYY H:MM)
_TS_FORMATS = (
    "%m/%d/%Y %H:%M",  # 06/05/2025 13:10 (also parses 6/5/2025 13:10)
    "%m/%d/%Y %H:%M:%S",  # in case seconds appear later
)

_NULL_TOKENS = {"", "null", "none", "na", "n/a", "nan"}


def clean_str(value: Optional[str]) -> Optional[str]:
    """Trim strings and normalize null-like values to None."""
    if value is None:
        return None
    v = value.strip()
    if v.lower() in _NULL_TOKENS:
        return None
    return v


def parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    """
    Parse ServiceNow timestamp string into datetime.
    Returns None if value is None/empty/unparseable.
    """
    v = clean_str(value)
    if v is None:
        return None

    for fmt in _TS_FORMATS:
        try:
            return datetime.strptime(v, fmt)
        except ValueError:
            continue

    return None


def add_silver_timestamps(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Given a raw/bronze record dict, add:
    - opened_at_ts
    - closed_at_ts
    """
    out = dict(row)
    out["opened_at_ts"] = parse_timestamp(row.get("opened_at"))
    out["closed_at_ts"] = parse_timestamp(row.get("closed_at"))
    return out


def validate_state_priority(row: Dict[str, Any]) -> bool:
    """Simple DQ helper: state and priority should exist for aggregation."""
    state = clean_str(row.get("state"))
    priority = row.get("priority")
    return state is not None and priority is not None
