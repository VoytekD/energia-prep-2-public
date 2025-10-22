from __future__ import annotations
from datetime import datetime
from zoneinfo import ZoneInfo

def to_local(ts_utc: datetime, tz: str = "Europe/Warsaw") -> datetime:
    return ts_utc.astimezone(ZoneInfo(tz)).replace(tzinfo=None)
