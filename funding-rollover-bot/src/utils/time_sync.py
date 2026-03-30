"""UTC time synchronisation utilities."""
from datetime import datetime, timezone
from typing import Optional

try:
    import ntplib
    _HAS_NTPLIB = True
except ImportError:
    _HAS_NTPLIB = False

_ntp_offset_seconds: float = 0.0  # cached offset from NTP


def sync_ntp(server: str = "pool.ntp.org", timeout: float = 2.0) -> None:
    """Attempt to synchronise against an NTP server and cache the offset."""
    global _ntp_offset_seconds
    if not _HAS_NTPLIB:
        return
    try:
        client = ntplib.NTPClient()
        response = client.request(server, version=3, timeout=timeout)
        _ntp_offset_seconds = response.offset
    except Exception:
        _ntp_offset_seconds = 0.0


def get_utc_now() -> datetime:
    """Return the current UTC time, adjusted by the cached NTP offset."""
    import time
    ts = time.time() + _ntp_offset_seconds
    return datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None)


def is_within_window(
    now: datetime,
    target: datetime,
    before_sec: float,
    after_sec: float,
) -> bool:
    """Return True when *now* is between (*target* - *before_sec*) and (*target* + *after_sec*)."""
    diff = (target - now).total_seconds()
    return -after_sec <= diff <= before_sec


def seconds_until(now: datetime, target: datetime) -> float:
    """Return the number of seconds from *now* until *target* (can be negative)."""
    return (target - now).total_seconds()
