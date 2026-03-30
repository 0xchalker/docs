"""NTP-synced time utilities."""
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

try:
    import ntplib
    _NTP_AVAILABLE = True
except ImportError:
    _NTP_AVAILABLE = False

_ntp_offset_seconds: float = 0.0
_last_ntp_sync: Optional[float] = None
_NTP_SYNC_INTERVAL = 300.0  # re-sync every 5 minutes


def _sync_ntp() -> None:
    """Attempt to sync with NTP server and update offset."""
    global _ntp_offset_seconds, _last_ntp_sync
    if not _NTP_AVAILABLE:
        return
    try:
        client = ntplib.NTPClient()
        response = client.request("pool.ntp.org", version=3, timeout=2.0)
        _ntp_offset_seconds = response.offset
        _last_ntp_sync = time.monotonic()
    except Exception:
        # Fall back to system time silently
        pass


def get_utc_now() -> datetime:
    """Return NTP-corrected UTC datetime. Falls back to system time on failure."""
    global _last_ntp_sync
    now_mono = time.monotonic()
    if _NTP_AVAILABLE and (
        _last_ntp_sync is None
        or (now_mono - _last_ntp_sync) > _NTP_SYNC_INTERVAL
    ):
        _sync_ntp()

    system_utc = datetime.now(tz=timezone.utc)
    if _ntp_offset_seconds != 0.0:
        system_utc = system_utc + timedelta(seconds=_ntp_offset_seconds)
    return system_utc


def is_within_window(
    now: datetime,
    target: datetime,
    before_sec: float,
    after_sec: float,
) -> bool:
    """Return True if now is within [target - before_sec, target + after_sec]."""
    delta = (now - target).total_seconds()
    return -before_sec <= delta <= after_sec


def seconds_until(now: datetime, target: datetime) -> float:
    """Return seconds until target from now (negative if already past)."""
    return (target - now).total_seconds()
