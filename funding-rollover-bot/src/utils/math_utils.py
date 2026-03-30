"""Mathematical utility functions."""
from typing import Sequence, Union
import statistics


def compute_percentile(series: Sequence[float], value: float) -> float:
    """Return the percentile rank of *value* within *series* in [0, 1].

    E.g. if value is lower than 10 % of the series this returns 0.10.
    """
    if not series:
        return 0.5
    n = len(series)
    rank = sum(1 for x in series if x <= value)
    return rank / n


def compute_zscore(series: Sequence[float], value: float) -> float:
    """Return z-score of *value* relative to *series*."""
    if len(series) < 2:
        return 0.0
    try:
        mean = statistics.mean(series)
        std = statistics.stdev(series)
    except statistics.StatisticsError:
        return 0.0
    if std == 0.0:
        return 0.0
    return (value - mean) / std


def compute_atr(ohlc_series: Sequence[dict], period: int = 14) -> float:
    """Compute Average True Range over *period* candles.

    Each element of *ohlc_series* must be a dict with keys 'high', 'low', 'close'.
    Returns ATR in price units.
    """
    if len(ohlc_series) < 2:
        return 0.0
    trs = []
    candles = list(ohlc_series)
    for i in range(1, len(candles)):
        h = candles[i]["high"]
        l = candles[i]["low"]
        prev_c = candles[i - 1]["close"]
        tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
        trs.append(tr)
    if not trs:
        return 0.0
    trs = trs[-period:]
    return sum(trs) / len(trs)


def bps_to_pct(bps: float) -> float:
    """Convert basis points to a decimal fraction (e.g. 10 bps -> 0.001)."""
    return bps / 10_000.0


def pct_to_bps(pct: float) -> float:
    """Convert decimal fraction to basis points (e.g. 0.001 -> 10 bps)."""
    return pct * 10_000.0


def safe_div(a: float, b: float, default: float = 0.0) -> float:
    """Divide *a* by *b*, returning *default* if *b* is zero."""
    if b == 0.0:
        return default
    return a / b
