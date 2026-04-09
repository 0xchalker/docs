"""Mathematical utility functions for the trading bot."""
from typing import Sequence, Optional
import numpy as np


def compute_percentile(series: Sequence[float], value: float) -> float:
    """
    Compute the percentile rank of `value` within `series`.
    Returns a float in [0, 1].
    """
    arr = np.asarray(series, dtype=float)
    if arr.size == 0:
        return 0.5
    return float(np.mean(arr <= value))


def compute_zscore(series: Sequence[float], value: float) -> float:
    """
    Compute the z-score of `value` relative to `series`.
    Returns 0.0 if standard deviation is zero.
    """
    arr = np.asarray(series, dtype=float)
    if arr.size < 2:
        return 0.0
    mu = float(np.mean(arr))
    sigma = float(np.std(arr, ddof=1))
    if sigma == 0.0:
        return 0.0
    return (value - mu) / sigma


def compute_atr(ohlc_series: Sequence[tuple], period: int = 14) -> float:
    """
    Compute Average True Range over the last `period` candles.
    Each element of ohlc_series should be (open, high, low, close).
    Returns ATR in price units.
    """
    arr = np.asarray(ohlc_series, dtype=float)
    if arr.shape[0] < 2 or arr.shape[1] < 4:
        return 0.0
    highs = arr[:, 1]
    lows = arr[:, 2]
    closes = arr[:, 3]
    prev_closes = closes[:-1]
    tr_hl = highs[1:] - lows[1:]
    tr_hpc = np.abs(highs[1:] - prev_closes)
    tr_lpc = np.abs(lows[1:] - prev_closes)
    tr = np.maximum(tr_hl, np.maximum(tr_hpc, tr_lpc))
    if tr.size == 0:
        return 0.0
    window = min(period, tr.size)
    return float(np.mean(tr[-window:]))


def bps_to_pct(bps: float) -> float:
    """Convert basis points to percentage (e.g. 10 bps -> 0.10%)."""
    return bps / 100.0


def pct_to_bps(pct: float) -> float:
    """Convert percentage to basis points (e.g. 0.10% -> 10 bps)."""
    return pct * 100.0


def safe_div(a: float, b: float, default: float = 0.0) -> float:
    """Safe division returning `default` when denominator is zero."""
    if b == 0.0:
        return default
    return a / b
