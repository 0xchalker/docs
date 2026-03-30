"""
Signal engine — computes long/short scores and locks trade decision.
Implements the exact scoring logic from the strategy spec.
"""
from typing import Dict, Tuple

from core.models import DecisionType, FeatureSnapshot
from utils.logging_utils import get_logger

logger = get_logger(__name__)


def _get_spread_cfg(cfg: Dict, symbol_group: str) -> float:
    return float(cfg.get("filters", {}).get("max_spread_bps", {}).get(symbol_group, 6.0))


def _get_atr_cfg(cfg: Dict, symbol_group: str) -> float:
    return float(cfg.get("filters", {}).get("max_atr_1m_bps", {}).get(symbol_group, 35.0))


def _get_low_long_liq(cfg: Dict) -> float:
    return float(cfg.get("risk", {}).get("low_long_liq_threshold_usd", 100_000))


def compute_long_score(f: FeatureSnapshot, cfg: Dict) -> int:
    """
    Compute the LONG signal score for a FeatureSnapshot.

    Scoring breakdown (max = 9):
      +2  deep negative OI-weighted funding + z-score <= -1.8
      +1  funding percentile in bottom 10% over 7d
      +2  OI increasing >= 1.5% over 15m
      +2  price holding above 15m low (dist >= 0.1%)
      +1  more short liquidations than long liquidations in 5m
      +1  taker buy/sell ratio >= 1.0 (buyers dominating)
      +1  execution quality (spread + ATR within limits)
    """
    thresholds = cfg.get("thresholds", {})
    symbol_group = _classify_group(f.symbol)
    max_spread = _get_spread_cfg(cfg, symbol_group)
    max_atr = _get_atr_cfg(cfg, symbol_group)

    funding_zscore_thresh = float(thresholds.get("funding_zscore_long", -1.8))
    funding_pct_thresh = float(thresholds.get("funding_percentile_long", 0.10))
    oi_delta_15m_thresh = float(thresholds.get("oi_delta_15m_long", 0.015))

    score = 0

    # Criterion 1: deep negative OI-weighted funding + z-score sufficiently negative
    if f.funding_oi_weighted_now < 0 and f.funding_zscore_24h <= funding_zscore_thresh:
        score += 2

    # Criterion 2: funding percentile at extreme low (bottom 10% of 7d)
    if f.funding_percentile_7d <= funding_pct_thresh:
        score += 1

    # Criterion 3: OI growing over 15 minutes (crowded short building)
    if f.oi_delta_15m >= oi_delta_15m_thresh:
        score += 2

    # Criterion 4: price holding above 15m low (no capitulation)
    if f.dist_to_15m_low >= 0.001:
        score += 2

    # Criterion 5: short liquidations dominating (crowded short being squeezed)
    if f.liq_short_5m_usd >= 1.2 * max(f.liq_long_5m_usd, 1.0):
        score += 1

    # Criterion 6: net buy pressure
    if f.taker_buy_sell_ratio_5m >= 1.00:
        score += 1

    # Criterion 7: execution quality
    if f.spread_bps <= max_spread and f.atr_1m_bps <= max_atr:
        score += 1

    logger.debug(
        "long_score",
        symbol=f.symbol,
        score=score,
        funding_oi_w=f.funding_oi_weighted_now,
        zscore=f.funding_zscore_24h,
        pct7d=f.funding_percentile_7d,
        oi_d15m=f.oi_delta_15m,
        dist_low=f.dist_to_15m_low,
        liq_s=f.liq_short_5m_usd,
        liq_l=f.liq_long_5m_usd,
        taker5m=f.taker_buy_sell_ratio_5m,
        spread=f.spread_bps,
        atr=f.atr_1m_bps,
    )
    return score


def compute_short_score(f: FeatureSnapshot, cfg: Dict) -> int:
    """
    Compute the SHORT signal score for a FeatureSnapshot.

    Scoring breakdown (max = 8):
      +1  funding rate negative (longs paying shorts)
      +2  price pushed up >= 0.15% in 5m  |  +1 price near 5m high (>70% range)
      +2  OI growing >= 0.8% in 5m (new longs entering into pump)
      +1  low long liquidation pressure (longs not being forced out)
      +1  taker sell ratio > 0.98 in 1m (sellers emerging)
      +1  execution quality
    """
    thresholds = cfg.get("thresholds", {})
    symbol_group = _classify_group(f.symbol)
    max_spread = _get_spread_cfg(cfg, symbol_group)
    max_atr = _get_atr_cfg(cfg, symbol_group)
    low_long_liq = _get_low_long_liq(cfg)

    price_delta_5m_thresh = float(thresholds.get("price_delta_5m_short", 0.0015))
    oi_delta_5m_thresh = float(thresholds.get("oi_delta_5m_short", 0.008))
    taker_ratio_cap_1m = float(thresholds.get("taker_ratio_short_cap_1m", 0.98))

    score = 0

    # Criterion 1: funding is negative (shorts being paid = bullish cost pressure)
    if f.funding_now < 0:
        score += 1

    # Criterion 2: price driven up before rollover
    if f.price_delta_5m >= price_delta_5m_thresh:
        score += 2
    elif f.price_now > 0 and f.high_5m > f.low_5m:
        price_range = f.high_5m - f.low_5m
        if price_range > 0 and f.price_now >= (f.low_5m + 0.7 * price_range):
            score += 1

    # Criterion 3: OI growing in the last 5m (fresh longs piling in after funding)
    if f.oi_delta_5m >= oi_delta_5m_thresh:
        score += 2

    # Criterion 4: low long liquidation (longs not being squeezed = they may close post-rollover)
    if f.liq_long_5m_usd <= low_long_liq:
        score += 1

    # Criterion 5: selling pressure emerging in last 1m
    if f.taker_buy_sell_ratio_1m < taker_ratio_cap_1m:
        score += 1

    # Criterion 6: execution quality
    if f.spread_bps <= max_spread and f.atr_1m_bps <= max_atr:
        score += 1

    logger.debug(
        "short_score",
        symbol=f.symbol,
        score=score,
        funding=f.funding_now,
        price_d5m=f.price_delta_5m,
        oi_d5m=f.oi_delta_5m,
        liq_l=f.liq_long_5m_usd,
        taker1m=f.taker_buy_sell_ratio_1m,
        spread=f.spread_bps,
        atr=f.atr_1m_bps,
    )
    return score


def lock_decision(
    f: FeatureSnapshot, cfg: Dict
) -> Tuple[DecisionType, int, int]:
    """
    Compute both scores and lock a trading decision.

    Returns (DecisionType, long_score, short_score).
    If feature snapshot is stale, always returns NO_TRADE.
    """
    if f.is_stale:
        logger.warning("Stale snapshot — returning NO_TRADE", symbol=f.symbol)
        return DecisionType.NO_TRADE, 0, 0

    thresholds = cfg.get("thresholds", {})
    long_threshold = int(thresholds.get("long_threshold", 6))
    short_threshold = int(thresholds.get("short_threshold", 6))
    long_conflict_cap = int(thresholds.get("long_conflict_cap", 4))
    short_conflict_cap = int(thresholds.get("short_conflict_cap", 4))

    long_score = compute_long_score(f, cfg)
    short_score = compute_short_score(f, cfg)

    logger.info(
        "lock_decision",
        symbol=f.symbol,
        long_score=long_score,
        short_score=short_score,
        long_threshold=long_threshold,
        short_threshold=short_threshold,
    )

    # LONG wins if long score meets threshold and short score is not conflicting
    if long_score >= long_threshold and short_score <= short_conflict_cap:
        return DecisionType.LONG_AT_ROLLOVER, long_score, short_score

    # SHORT wins if short score meets threshold and long score is not conflicting
    if short_score >= short_threshold and long_score <= long_conflict_cap:
        return DecisionType.SHORT_AT_ROLLOVER, long_score, short_score

    return DecisionType.NO_TRADE, long_score, short_score


def _classify_group(symbol: str) -> str:
    """Classify symbol into BTC / ETH / ALT bucket."""
    s = symbol.upper()
    if "BTC" in s:
        return "BTC"
    if "ETH" in s:
        return "ETH"
    return "ALT"
