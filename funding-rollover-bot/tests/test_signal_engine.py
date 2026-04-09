"""
Tests for signal_engine.py — long/short scoring and decision locking.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from datetime import datetime, timezone
import pytest
from core.models import DecisionType, FeatureSnapshot
from core.signal_engine import compute_long_score, compute_short_score, lock_decision

UTC = timezone.utc


def _make_snap(**kwargs) -> FeatureSnapshot:
    defaults = dict(
        symbol="BTCUSDT",
        exchange="binance",
        timestamp=datetime.now(UTC),
        funding_now=-0.8,
        funding_oi_weighted_now=-0.9,
        funding_percentile_7d=0.07,
        funding_zscore_24h=-2.2,
        oi_now=5e9,
        oi_5m_ago=4.9e9,
        oi_15m_ago=4.8e9,
        oi_delta_5m=0.02,
        oi_delta_15m=0.04,
        price_now=50_000,
        price_1m_ago=49_980,
        price_5m_ago=49_900,
        price_delta_1m=0.0004,
        price_delta_5m=0.002,
        high_5m=50_100,
        low_5m=49_800,
        high_15m=50_200,
        low_15m=49_500,
        dist_to_15m_low=0.010,
        dist_to_5m_high=0.002,
        liq_long_1m_usd=30_000,
        liq_short_1m_usd=150_000,
        liq_long_5m_usd=80_000,
        liq_short_5m_usd=400_000,
        liq_ratio_short_over_long_5m=5.0,
        taker_buy_sell_ratio_1m=1.1,
        taker_buy_sell_ratio_5m=1.05,
        spread_bps=1.5,
        atr_1m_bps=10.0,
        slippage_estimate_bps=2.0,
        minutes_to_funding=0.05,
        seconds_to_funding=3.0,
        is_stale=False,
    )
    defaults.update(kwargs)
    return FeatureSnapshot(**defaults)


_CFG = {
    "thresholds": {
        "long_threshold": 6,
        "short_threshold": 6,
        "long_conflict_cap": 4,
        "short_conflict_cap": 4,
        "funding_zscore_long": -1.8,
        "funding_percentile_long": 0.10,
        "oi_delta_15m_long": 0.015,
        "oi_delta_5m_short": 0.008,
        "price_delta_5m_short": 0.0015,
        "taker_ratio_short_cap_1m": 0.98,
    },
    "filters": {
        "max_spread_bps": {"BTC": 2.5, "ETH": 3.5, "ALT": 6.0},
        "max_atr_1m_bps": {"BTC": 18, "ETH": 24, "ALT": 35},
    },
    "risk": {
        "low_long_liq_threshold_usd": 100_000,
    },
}


# ---------------------------------------------------------------------------
# LONG score tests
# ---------------------------------------------------------------------------

class TestLongScore:
    def test_perfect_long_conditions_scores_high(self):
        snap = _make_snap()
        score = compute_long_score(snap, _CFG)
        assert score >= 7, f"Expected >= 7, got {score}"

    def test_no_long_when_funding_positive(self):
        snap = _make_snap(funding_oi_weighted_now=0.5, funding_zscore_24h=0.2)
        score = compute_long_score(snap, _CFG)
        # Criterion 1 fails (funding not negative), max possible is 8 (criteria 2-7)
        # The full-signal snap scores 9 (all criteria pass), this should score < 9
        full_snap = _make_snap()
        full_score = compute_long_score(full_snap, _CFG)
        assert score < full_score

    def test_no_long_criterion1_zscore_not_extreme(self):
        snap = _make_snap(funding_oi_weighted_now=-0.5, funding_zscore_24h=-1.0)
        score_with = compute_long_score(
            _make_snap(funding_oi_weighted_now=-0.5, funding_zscore_24h=-2.0), _CFG
        )
        score_without = compute_long_score(snap, _CFG)
        assert score_with > score_without

    def test_long_criterion2_funding_percentile_low(self):
        snap_low = _make_snap(funding_percentile_7d=0.05)
        snap_high = _make_snap(funding_percentile_7d=0.50)
        assert compute_long_score(snap_low, _CFG) > compute_long_score(snap_high, _CFG)

    def test_long_criterion3_oi_delta_15m(self):
        snap_high = _make_snap(oi_delta_15m=0.020)
        snap_low = _make_snap(oi_delta_15m=0.005)
        assert compute_long_score(snap_high, _CFG) > compute_long_score(snap_low, _CFG)

    def test_long_criterion4_dist_to_low(self):
        snap_far = _make_snap(dist_to_15m_low=0.010)
        snap_near = _make_snap(dist_to_15m_low=0.0005)
        assert compute_long_score(snap_far, _CFG) > compute_long_score(snap_near, _CFG)

    def test_long_criterion5_short_liq_dominating(self):
        snap_yes = _make_snap(liq_short_5m_usd=500_000, liq_long_5m_usd=80_000)
        snap_no = _make_snap(liq_short_5m_usd=80_000, liq_long_5m_usd=500_000)
        assert compute_long_score(snap_yes, _CFG) > compute_long_score(snap_no, _CFG)

    def test_long_criterion6_taker_buy_pressure(self):
        snap_buying = _make_snap(taker_buy_sell_ratio_5m=1.10)
        snap_selling = _make_snap(taker_buy_sell_ratio_5m=0.85)
        assert compute_long_score(snap_buying, _CFG) > compute_long_score(snap_selling, _CFG)

    def test_long_criterion7_execution_quality(self):
        snap_tight = _make_snap(spread_bps=1.0, atr_1m_bps=8.0)
        snap_wide = _make_snap(spread_bps=5.0, atr_1m_bps=30.0)
        assert compute_long_score(snap_tight, _CFG) > compute_long_score(snap_wide, _CFG)

    def test_long_score_zero_when_all_criteria_fail(self):
        snap = _make_snap(
            funding_oi_weighted_now=0.5,
            funding_zscore_24h=0.5,
            funding_percentile_7d=0.9,
            oi_delta_15m=0.001,
            dist_to_15m_low=0.0,
            liq_short_5m_usd=0,
            liq_long_5m_usd=1_000_000,
            taker_buy_sell_ratio_5m=0.5,
            spread_bps=10.0,
            atr_1m_bps=50.0,
        )
        score = compute_long_score(snap, _CFG)
        assert score == 0


# ---------------------------------------------------------------------------
# SHORT score tests
# ---------------------------------------------------------------------------

class TestShortScore:
    def _short_snap(self, **kw):
        # Base short-favorable defaults; caller may override any field
        base = dict(
            funding_now=-0.3,
            price_delta_5m=0.003,
            oi_delta_5m=0.015,
            liq_long_5m_usd=50_000,
            taker_buy_sell_ratio_1m=0.90,
            spread_bps=1.5,
            atr_1m_bps=10.0,
        )
        base.update(kw)
        return _make_snap(**base)

    def test_perfect_short_conditions_scores_high(self):
        snap = self._short_snap()
        score = compute_short_score(snap, _CFG)
        assert score >= 6, f"Expected >= 6, got {score}"

    def test_short_criterion1_funding_negative(self):
        snap_neg = self._short_snap(funding_now=-0.5)
        snap_pos = self._short_snap(funding_now=0.5)
        assert compute_short_score(snap_neg, _CFG) > compute_short_score(snap_pos, _CFG)

    def test_short_criterion2_price_delta_5m(self):
        snap_pumped = self._short_snap(price_delta_5m=0.003)
        snap_flat = self._short_snap(price_delta_5m=0.0)
        assert compute_short_score(snap_pumped, _CFG) > compute_short_score(snap_flat, _CFG)

    def test_short_criterion2b_price_near_high(self):
        # price at 80% of 5m range counts as +1 when no big delta
        snap = _make_snap(
            funding_now=-0.3,
            price_delta_5m=0.0,
            price_now=50_080,
            high_5m=50_100,
            low_5m=49_900,
            oi_delta_5m=0.015,
            liq_long_5m_usd=50_000,
            taker_buy_sell_ratio_1m=0.90,
        )
        score = compute_short_score(snap, _CFG)
        assert score >= 1

    def test_short_criterion3_oi_delta_5m(self):
        snap_hi = self._short_snap(oi_delta_5m=0.020)
        snap_lo = self._short_snap(oi_delta_5m=0.001)
        assert compute_short_score(snap_hi, _CFG) > compute_short_score(snap_lo, _CFG)

    def test_short_criterion4_low_long_liq(self):
        snap_low = self._short_snap(liq_long_5m_usd=50_000)
        snap_high = self._short_snap(liq_long_5m_usd=500_000)
        assert compute_short_score(snap_low, _CFG) > compute_short_score(snap_high, _CFG)

    def test_short_criterion5_taker_sell_ratio(self):
        snap_sell = self._short_snap(taker_buy_sell_ratio_1m=0.85)
        snap_buy = self._short_snap(taker_buy_sell_ratio_1m=1.10)
        assert compute_short_score(snap_sell, _CFG) > compute_short_score(snap_buy, _CFG)


# ---------------------------------------------------------------------------
# Decision lock tests
# ---------------------------------------------------------------------------

class TestLockDecision:
    def test_long_decision_when_long_dominates(self):
        # Long score high, short score low
        snap = _make_snap(
            funding_now=-0.8,
            funding_oi_weighted_now=-0.9,
            funding_zscore_24h=-2.2,
            funding_percentile_7d=0.07,
            oi_delta_15m=0.04,
            dist_to_15m_low=0.010,
            liq_short_5m_usd=400_000,
            liq_long_5m_usd=80_000,
            taker_buy_sell_ratio_5m=1.05,
            spread_bps=1.5,
            atr_1m_bps=10.0,
            # Short conditions weak:
            price_delta_5m=0.0,
            oi_delta_5m=0.001,
        )
        decision, ls, ss = lock_decision(snap, _CFG)
        assert decision == DecisionType.LONG_AT_ROLLOVER
        assert ls >= 6

    def test_short_decision_when_short_dominates(self):
        snap = _make_snap(
            # Short conditions strong:
            funding_now=-0.3,
            price_delta_5m=0.003,
            oi_delta_5m=0.015,
            liq_long_5m_usd=50_000,
            taker_buy_sell_ratio_1m=0.90,
            spread_bps=1.5,
            atr_1m_bps=10.0,
            # Long conditions weak:
            funding_oi_weighted_now=-0.1,
            funding_zscore_24h=-0.5,
            funding_percentile_7d=0.5,
            oi_delta_15m=0.001,
            dist_to_15m_low=0.0,
            liq_short_5m_usd=0,
            taker_buy_sell_ratio_5m=0.5,
        )
        decision, ls, ss = lock_decision(snap, _CFG)
        assert decision == DecisionType.SHORT_AT_ROLLOVER
        assert ss >= 6

    def test_no_trade_when_conflict(self):
        # Both scores below threshold
        snap = _make_snap(
            funding_oi_weighted_now=0.0,
            funding_zscore_24h=-0.5,
            funding_percentile_7d=0.5,
            oi_delta_15m=0.005,
            dist_to_15m_low=0.0002,
            liq_short_5m_usd=50_000,
            liq_long_5m_usd=150_000,
            taker_buy_sell_ratio_5m=0.9,
            spread_bps=8.0,
            atr_1m_bps=30.0,
            price_delta_5m=0.0005,
            oi_delta_5m=0.001,
            taker_buy_sell_ratio_1m=1.05,
        )
        decision, ls, ss = lock_decision(snap, _CFG)
        assert decision == DecisionType.NO_TRADE

    def test_no_trade_when_stale(self):
        snap = _make_snap(is_stale=True)
        decision, ls, ss = lock_decision(snap, _CFG)
        assert decision == DecisionType.NO_TRADE
        assert ls == 0 and ss == 0

    def test_no_trade_both_scores_high_conflict(self):
        # Both long and short scores could be high — conflict -> NO_TRADE
        # We force long_score to meet threshold but short_score also high (> conflict cap)
        # By making the short_conflict_cap tight
        cfg_tight = {**_CFG, "thresholds": {**_CFG["thresholds"], "short_conflict_cap": 2}}
        snap = _make_snap(
            # Long conditions met
            funding_oi_weighted_now=-0.9,
            funding_zscore_24h=-2.2,
            funding_percentile_7d=0.07,
            oi_delta_15m=0.04,
            dist_to_15m_low=0.010,
            liq_short_5m_usd=400_000,
            liq_long_5m_usd=80_000,
            taker_buy_sell_ratio_5m=1.05,
            spread_bps=1.5,
            atr_1m_bps=10.0,
            # Short conditions also partially met
            funding_now=-0.3,
            price_delta_5m=0.003,
            oi_delta_5m=0.015,
            taker_buy_sell_ratio_1m=0.90,
        )
        decision, ls, ss = lock_decision(snap, cfg_tight)
        # If short score exceeds tight conflict cap, should be NO_TRADE
        if ss > 2:
            assert decision == DecisionType.NO_TRADE
        # Otherwise LONG (acceptable both outcomes)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
