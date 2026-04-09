"""
Tests for execution guards: slippage, entry window, dry run, position sizing.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import asyncio
from datetime import datetime, timezone, timedelta
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from core.models import DecisionType, FeatureSnapshot, FundingEvent, OrderResult
from core.execution_engine import ExecutionEngine
from core.risk_engine import RiskEngine

UTC = timezone.utc


def _make_snap(spread_bps=1.5, atr_1m_bps=10.0, price_now=50_000.0, is_stale=False, **kw):
    return FeatureSnapshot(
        symbol="BTCUSDT",
        exchange="binance",
        timestamp=datetime.now(UTC),
        funding_now=-0.8,
        funding_oi_weighted_now=-0.9,
        funding_percentile_7d=0.07,
        funding_zscore_24h=-2.2,
        oi_delta_5m=0.018,
        oi_delta_15m=0.033,
        price_now=price_now,
        price_5m_ago=price_now * 0.998,
        price_delta_5m=0.002,
        high_5m=price_now * 1.001,
        low_5m=price_now * 0.998,
        high_15m=price_now * 1.002,
        low_15m=price_now * 0.995,
        dist_to_15m_low=0.005,
        liq_short_5m_usd=400_000,
        liq_long_5m_usd=80_000,
        taker_buy_sell_ratio_1m=1.05,
        taker_buy_sell_ratio_5m=1.03,
        spread_bps=spread_bps,
        atr_1m_bps=atr_1m_bps,
        slippage_estimate_bps=spread_bps * 1.5,
        is_stale=is_stale,
        **kw,
    )


_CFG = {
    "strategy": {"dry_run": True},
    "thresholds": {
        "long_threshold": 6, "short_threshold": 6,
        "long_conflict_cap": 4, "short_conflict_cap": 4,
        "funding_zscore_long": -1.8, "funding_percentile_long": 0.10,
        "oi_delta_15m_long": 0.015, "oi_delta_5m_short": 0.008,
        "price_delta_5m_short": 0.0015, "taker_ratio_short_cap_1m": 0.98,
    },
    "filters": {
        "max_spread_bps": {"BTC": 2.5, "ETH": 3.5, "ALT": 6.0},
        "max_atr_1m_bps": {"BTC": 18, "ETH": 24, "ALT": 35},
    },
    "risk": {
        "risk_per_trade": 0.0025,
        "low_long_liq_threshold_usd": 100_000,
        "stop_distance_pct": {"BTC": 0.0012, "ETH": 0.0015, "ALT": 0.002},
        "tp1_pct": {"BTC": 0.0016, "ETH": 0.002, "ALT": 0.0024},
        "tp2_pct": {"BTC": 0.0028, "ETH": 0.0035, "ALT": 0.004},
        "max_hold_seconds": {"LONG_AT_ROLLOVER": 45, "SHORT_AT_ROLLOVER": 30},
        "max_consecutive_losses": 2,
        "daily_max_loss_pct": 0.015,
    },
    "execution": {
        "max_entry_slippage_bps": {"BTC": 4, "ETH": 6, "ALT": 10},
    },
    "data": {"stale_feature_max_age_sec": {}},
}


def _make_execution_engine(dry_run=True):
    mock_venue = AsyncMock()
    mock_venue.get_account_equity = AsyncMock(return_value=10_000.0)
    mock_venue.get_mark_price = AsyncMock(return_value=50_000.0)
    mock_venue.get_best_bid_ask = AsyncMock(return_value=(49_990.0, 50_010.0, 2.0))
    mock_venue.get_best_marketable_limit_price = AsyncMock(return_value=50_015.0)
    mock_venue.place_order = AsyncMock(return_value=OrderResult(
        accepted=True, order_id="TEST-001",
        fill_price=50_015.0, fill_qty=0.024, slippage_bps=0.3,
    ))
    mock_venue.close_position = AsyncMock(return_value=OrderResult(
        accepted=True, order_id="TEST-002", fill_price=50_060.0, fill_qty=0.024,
    ))
    mock_venue.partial_close = AsyncMock(return_value=OrderResult(
        accepted=True, fill_price=50_040.0, fill_qty=0.012,
    ))
    risk = RiskEngine(_CFG)
    return ExecutionEngine(mock_venue, risk, _CFG, dry_run=dry_run), mock_venue


def _make_event():
    return FundingEvent(
        symbol="BTCUSDT",
        exchange="binance",
        funding_timestamp=datetime.now(UTC),
    )


# ---------------------------------------------------------------------------
# Slippage guard
# ---------------------------------------------------------------------------

class TestSlippageGuard:
    def test_blocks_entry_when_spread_too_high(self):
        engine, _ = _make_execution_engine()
        # BTC max_entry_slippage_bps = 4; spread_bps=10 > 4
        snap = _make_snap(spread_bps=10.0)
        event = _make_event()
        result = asyncio.get_event_loop().run_until_complete(
            engine.enter_at_rollover(event, DecisionType.LONG_AT_ROLLOVER, snap)
        )
        assert not result.accepted
        assert "spread" in result.reject_reason.lower() or "bps" in result.reject_reason.lower()

    def test_allows_entry_when_spread_within_limit(self):
        engine, mock_venue = _make_execution_engine()
        snap = _make_snap(spread_bps=1.5)
        event = _make_event()
        result = asyncio.get_event_loop().run_until_complete(
            engine.enter_at_rollover(event, DecisionType.LONG_AT_ROLLOVER, snap)
        )
        assert result.accepted

    def test_blocks_short_entry_when_spread_too_high(self):
        engine, _ = _make_execution_engine()
        snap = _make_snap(spread_bps=8.0)
        event = _make_event()
        result = asyncio.get_event_loop().run_until_complete(
            engine.enter_at_rollover(event, DecisionType.SHORT_AT_ROLLOVER, snap)
        )
        assert not result.accepted


# ---------------------------------------------------------------------------
# Dry run mode
# ---------------------------------------------------------------------------

class TestDryRunMode:
    def test_dry_run_returns_accepted_without_real_order(self):
        """In dry_run mode, place_order is still called on the mock but bot doesn't blow up."""
        engine, mock_venue = _make_execution_engine(dry_run=True)
        snap = _make_snap(spread_bps=1.5)
        event = _make_event()
        result = asyncio.get_event_loop().run_until_complete(
            engine.enter_at_rollover(event, DecisionType.LONG_AT_ROLLOVER, snap)
        )
        assert result.accepted

    def test_no_trade_returns_rejected(self):
        engine, _ = _make_execution_engine()
        snap = _make_snap()
        event = _make_event()
        result = asyncio.get_event_loop().run_until_complete(
            engine.enter_at_rollover(event, DecisionType.NO_TRADE, snap)
        )
        assert not result.accepted
        assert result.reject_reason == "NO_TRADE decision"


# ---------------------------------------------------------------------------
# Position sizing
# ---------------------------------------------------------------------------

class TestPositionSizing:
    def test_size_scales_with_equity(self):
        risk = RiskEngine(_CFG)
        size_small = risk.compute_size("BTCUSDT", 0.0012, 5_000.0)
        size_large = risk.compute_size("BTCUSDT", 0.0012, 10_000.0)
        assert abs(size_large - 2 * size_small) < 1e-6

    def test_size_scales_inversely_with_stop_distance(self):
        risk = RiskEngine(_CFG)
        size_tight = risk.compute_size("BTCUSDT", 0.001, 10_000.0)
        size_wide = risk.compute_size("BTCUSDT", 0.002, 10_000.0)
        assert size_tight > size_wide

    def test_size_respects_max_notional(self):
        risk = RiskEngine(_CFG)
        # Very small stop = huge position, but capped at max_notional
        size = risk.compute_size("BTCUSDT", 0.0001, 10_000.0, max_notional=500.0)
        assert size <= 500.0

    def test_zero_stop_returns_zero(self):
        risk = RiskEngine(_CFG)
        size = risk.compute_size("BTCUSDT", 0.0, 10_000.0)
        assert size == 0.0

    def test_risk_per_trade_respected(self):
        risk = RiskEngine(_CFG)
        # risk_per_trade = 0.0025, equity = 10000, stop = 0.0012
        # Expected: (10000 * 0.0025) / 0.0012 = 25 / 0.0012 = 20833
        notional = risk.compute_size("BTCUSDT", 0.0012, 10_000.0)
        expected = (10_000 * 0.0025) / 0.0012
        assert abs(notional - expected) < 0.01


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
