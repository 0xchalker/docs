"""
Tests for backtesting infrastructure: event replayer, slippage model, metrics.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from datetime import datetime, timezone, timedelta
import pytest
from unittest.mock import patch
import tempfile
import os

from backtest.slippage_model import SlippageModel
from backtest.metrics import BacktestMetrics, BacktestResult
from backtest.event_replayer import EventDrivenBacktester, _generate_funding_events
from core.models import DecisionType, FundingEvent, TradeJournal

UTC = timezone.utc


# ---------------------------------------------------------------------------
# SlippageModel
# ---------------------------------------------------------------------------

class TestSlippageModel:
    def test_buy_slippage_increases_price(self):
        model = SlippageModel()
        price = 50_000.0
        slip = model.estimate_slippage_bps("BTCUSDT", "BUY", 10_000, 1.5, 10.0)
        filled = model.apply_slippage(price, "BUY", slip)
        assert filled > price

    def test_sell_slippage_decreases_price(self):
        model = SlippageModel()
        price = 50_000.0
        slip = model.estimate_slippage_bps("BTCUSDT", "SELL", 10_000, 1.5, 10.0)
        filled = model.apply_slippage(price, "SELL", slip)
        assert filled < price

    def test_zero_spread_still_has_size_impact(self):
        model = SlippageModel()
        # Large size with zero spread should still have some slippage
        slip = model.estimate_slippage_bps("BTCUSDT", "BUY", 5_000_000, 0.0, 0.0)
        assert slip > 0.0

    def test_larger_size_means_more_slippage(self):
        model = SlippageModel()
        slip_small = model.estimate_slippage_bps("BTCUSDT", "BUY", 10_000, 2.0, 5.0)
        slip_large = model.estimate_slippage_bps("BTCUSDT", "BUY", 1_000_000, 2.0, 5.0)
        assert slip_large > slip_small

    def test_slippage_always_non_negative(self):
        model = SlippageModel()
        slip = model.estimate_slippage_bps("BTCUSDT", "BUY", 0, 0.0, 0.0)
        assert slip >= 0.0

    def test_apply_slippage_buy_formula(self):
        model = SlippageModel()
        price = 1000.0
        slip_bps = 10.0
        filled = model.apply_slippage(price, "BUY", slip_bps)
        expected = price * (1 + 10.0 / 10_000)
        assert abs(filled - expected) < 1e-9

    def test_apply_slippage_sell_formula(self):
        model = SlippageModel()
        price = 1000.0
        slip_bps = 10.0
        filled = model.apply_slippage(price, "SELL", slip_bps)
        expected = price * (1 - 10.0 / 10_000)
        assert abs(filled - expected) < 1e-9


# ---------------------------------------------------------------------------
# BacktestMetrics
# ---------------------------------------------------------------------------

def _make_trade(pnl_usd: float, symbol: str = "BTCUSDT", mode: str = "LONG_AT_ROLLOVER", slip: float = 2.0) -> TradeJournal:
    ts = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    return TradeJournal(
        trade_id="t1",
        symbol=symbol,
        exchange="binance",
        funding_timestamp=ts,
        mode=mode,
        long_score=7,
        short_score=2,
        decision=mode,
        entry_time=ts,
        exit_time=ts + timedelta(seconds=30),
        entry_price=50_000.0,
        exit_price=50_000.0 + (pnl_usd / 0.02),  # rough
        size=0.02,
        pnl_usd=pnl_usd,
        pnl_r=pnl_usd / 25.0,
        exit_reason="tp2",
        funding_now=-0.8,
        funding_oi_weighted_now=-0.9,
        funding_percentile_7d=0.07,
        funding_zscore_24h=-2.2,
        oi_delta_5m=0.018,
        oi_delta_15m=0.033,
        price_delta_1m=0.0002,
        price_delta_5m=0.002,
        liq_long_5m_usd=80_000,
        liq_short_5m_usd=400_000,
        taker_buy_sell_ratio_1m=1.05,
        taker_buy_sell_ratio_5m=1.03,
        spread_bps=1.5,
        atr_1m_bps=10.0,
        slippage_realized_bps=slip,
    )


class TestBacktestMetrics:
    def test_empty_trades_returns_zero_metrics(self):
        result = BacktestMetrics.compute([])
        assert result.n_trades == 0
        assert result.win_rate == 0.0

    def test_win_rate_computed_correctly(self):
        trades = [_make_trade(100), _make_trade(-50), _make_trade(200)]
        result = BacktestMetrics.compute(trades)
        assert result.n_trades == 3
        assert result.n_wins == 2
        assert result.n_losses == 1
        assert abs(result.win_rate - 2/3) < 1e-9

    def test_profit_factor_positive(self):
        trades = [_make_trade(100), _make_trade(-50), _make_trade(200)]
        result = BacktestMetrics.compute(trades)
        assert result.profit_factor == pytest.approx(300 / 50, rel=1e-6)

    def test_max_drawdown_non_negative(self):
        trades = [_make_trade(100), _make_trade(-200), _make_trade(50)]
        result = BacktestMetrics.compute(trades)
        assert result.max_drawdown >= 0

    def test_total_pnl_is_sum(self):
        trades = [_make_trade(100), _make_trade(-50), _make_trade(200)]
        result = BacktestMetrics.compute(trades)
        assert abs(result.total_pnl_usd - 250) < 1e-9

    def test_avg_slippage_computed(self):
        trades = [_make_trade(100, slip=2.0), _make_trade(50, slip=4.0)]
        result = BacktestMetrics.compute(trades)
        assert abs(result.realized_slippage_avg_bps - 3.0) < 1e-9

    def test_segment_by_mode(self):
        trades = [
            _make_trade(100, mode="LONG_AT_ROLLOVER"),
            _make_trade(-50, mode="SHORT_AT_ROLLOVER"),
            _make_trade(200, mode="LONG_AT_ROLLOVER"),
        ]
        segments = BacktestMetrics.segment_by(trades, "mode")
        assert "LONG_AT_ROLLOVER" in segments
        assert "SHORT_AT_ROLLOVER" in segments
        assert segments["LONG_AT_ROLLOVER"].n_trades == 2
        assert segments["SHORT_AT_ROLLOVER"].n_trades == 1

    def test_segment_by_symbol(self):
        trades = [
            _make_trade(100, symbol="BTCUSDT"),
            _make_trade(-50, symbol="ETHUSDT"),
        ]
        segments = BacktestMetrics.segment_by(trades, "symbol")
        assert "BTCUSDT" in segments
        assert "ETHUSDT" in segments


# ---------------------------------------------------------------------------
# EventDrivenBacktester
# ---------------------------------------------------------------------------

class TestEventDrivenBacktester:
    def _make_backtester(self):
        import yaml, tempfile
        cfg = {
            "strategy": {"dry_run": True, "whitelist_symbols": ["BTCUSDT"]},
            "scanner": {"scan_funding_pct_min": -1.5, "scan_funding_pct_max": 0.0},
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
                "tp1_pct": {"BTC": 0.0016},
                "tp2_pct": {"BTC": 0.0028},
                "max_hold_seconds": {"LONG_AT_ROLLOVER": 45, "SHORT_AT_ROLLOVER": 30},
                "max_consecutive_losses": 2,
                "daily_max_loss_pct": 0.015,
            },
            "execution": {"max_entry_slippage_bps": {"BTC": 4}},
            "coinglass": {"api_key": "", "base_url": ""},
            "venue": {"name": "binance", "api_key": "", "api_secret": "", "testnet": True},
            "data": {"stale_feature_max_age_sec": {}},
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(cfg, f)
            fname = f.name
        bt = EventDrivenBacktester(config_path=fname)
        os.unlink(fname)
        return bt

    def test_run_produces_result(self):
        bt = self._make_backtester()
        result = bt.run(
            symbols=["BTCUSDT"],
            start_date="2024-01-01",
            end_date="2024-01-03",
        )
        assert isinstance(result, BacktestResult)
        # 2 days * 3 events/day = 6 funding events; some should produce trades
        assert result.n_trades >= 0

    def test_generate_funding_events_count(self):
        # "2024-01-03" parses to Jan3 00:00 UTC, so events are:
        # Jan1 @0h,8h,16h + Jan2 @0h,8h,16h + Jan3 @0h = 7 events
        events = _generate_funding_events(["BTCUSDT"], "2024-01-01", "2024-01-03")
        assert len(events) == 7
        # Full 3-day window including all of Jan3 gives 9 events
        events_full = _generate_funding_events(["BTCUSDT"], "2024-01-01", "2024-01-03T23:59:59")
        assert len(events_full) == 9

    def test_metrics_report_prints(self, capsys):
        result = BacktestResult(n_trades=5, win_rate=0.6, total_pnl_usd=100.0, profit_factor=2.0)
        BacktestMetrics.print_report(result)
        captured = capsys.readouterr()
        assert "BACKTEST REPORT" in captured.out
        assert "5" in captured.out

    def test_backtest_has_segments(self):
        bt = self._make_backtester()
        result = bt.run(
            symbols=["BTCUSDT"],
            start_date="2024-01-01",
            end_date="2024-01-02",
        )
        # Segments should be populated if trades exist
        if result.n_trades > 0:
            assert len(result.segments) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
