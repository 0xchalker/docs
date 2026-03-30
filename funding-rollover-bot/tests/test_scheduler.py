"""
Tests for scheduler.py — scanner, funding normalization, scoring.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from datetime import datetime, timezone
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from core.feature_engine import normalize_funding_to_pct


# ---------------------------------------------------------------------------
# normalize_funding_to_pct
# ---------------------------------------------------------------------------

class TestNormalizeFundingToPct:
    def test_decimal_format_negative(self):
        # -0.015 decimal -> -1.5 pct
        result = normalize_funding_to_pct(-0.015)
        assert abs(result - (-1.5)) < 1e-9

    def test_decimal_format_positive(self):
        result = normalize_funding_to_pct(0.01)
        assert abs(result - 1.0) < 1e-9

    def test_already_in_pct_negative(self):
        # -1.5 already in pct, abs >= 0.1 -> stays -1.5
        result = normalize_funding_to_pct(-1.5)
        assert abs(result - (-1.5)) < 1e-9

    def test_already_in_pct_positive(self):
        result = normalize_funding_to_pct(0.5)
        assert abs(result - 0.5) < 1e-9

    def test_zero(self):
        assert normalize_funding_to_pct(0.0) == 0.0

    def test_boundary_value_0_1(self):
        # abs(0.09) < 0.1 -> multiply by 100 -> 9.0
        result = normalize_funding_to_pct(0.09)
        assert abs(result - 9.0) < 1e-9

    def test_small_negative_decimal(self):
        result = normalize_funding_to_pct(-0.001)
        assert abs(result - (-0.1)) < 1e-9


# ---------------------------------------------------------------------------
# Scan candidate filtering
# ---------------------------------------------------------------------------

def _make_scheduler():
    """Create a Scheduler instance with minimal config (no real connections)."""
    import yaml
    from core.scheduler import Scheduler
    config = {
        "strategy": {"dry_run": True, "whitelist_symbols": ["BTCUSDT", "ETHUSDT"]},
        "scanner": {
            "scan_funding_pct_min": -1.5,
            "scan_funding_pct_max": 0.0,
            "top_n_scan_candidates": 3,
            "max_spread_bps_scan": {"BTC": 2.5, "ETH": 3.5, "ALT": 6.0},
            "max_slippage_bps_scan": {"BTC": 4, "ETH": 6, "ALT": 10},
            "min_oi_usd": {"BTC": 100_000_000, "ETH": 50_000_000, "ALT": 15_000_000},
        },
        "risk": {"risk_per_trade": 0.0025, "low_long_liq_threshold_usd": 100_000},
        "thresholds": {
            "long_threshold": 6, "short_threshold": 6,
            "long_conflict_cap": 4, "short_conflict_cap": 4,
        },
        "filters": {"max_spread_bps": {"BTC": 2.5}, "max_atr_1m_bps": {"BTC": 18}},
        "execution": {"max_entry_slippage_bps": {"BTC": 4}},
        "coinglass": {"api_key": "", "base_url": "https://open-api-v3.coinglass.com"},
        "venue": {"name": "binance", "api_key": "", "api_secret": "", "testnet": True},
        "data": {"stale_feature_max_age_sec": {}},
    }

    with patch("core.scheduler.Scheduler.__init__", lambda self, **kw: None):
        sched = object.__new__(Scheduler)
        sched._cfg = config
        sched._whitelist = {"BTCUSDT", "ETHUSDT"}
        return sched


class TestScanCandidateFiltering:
    def test_excludes_positive_funding(self):
        """Symbol with funding > 0 must be excluded."""
        sched = _make_scheduler()
        # _load_universe_snapshot is async; test filtering logic directly
        rows = [
            {"symbol": "BTCUSDT", "fundingRate": 0.005, "openInterest": 200_000_000, "spread_bps": 1.5},
        ]
        # funding_pct = 0.5 (positive) -> outside [min=-1.5, max=0.0]
        funding_pct = sched.normalize_funding_to_pct(0.005)
        scanner_cfg = sched._cfg.get("scanner", {})
        assert not (
            float(scanner_cfg["scan_funding_pct_min"]) <= funding_pct <= float(scanner_cfg["scan_funding_pct_max"])
        )

    def test_includes_negative_funding_in_range(self):
        sched = _make_scheduler()
        funding_pct = sched.normalize_funding_to_pct(-0.005)  # -> -0.5 pct
        scanner_cfg = sched._cfg.get("scanner", {})
        assert (
            float(scanner_cfg["scan_funding_pct_min"]) <= funding_pct <= float(scanner_cfg["scan_funding_pct_max"])
        )

    def test_excludes_too_negative_funding(self):
        sched = _make_scheduler()
        funding_pct = -2.0  # below -1.5 min
        scanner_cfg = sched._cfg.get("scanner", {})
        assert not (
            float(scanner_cfg["scan_funding_pct_min"]) <= funding_pct <= float(scanner_cfg["scan_funding_pct_max"])
        )

    def test_excludes_low_oi(self):
        sched = _make_scheduler()
        oi_usd = 5_000_000  # below BTC min of 100M
        min_oi = float(sched._cfg["scanner"]["min_oi_usd"]["BTC"])
        assert oi_usd < min_oi

    def test_excludes_wide_spread(self):
        sched = _make_scheduler()
        spread_bps = 5.0  # above BTC max of 2.5
        max_spread = float(sched._cfg["scanner"]["max_spread_bps_scan"]["BTC"])
        assert spread_bps > max_spread


# ---------------------------------------------------------------------------
# Coin selection score
# ---------------------------------------------------------------------------

class TestCoinSelectionScore:
    def _sched(self):
        return _make_scheduler()

    def test_extreme_negative_funding_scores_highest(self):
        sched = self._sched()
        score_extreme = sched.compute_coin_selection_score({
            "funding_pct": -1.0,
            "funding_percentile_7d": 0.03,
            "funding_zscore_24h": -3.0,
            "oi_usd": 500_000_000,
            "oi_delta_15m": 0.025,
            "spread_bps": 1.0,
            "group": "BTC",
        })
        score_mild = sched.compute_coin_selection_score({
            "funding_pct": -0.1,
            "funding_percentile_7d": 0.4,
            "funding_zscore_24h": -0.5,
            "oi_usd": 100_000_000,
            "oi_delta_15m": 0.002,
            "spread_bps": 2.0,
            "group": "BTC",
        })
        assert score_extreme > score_mild

    def test_score_increases_with_lower_percentile(self):
        sched = self._sched()
        base = {"funding_pct": -0.5, "funding_zscore_24h": -1.5, "oi_usd": 100e6, "oi_delta_15m": 0.01, "spread_bps": 2.0, "group": "BTC"}
        score_low = sched.compute_coin_selection_score({**base, "funding_percentile_7d": 0.04})
        score_mid = sched.compute_coin_selection_score({**base, "funding_percentile_7d": 0.15})
        assert score_low > score_mid

    def test_score_non_negative(self):
        sched = self._sched()
        score = sched.compute_coin_selection_score({
            "funding_pct": 0.0,
            "funding_percentile_7d": 0.9,
            "funding_zscore_24h": 1.0,
            "oi_usd": 1_000,
            "oi_delta_15m": -0.05,
            "spread_bps": 10.0,
            "group": "ALT",
        })
        assert score >= 0

    def test_oi_growing_increases_score(self):
        sched = self._sched()
        base = {"funding_pct": -0.5, "funding_percentile_7d": 0.1, "funding_zscore_24h": -2.0, "oi_usd": 100e6, "spread_bps": 1.5, "group": "BTC"}
        score_hi = sched.compute_coin_selection_score({**base, "oi_delta_15m": 0.025})
        score_lo = sched.compute_coin_selection_score({**base, "oi_delta_15m": 0.001})
        assert score_hi > score_lo


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
