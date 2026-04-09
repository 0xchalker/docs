"""
Feature engine — computes and caches FeatureSnapshot objects.
Fetches data from CoinGlass REST, exchange WS ticks, and computes
derived fields like OI deltas, price deltas, funding z-scores.
"""
import asyncio
import time
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta
from typing import Any, Deque, Dict, List, Optional, Tuple

from core.models import FeatureSnapshot, FundingEvent
from utils.logging_utils import get_logger
from utils.math_utils import compute_percentile, compute_zscore, safe_div, pct_to_bps

logger = get_logger(__name__)

# Ring buffer sizes
_PRICE_HISTORY_SECONDS = 900  # 15 minutes
_MAX_TICKS = 10_000


def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def normalize_funding_to_pct(raw: float) -> float:
    """
    Normalize funding rate to percent representation.
    If abs(raw) < 0.1 assume it's a decimal (e.g., -0.015 -> -1.5).
    Otherwise assume it's already in percent units.
    """
    if abs(raw) < 0.1:
        return raw * 100.0
    return raw


class _PriceTick:
    __slots__ = ("ts", "price")

    def __init__(self, ts: float, price: float):
        self.ts = ts
        self.price = price


class FeatureEngine:
    """
    Maintains a per-symbol rolling window of price/OI/funding/liquidation data
    and computes FeatureSnapshot on demand.
    """

    def __init__(self, coinglass_client: Any, venue_ws: Any, config: Dict) -> None:
        self._cg = coinglass_client
        self._venue_ws = venue_ws
        self._cfg = config

        # symbol -> deque of _PriceTick
        self._price_ticks: Dict[str, Deque[_PriceTick]] = defaultdict(lambda: deque(maxlen=_MAX_TICKS))
        # symbol -> latest snapshot
        self._snapshots: Dict[str, FeatureSnapshot] = {}
        # symbol -> {ts: float, value: float} for OI
        self._oi_series: Dict[str, Deque[Tuple[float, float]]] = defaultdict(lambda: deque(maxlen=200))
        # symbol -> funding rate history list (float pct)
        self._funding_history: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=200))
        # symbol -> (bid, ask) latest
        self._book: Dict[str, Tuple[float, float]] = {}
        # symbol -> (liq_long_usd, liq_short_usd) rolling 5m
        self._liq_windows: Dict[str, Deque[Tuple[float, float, float]]] = defaultdict(lambda: deque(maxlen=500))
        # symbol -> (ts, buy_vol, sell_vol) rolling 5m taker
        self._taker_windows: Dict[str, Deque[Tuple[float, float, float]]] = defaultdict(lambda: deque(maxlen=500))
        # Freshness timestamps (monotonic)
        self._last_oi_update: Dict[str, float] = {}
        self._last_liq_update: Dict[str, float] = {}
        self._last_taker_update: Dict[str, float] = {}
        self._last_funding_update: Dict[str, float] = {}

    # -------------------------------------------------------------------------
    # External data ingestion
    # -------------------------------------------------------------------------

    def on_book_ticker(self, symbol: str, data: Dict) -> None:
        bid = float(data.get("b", 0))
        ask = float(data.get("a", 0))
        price = (bid + ask) / 2.0 if bid and ask else float(data.get("p", 0))
        if price > 0:
            self._book[symbol] = (bid, ask)
            self._price_ticks[symbol].append(_PriceTick(time.monotonic(), price))

    def on_mark_price(self, symbol: str, data: Dict) -> None:
        mark = float(data.get("p", 0))
        if mark > 0:
            self._price_ticks[symbol].append(_PriceTick(time.monotonic(), mark))

    def on_oi_update(self, symbol: str, oi_usd: float) -> None:
        self._oi_series[symbol].append((time.monotonic(), oi_usd))
        self._last_oi_update[symbol] = time.monotonic()

    def on_liquidation(self, symbol: str, side: str, usd_value: float) -> None:
        ts = time.monotonic()
        long_usd = usd_value if side.upper() in ("LONG", "BUY") else 0.0
        short_usd = usd_value if side.upper() in ("SHORT", "SELL") else 0.0
        self._liq_windows[symbol].append((ts, long_usd, short_usd))
        self._last_liq_update[symbol] = ts

    def on_taker_volume(self, symbol: str, buy_vol: float, sell_vol: float) -> None:
        ts = time.monotonic()
        self._taker_windows[symbol].append((ts, buy_vol, sell_vol))
        self._last_taker_update[symbol] = ts

    # -------------------------------------------------------------------------
    # Prefetch and refresh
    # -------------------------------------------------------------------------

    async def prefetch(self, event: FundingEvent) -> None:
        """Load historical data for a symbol to warm up rolling windows."""
        symbol = event.symbol
        exchange = event.exchange
        logger.info("Feature engine prefetch", symbol=symbol, exchange=exchange)

        now_ms = int(_now_utc().timestamp() * 1000)
        start_7d_ms = now_ms - 7 * 24 * 3600 * 1000
        start_1d_ms = now_ms - 24 * 3600 * 1000
        start_1h_ms = now_ms - 3600 * 1000

        try:
            # Funding history for percentile / z-score
            fr_hist = await self._cg.get_funding_rate_history(
                symbol, exchange, interval="h8", start=start_7d_ms, end=now_ms
            )
            for row in fr_hist:
                raw = float(row.get("fundingRate", row.get("rate", 0)))
                self._funding_history[symbol].append(normalize_funding_to_pct(raw))
            self._last_funding_update[symbol] = time.monotonic()

            # OI history for delta computation
            oi_hist = await self._cg.get_oi_history(
                symbol, exchange, interval="m1", start=start_1h_ms, end=now_ms
            )
            for row in oi_hist:
                ts_ms = float(row.get("t", row.get("time", 0)))
                oi_usd = float(row.get("oi", row.get("openInterest", 0)))
                self._oi_series[symbol].append((ts_ms / 1000.0, oi_usd))
            self._last_oi_update[symbol] = time.monotonic()

            # Liquidation history
            liq_hist = await self._cg.get_liquidation_history(
                symbol, exchange, interval="m1", start=start_1h_ms, end=now_ms
            )
            for row in liq_hist:
                ts_s = float(row.get("t", row.get("time", 0))) / 1000.0
                long_usd = float(row.get("longLiquidationUsd", row.get("buyLiqUsd", 0)))
                short_usd = float(row.get("shortLiquidationUsd", row.get("sellLiqUsd", 0)))
                self._liq_windows[symbol].append((ts_s, long_usd, short_usd))
            self._last_liq_update[symbol] = time.monotonic()

            # Taker volume history
            taker_hist = await self._cg.get_taker_buy_sell_history(
                symbol, exchange, interval="m1", start=start_1h_ms, end=now_ms
            )
            for row in taker_hist:
                ts_s = float(row.get("t", row.get("time", 0))) / 1000.0
                buy_vol = float(row.get("buyVol", row.get("takerBuyVol", 0)))
                sell_vol = float(row.get("sellVol", row.get("takerSellVol", 0)))
                self._taker_windows[symbol].append((ts_s, buy_vol, sell_vol))
            self._last_taker_update[symbol] = time.monotonic()

        except Exception as exc:
            logger.warning("Feature prefetch partial failure", symbol=symbol, error=str(exc))

    async def refresh_context(self, event: FundingEvent) -> None:
        """Medium-frequency update — called at T-30m, T-15m, T-5m."""
        await self.prefetch(event)

    async def refresh_fast(self, event: FundingEvent) -> None:
        """High-frequency update — called at T-60s and T-10s."""
        symbol = event.symbol
        exchange = event.exchange
        now_ms = int(_now_utc().timestamp() * 1000)
        start_5m_ms = now_ms - 5 * 60 * 1000
        try:
            # Short OI refresh
            oi_data = await self._cg.get_oi_history(
                symbol, exchange, interval="m1", start=start_5m_ms, end=now_ms
            )
            for row in oi_data:
                ts_ms = float(row.get("t", row.get("time", 0)))
                oi_usd = float(row.get("oi", row.get("openInterest", 0)))
                self._oi_series[symbol].append((ts_ms / 1000.0, oi_usd))
            self._last_oi_update[symbol] = time.monotonic()

            # Short liquidation refresh
            liq_data = await self._cg.get_liquidation_history(
                symbol, exchange, interval="m1", start=start_5m_ms, end=now_ms
            )
            for row in liq_data:
                ts_s = float(row.get("t", row.get("time", 0))) / 1000.0
                long_usd = float(row.get("longLiquidationUsd", row.get("buyLiqUsd", 0)))
                short_usd = float(row.get("shortLiquidationUsd", row.get("sellLiqUsd", 0)))
                self._liq_windows[symbol].append((ts_s, long_usd, short_usd))
            self._last_liq_update[symbol] = time.monotonic()

            # Short taker refresh
            taker_data = await self._cg.get_taker_buy_sell_history(
                symbol, exchange, interval="m1", start=start_5m_ms, end=now_ms
            )
            for row in taker_data:
                ts_s = float(row.get("t", row.get("time", 0))) / 1000.0
                buy_vol = float(row.get("buyVol", row.get("takerBuyVol", 0)))
                sell_vol = float(row.get("sellVol", row.get("takerSellVol", 0)))
                self._taker_windows[symbol].append((ts_s, buy_vol, sell_vol))
            self._last_taker_update[symbol] = time.monotonic()

        except Exception as exc:
            logger.warning("Feature fast refresh partial failure", symbol=symbol, error=str(exc))

    # -------------------------------------------------------------------------
    # Snapshot computation
    # -------------------------------------------------------------------------

    def get_snapshot(self, symbol: str, exchange: str, funding_ts: Optional[datetime] = None) -> FeatureSnapshot:
        """Build and return a current FeatureSnapshot."""
        snap = FeatureSnapshot(symbol=symbol, exchange=exchange, timestamp=_now_utc())
        now_mono = time.monotonic()

        # -- Price fields --
        ticks = self._price_ticks.get(symbol, deque())
        if ticks:
            prices = list(ticks)
            snap.price_now = prices[-1].price
            snap.price_1m_ago = self._price_at_seconds_ago(prices, 60)
            snap.price_5m_ago = self._price_at_seconds_ago(prices, 300)
            recent_prices = [t.price for t in prices if now_mono - t.ts <= 300]
            if recent_prices:
                snap.high_5m = max(recent_prices)
                snap.low_5m = min(recent_prices)
            recent_15m = [t.price for t in prices if now_mono - t.ts <= 900]
            if recent_15m:
                snap.high_15m = max(recent_15m)
                snap.low_15m = min(recent_15m)

        # -- OI fields --
        oi_series = list(self._oi_series.get(symbol, deque()))
        if oi_series:
            snap.oi_now = oi_series[-1][1]
            snap.oi_5m_ago = self._oi_at_seconds_ago(oi_series, 300)
            snap.oi_15m_ago = self._oi_at_seconds_ago(oi_series, 900)

        # -- Funding fields --
        fr_hist = list(self._funding_history.get(symbol, deque()))
        if fr_hist:
            snap.funding_now = fr_hist[-1]
            snap.funding_oi_weighted_now = fr_hist[-1]  # simplified; ideally OI-weighted
            snap.funding_percentile_7d = compute_percentile(fr_hist, snap.funding_now)
            recent_24h = fr_hist[-3:]  # approximately 24h at 8h intervals
            snap.funding_zscore_24h = compute_zscore(fr_hist, snap.funding_now) if len(fr_hist) > 1 else 0.0

        # -- Liquidation fields --
        now_wall = _now_utc().timestamp()
        liq_1m = [(long, short) for ts, long, short in self._liq_windows.get(symbol, deque())
                  if now_wall - ts <= 60]
        liq_5m = [(long, short) for ts, long, short in self._liq_windows.get(symbol, deque())
                  if now_wall - ts <= 300]
        snap.liq_long_1m_usd = sum(l for l, s in liq_1m)
        snap.liq_short_1m_usd = sum(s for l, s in liq_1m)
        snap.liq_long_5m_usd = sum(l for l, s in liq_5m)
        snap.liq_short_5m_usd = sum(s for l, s in liq_5m)

        # -- Taker flow --
        taker_1m = [(bv, sv) for ts, bv, sv in self._taker_windows.get(symbol, deque())
                    if now_wall - ts <= 60]
        taker_5m = [(bv, sv) for ts, bv, sv in self._taker_windows.get(symbol, deque())
                    if now_wall - ts <= 300]
        buy_1m = sum(b for b, s in taker_1m)
        sell_1m = sum(s for b, s in taker_1m)
        buy_5m = sum(b for b, s in taker_5m)
        sell_5m = sum(s for b, s in taker_5m)
        snap.taker_buy_sell_ratio_1m = safe_div(buy_1m, sell_1m, default=1.0)
        snap.taker_buy_sell_ratio_5m = safe_div(buy_5m, sell_5m, default=1.0)

        # -- Book / spread --
        book = self._book.get(symbol)
        if book:
            bid, ask = book
            mid = (bid + ask) / 2.0
            snap.spread_bps = safe_div(ask - bid, mid, 0.0) * 10_000

        # -- Timing --
        if funding_ts is not None:
            secs = (funding_ts - _now_utc()).total_seconds()
            snap.seconds_to_funding = secs
            snap.minutes_to_funding = secs / 60.0

        # -- Derived fields --
        self._compute_derived_fields(snap)

        # -- Freshness check --
        self._check_freshness(snap, now_mono)

        self._snapshots[symbol] = snap
        return snap

    def _price_at_seconds_ago(self, ticks: List[_PriceTick], seconds: float) -> float:
        """Return the price closest to `seconds` ago from the most recent tick."""
        if not ticks:
            return 0.0
        now = ticks[-1].ts
        target = now - seconds
        best = ticks[0]
        for t in reversed(ticks):
            if t.ts <= target:
                best = t
                break
        return best.price

    def _oi_at_seconds_ago(self, oi_series: List[Tuple[float, float]], seconds: float) -> float:
        """Return OI closest to `seconds` ago."""
        if not oi_series:
            return 0.0
        now_ts = oi_series[-1][0]
        target = now_ts - seconds
        best = oi_series[0][1]
        for ts, oi in reversed(oi_series):
            if ts <= target:
                best = oi
                break
        return best

    def _compute_derived_fields(self, snap: FeatureSnapshot) -> None:
        """Compute delta and derived fields from raw values."""
        if snap.price_now > 0 and snap.price_1m_ago > 0:
            snap.price_delta_1m = (snap.price_now - snap.price_1m_ago) / snap.price_1m_ago
        if snap.price_now > 0 and snap.price_5m_ago > 0:
            snap.price_delta_5m = (snap.price_now - snap.price_5m_ago) / snap.price_5m_ago
        if snap.oi_now > 0 and snap.oi_5m_ago > 0:
            snap.oi_delta_5m = (snap.oi_now - snap.oi_5m_ago) / snap.oi_5m_ago
        if snap.oi_now > 0 and snap.oi_15m_ago > 0:
            snap.oi_delta_15m = (snap.oi_now - snap.oi_15m_ago) / snap.oi_15m_ago
        if snap.price_now > 0 and snap.low_15m > 0:
            snap.dist_to_15m_low = (snap.price_now - snap.low_15m) / snap.low_15m
        if snap.price_now > 0 and snap.high_5m > 0:
            snap.dist_to_5m_high = (snap.high_5m - snap.price_now) / snap.high_5m
        snap.liq_ratio_short_over_long_5m = safe_div(snap.liq_short_5m_usd, max(snap.liq_long_5m_usd, 1.0))

    def _check_freshness(self, snap: FeatureSnapshot, now_mono: float) -> None:
        """Mark snapshot stale if any critical feed is too old."""
        stale_cfg = self._cfg.get("data", {}).get("stale_feature_max_age_sec", {})
        oi_max = float(stale_cfg.get("oi", 20))
        liq_max = float(stale_cfg.get("liquidation", 15))
        taker_max = float(stale_cfg.get("taker", 15))
        funding_max = float(stale_cfg.get("funding", 30))
        symbol = snap.symbol
        if now_mono - self._last_oi_update.get(symbol, 0) > oi_max:
            snap.is_stale = True
        if now_mono - self._last_liq_update.get(symbol, 0) > liq_max:
            snap.is_stale = True
        if now_mono - self._last_taker_update.get(symbol, 0) > taker_max:
            snap.is_stale = True
        if now_mono - self._last_funding_update.get(symbol, 0) > funding_max:
            snap.is_stale = True
