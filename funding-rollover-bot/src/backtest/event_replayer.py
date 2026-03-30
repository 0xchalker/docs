"""
Event-driven backtester.
Replays historical funding events, simulates entries/exits with slippage,
and records results in TradeJournal.
"""
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml

from backtest.slippage_model import SlippageModel
from backtest.metrics import BacktestMetrics, BacktestResult
from core.models import (
    DecisionType, FeatureSnapshot, FundingEvent, OpenPosition, TradeJournal,
)
from core.signal_engine import lock_decision
from core.risk_engine import RiskEngine
from utils.logging_utils import get_logger
from utils.math_utils import safe_div

logger = get_logger(__name__)

# Funding intervals: Binance pays every 8 hours at 00:00, 08:00, 16:00 UTC
_FUNDING_HOURS = [0, 8, 16]


def _generate_funding_events(
    symbols: List[str],
    start_date: str,
    end_date: str,
    exchange: str = "binance",
) -> List[FundingEvent]:
    """Generate synthetic funding events every 8h for given date range."""
    start_dt = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
    end_dt = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)
    events = []
    cur = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    while cur <= end_dt:
        for h in _FUNDING_HOURS:
            ts = cur.replace(hour=h)
            if start_dt <= ts <= end_dt:
                for symbol in symbols:
                    events.append(FundingEvent(
                        symbol=symbol,
                        exchange=exchange,
                        funding_timestamp=ts,
                    ))
        cur += timedelta(days=1)
    events.sort(key=lambda e: e.funding_timestamp)
    return events


def _build_synthetic_snapshot(
    event: FundingEvent,
    offset_seconds: float,
    seed_funding: float = -0.8,
) -> FeatureSnapshot:
    """
    Build a synthetic FeatureSnapshot for backtesting.
    In production this would load from parquet/SQLite history.
    We vary features deterministically around the event.
    """
    import math
    t = offset_seconds  # seconds relative to funding
    symbol = event.symbol
    base_price = 50_000.0 if "BTC" in symbol else 2_500.0

    # Simulate mild pre-funding price pump then dump
    price_now = base_price * (1 + 0.0005 * math.sin(t / 60.0))
    price_5m_ago = base_price * (1 - 0.0003)
    price_15m_ago = base_price * (1 - 0.0006)

    snap = FeatureSnapshot(
        symbol=symbol,
        exchange=event.exchange,
        timestamp=event.funding_timestamp + timedelta(seconds=offset_seconds),
        funding_now=seed_funding,
        funding_oi_weighted_now=seed_funding * 1.05,
        funding_percentile_7d=0.06,
        funding_zscore_24h=-2.1,
        oi_now=5e9,
        oi_5m_ago=5e9 * 0.982,
        oi_15m_ago=5e9 * 0.968,
        oi_delta_5m=0.018,
        oi_delta_15m=0.033,
        price_now=price_now,
        price_1m_ago=price_now * 0.9998,
        price_5m_ago=price_5m_ago,
        price_delta_1m=0.0002,
        price_delta_5m=(price_now - price_5m_ago) / price_5m_ago,
        high_5m=price_now * 1.001,
        low_5m=price_now * 0.998,
        high_15m=price_now * 1.0015,
        low_15m=price_now * 0.996,
        dist_to_15m_low=0.004,
        dist_to_5m_high=0.001,
        liq_long_1m_usd=50_000,
        liq_short_1m_usd=180_000,
        liq_long_5m_usd=200_000,
        liq_short_5m_usd=650_000,
        liq_ratio_short_over_long_5m=3.25,
        taker_buy_sell_ratio_1m=1.05,
        taker_buy_sell_ratio_5m=1.03,
        spread_bps=1.2,
        atr_1m_bps=12.0,
        slippage_estimate_bps=2.0,
        minutes_to_funding=-offset_seconds / 60.0,
        seconds_to_funding=-offset_seconds,
        is_stale=False,
    )
    return snap


class EventDrivenBacktester:
    """
    Replays historical funding events and simulates trades.
    """

    def __init__(self, config_path: str = "config/strategy.yaml") -> None:
        with open(config_path) as f:
            self._cfg = yaml.safe_load(f)
        self._risk = RiskEngine(self._cfg)
        self._slippage = SlippageModel()
        self._equity = 10_000.0

    def run(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str,
    ) -> BacktestResult:
        """
        Run backtest over all funding events in date range.

        For each event:
          1. Build synthetic feature snapshots at T-15m → T+0
          2. Compute signal scores and lock decision
          3. Simulate entry at T+0 with slippage
          4. Apply TP/SL/time stop using 1s simulated price series
          5. Record in TradeJournal
        """
        logger.info("Starting backtest", symbols=symbols, start=start_date, end=end_date)
        events = _generate_funding_events(symbols, start_date, end_date)
        logger.info("Total funding events", count=len(events))

        trades: List[TradeJournal] = []

        for event in events:
            trade = self._simulate_event(event)
            if trade is not None:
                trades.append(trade)

        result = BacktestMetrics.compute(trades)

        # Segment by mode and symbol
        result.segments = {
            **BacktestMetrics.segment_by(trades, "mode"),
            **{f"sym_{k}": v for k, v in BacktestMetrics.segment_by(trades, "symbol").items()},
        }

        logger.info(
            "Backtest complete",
            n_trades=result.n_trades,
            win_rate=f"{result.win_rate*100:.1f}%",
            total_pnl=result.total_pnl_usd,
            profit_factor=result.profit_factor,
        )
        return result

    def _simulate_event(self, event: FundingEvent) -> Optional[TradeJournal]:
        """Simulate a single funding event. Returns TradeJournal or None if no trade."""
        # Build snapshot at T-3s (decision point)
        snap = _build_synthetic_snapshot(event, offset_seconds=-3.0)

        decision, long_score, short_score = lock_decision(snap, self._cfg)
        if decision == DecisionType.NO_TRADE:
            return None

        # Simulate entry at T+0
        group = self._risk.classify_symbol_group(event.symbol)
        stop_dist = self._risk.get_stop_distance_pct(group)
        notional = self._risk.compute_size(event.symbol, stop_dist, self._equity)

        entry_snap = _build_synthetic_snapshot(event, offset_seconds=0.5)
        mid_price = entry_snap.price_now
        if mid_price <= 0:
            return None

        side = "BUY" if decision == DecisionType.LONG_AT_ROLLOVER else "SELL"
        slip_bps = self._slippage.estimate_slippage_bps(
            event.symbol, side, notional, snap.spread_bps, snap.atr_1m_bps
        )
        entry_price = self._slippage.apply_slippage(mid_price, side, slip_bps)
        size = safe_div(notional, entry_price)

        pos = OpenPosition(
            symbol=event.symbol,
            exchange=event.exchange,
            side=side,
            entry_price=entry_price,
            size=size,
            entry_time=event.funding_timestamp,
            group=group,
            mode=decision.value,
        )

        # Simulate 1-second price series from T+0 to T+max_hold
        max_hold = self._risk.get_max_hold_seconds(decision.value)
        exit_price, exit_reason, exit_time_s = self._simulate_price_walk(
            pos, event, int(max_hold)
        )

        exit_time = event.funding_timestamp + timedelta(seconds=exit_time_s)
        # Apply exit slippage
        exit_slip_bps = self._slippage.estimate_slippage_bps(
            event.symbol, "SELL" if side == "BUY" else "BUY",
            size * exit_price, snap.spread_bps, snap.atr_1m_bps
        )
        exit_price_after_slip = self._slippage.apply_slippage(
            exit_price, "SELL" if side == "BUY" else "BUY", exit_slip_bps
        )

        side_mult = 1 if side == "BUY" else -1
        pnl_usd = (exit_price_after_slip - entry_price) * size * side_mult
        risk_usd = self._equity * float(self._cfg.get("risk", {}).get("risk_per_trade", 0.0025))
        pnl_r = safe_div(pnl_usd, max(risk_usd, 1e-9))

        journal = TradeJournal(
            trade_id=str(uuid.uuid4()),
            symbol=event.symbol,
            exchange=event.exchange,
            funding_timestamp=event.funding_timestamp,
            mode=decision.value,
            long_score=long_score,
            short_score=short_score,
            decision=decision.value,
            entry_time=event.funding_timestamp,
            exit_time=exit_time,
            entry_price=entry_price,
            exit_price=exit_price_after_slip,
            size=size,
            pnl_usd=pnl_usd,
            pnl_r=pnl_r,
            exit_reason=exit_reason,
            funding_now=snap.funding_now,
            funding_oi_weighted_now=snap.funding_oi_weighted_now,
            funding_percentile_7d=snap.funding_percentile_7d,
            funding_zscore_24h=snap.funding_zscore_24h,
            oi_delta_5m=snap.oi_delta_5m,
            oi_delta_15m=snap.oi_delta_15m,
            price_delta_1m=snap.price_delta_1m,
            price_delta_5m=snap.price_delta_5m,
            liq_long_5m_usd=snap.liq_long_5m_usd,
            liq_short_5m_usd=snap.liq_short_5m_usd,
            taker_buy_sell_ratio_1m=snap.taker_buy_sell_ratio_1m,
            taker_buy_sell_ratio_5m=snap.taker_buy_sell_ratio_5m,
            spread_bps=snap.spread_bps,
            atr_1m_bps=snap.atr_1m_bps,
            slippage_realized_bps=slip_bps + exit_slip_bps,
        )
        return journal

    def _simulate_price_walk(
        self,
        pos: OpenPosition,
        event: FundingEvent,
        max_hold_s: int,
    ) -> Tuple[float, str, float]:
        """
        Simulate second-by-second price action after entry.
        Returns (exit_price, exit_reason, seconds_held).
        """
        import math, random
        random.seed(int(event.funding_timestamp.timestamp()) + hash(pos.symbol))

        group = pos.group
        stop_dist = self._risk.get_stop_distance_pct(group)
        tp1_dist = self._risk.get_tp1_pct(group)
        tp2_dist = self._risk.get_tp2_pct(group)

        price = pos.entry_price
        # Simulate random walk with slight drift in favorable direction
        drift = 0.0002 if pos.mode == "LONG_AT_ROLLOVER" else -0.0002
        vol_per_sec = pos.entry_price * 0.0001  # 1 bps per second volatility

        for s in range(1, max_hold_s + 1):
            noise = random.gauss(drift, 1.0) * vol_per_sec
            price = price + noise

            pnl_pct = safe_div(price - pos.entry_price, pos.entry_price)
            if pos.side == "SELL":
                pnl_pct = -pnl_pct

            if pnl_pct <= -stop_dist:
                return price, "stop_loss", float(s)
            if pnl_pct >= tp2_dist:
                return price, "tp2", float(s)

        return price, "time_stop", float(max_hold_s)
