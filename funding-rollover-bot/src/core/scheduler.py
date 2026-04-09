"""
Main event loop scheduler.
Orchestrates the scanner, state machine, feature engine, signal engine, and execution engine.
"""
import asyncio
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import yaml

from core.models import (
    BotState, DecisionType, FundingEvent, FeatureSnapshot, ScanCandidate,
)
from core.state_machine import StateMachine
from core.signal_engine import lock_decision, compute_long_score, compute_short_score
from core.risk_engine import RiskEngine
from core.execution_engine import ExecutionEngine
from core.feature_engine import FeatureEngine, normalize_funding_to_pct
from core.journal import JournalWriter
from adapters.coinglass_rest import CoinGlassRestClient
from adapters.venue_rest import VenueRestClient
from adapters.venue_ws import VenueWebSocket
from storage.sqlite_store import SQLiteStore
from utils.time_sync import get_utc_now, seconds_until, is_within_window
from utils.math_utils import compute_percentile, compute_zscore, safe_div
from utils.logging_utils import get_logger

logger = get_logger(__name__)

# Scanner timing offsets before funding event (seconds)
_SCAN_OFFSETS = [30 * 60, 15 * 60, 5 * 60, 60, 10]
_DECISION_LOCK_OFFSET = 3   # seconds before funding
_ENTRY_OFFSET = 0           # seconds: at funding
_ENTRY_WINDOW = 1           # seconds after funding still valid
_COOLDOWN_SECONDS = 60


class Scheduler:
    """
    Drives the entire bot lifecycle.
    """

    def __init__(self, config_path: str = "config/strategy.yaml", dry_run: bool = True) -> None:
        with open(config_path) as f:
            self._cfg = yaml.safe_load(f)

        # Override dry_run from CLI flag
        if dry_run:
            self._cfg.setdefault("strategy", {})["dry_run"] = True

        self._dry_run = self._cfg.get("strategy", {}).get("dry_run", True)

        cg_cfg = self._cfg.get("coinglass", {})
        venue_cfg = self._cfg.get("venue", {})

        self._cg = CoinGlassRestClient(
            api_key=cg_cfg.get("api_key", ""),
            base_url=cg_cfg.get("base_url", "https://open-api-v3.coinglass.com"),
        )
        self._venue_rest = VenueRestClient(
            api_key=venue_cfg.get("api_key", ""),
            api_secret=venue_cfg.get("api_secret", ""),
            testnet=venue_cfg.get("testnet", True),
            dry_run=self._dry_run,
        )
        self._venue_ws = VenueWebSocket(
            api_key=venue_cfg.get("api_key", ""),
            testnet=venue_cfg.get("testnet", True),
        )

        self._feature_engine = FeatureEngine(self._cg, self._venue_ws, self._cfg)
        self._risk_engine = RiskEngine(self._cfg)
        self._execution_engine = ExecutionEngine(
            self._venue_rest, self._risk_engine, self._cfg, dry_run=self._dry_run
        )
        self._state_machine = StateMachine()
        self._store = SQLiteStore()
        self._journal = JournalWriter(self._store)

        # symbol -> FundingEvent (next upcoming)
        self._upcoming_events: Dict[str, FundingEvent] = {}
        # symbol -> ScanCandidate
        self._armed_candidates: Dict[str, ScanCandidate] = {}
        # symbol -> (DecisionType, long_score, short_score)
        self._locked_decisions: Dict[str, Any] = {}
        # symbol -> entry OrderResult
        self._entry_results: Dict[str, Any] = {}
        # symbol -> FeatureSnapshot at decision lock
        self._locked_snapshots: Dict[str, FeatureSnapshot] = {}
        # Daily stats tracking
        self._daily_stats: Dict[str, Any] = {
            "pnl_usd": 0.0,
            "starting_equity": 10_000.0,
            "consecutive_losses": 0,
            "n_trades": 0,
        }

        # Whitelist of symbols to trade
        self._whitelist = set(self._cfg.get("strategy", {}).get("whitelist_symbols", []))

    # -------------------------------------------------------------------------
    # Main loop
    # -------------------------------------------------------------------------

    async def run_forever(self) -> None:
        """Main entry point. Starts WS connections and runs tick loop."""
        logger.info("Scheduler starting", dry_run=self._dry_run)
        await self._store.initialize()
        await self._venue_ws.connect()

        # Subscribe WS feeds for all whitelisted symbols
        for symbol in self._whitelist:
            await self._venue_ws.subscribe_book_ticker(
                symbol, lambda data, s=symbol: self._feature_engine.on_book_ticker(s, data)
            )
            await self._venue_ws.subscribe_mark_price(
                symbol, lambda data, s=symbol: self._feature_engine.on_mark_price(s, data)
            )

        # Pre-fetch initial data
        equity = await self._venue_rest.get_account_equity()
        self._daily_stats["starting_equity"] = equity
        logger.info("Account equity", equity=equity)

        try:
            while True:
                now = get_utc_now()
                await self.on_tick(now)
                await asyncio.sleep(0.5)  # 500ms tick
        except KeyboardInterrupt:
            logger.info("Scheduler stopping")
        finally:
            await self._cg.close()
            await self._venue_rest.close()
            await self._venue_ws.disconnect()

    async def on_tick(self, now: datetime) -> None:
        """Process all active symbols on each tick."""
        # Refresh upcoming funding events periodically
        await self._refresh_upcoming_events(now)

        # Drawdown guard
        if self._risk_engine.check_drawdown_guard(self._daily_stats):
            logger.warning("Drawdown guard active — skipping tick")
            return

        for symbol, event in list(self._upcoming_events.items()):
            if symbol not in self._whitelist:
                continue
            secs = seconds_until(now, event.funding_timestamp)
            state = self._state_machine.get_state(symbol)
            await self._process_symbol(symbol, event, secs, state, now)

    async def _process_symbol(
        self,
        symbol: str,
        event: FundingEvent,
        secs_to_funding: float,
        state: BotState,
        now: datetime,
    ) -> None:
        """Drive the state machine for a single symbol."""

        if state == BotState.FLAT:
            # Begin warmup at T-35m
            if secs_to_funding <= 35 * 60:
                try:
                    self._state_machine.transition(symbol, BotState.WARMUP)
                    await self._feature_engine.prefetch(event)
                    self._state_machine.transition(symbol, BotState.PRE_FUNDING_SCAN)
                except Exception as exc:
                    logger.error("Warmup failed", symbol=symbol, error=str(exc))
                    self._state_machine.force_state(symbol, BotState.FLAT)

        elif state == BotState.PRE_FUNDING_SCAN:
            # Periodic refresh at scan intervals
            if any(abs(secs_to_funding - offset) < 2 for offset in [15 * 60, 5 * 60, 60, 10]):
                await self._feature_engine.refresh_context(event)

            # Run scanner at T-5m to select candidates
            if is_within_window(now, event.funding_timestamp, before_sec=5 * 60, after_sec=-4 * 60):
                candidates = await self.scan_negative_funding_candidates(now)
                if any(c.symbol == symbol for c in candidates):
                    try:
                        self._state_machine.transition(symbol, BotState.ARMED)
                        self._armed_candidates[symbol] = next(c for c in candidates if c.symbol == symbol)
                    except ValueError:
                        pass

        elif state == BotState.ARMED:
            # Fast refresh at T-60s and T-10s
            if abs(secs_to_funding - 60) < 2 or abs(secs_to_funding - 10) < 2:
                await self._feature_engine.refresh_fast(event)

            # Lock decision at T-3s
            if is_within_window(now, event.funding_timestamp, before_sec=4, after_sec=-1):
                snap = self._feature_engine.get_snapshot(symbol, event.exchange, event.funding_timestamp)
                decision, ls, ss = lock_decision(snap, self._cfg)
                self._locked_decisions[symbol] = (decision, ls, ss)
                self._locked_snapshots[symbol] = snap
                try:
                    self._state_machine.transition(symbol, BotState.DECISION_LOCKED)
                except ValueError:
                    pass

        elif state == BotState.DECISION_LOCKED:
            decision, ls, ss = self._locked_decisions.get(symbol, (DecisionType.NO_TRADE, 0, 0))
            if decision == DecisionType.NO_TRADE:
                logger.info("NO_TRADE decision — going to cooldown", symbol=symbol)
                self._state_machine.transition(symbol, BotState.COOLDOWN)
                return

            # Enter at T+0 to T+1s
            if is_within_window(now, event.funding_timestamp, before_sec=0.1, after_sec=1.0):
                snap = self._locked_snapshots.get(symbol)
                if snap is None:
                    self._state_machine.transition(symbol, BotState.COOLDOWN)
                    return
                try:
                    self._state_machine.transition(symbol, BotState.ENTERING)
                except ValueError:
                    pass
                result = await self._execution_engine.enter_at_rollover(event, decision, snap)
                self._entry_results[symbol] = result
                if result.accepted:
                    self._state_machine.transition(symbol, BotState.IN_POSITION)
                else:
                    self._state_machine.transition(symbol, BotState.COOLDOWN)

        elif state == BotState.IN_POSITION:
            await self._manage_position(symbol, event, now)

        elif state == BotState.EXITING:
            # Confirm position cleared (handled in _manage_position)
            if not self._execution_engine.has_open_position(symbol):
                self._state_machine.transition(symbol, BotState.COOLDOWN)

        elif state == BotState.COOLDOWN:
            # After cooldown period, reset to FLAT
            # Track cooldown entry time via event metadata
            elapsed = (now - event.funding_timestamp).total_seconds()
            if elapsed >= _COOLDOWN_SECONDS or elapsed < 0:
                self._state_machine.transition(symbol, BotState.FLAT)
                self._cleanup_symbol(symbol)

    async def _manage_position(self, symbol: str, event: FundingEvent, now: datetime) -> None:
        """Check TP/SL/time stop for open position."""
        pos = self._execution_engine.get_open_position(symbol)
        if pos is None:
            self._state_machine.transition(symbol, BotState.EXITING)
            return

        # Get current price
        try:
            current_price = await self._venue_rest.get_mark_price(symbol)
        except Exception:
            return

        action = self._risk_engine.manage_open_position(pos, current_price, now)

        if action == "TP1":
            result = await self._execution_engine.partial_close(symbol, 0.5, "TP1")
            if result.accepted:
                await self._venue_rest.move_stop_to_break_even(symbol)
        elif action == "EXIT":
            try:
                self._state_machine.transition(symbol, BotState.EXITING)
            except ValueError:
                pass
            result = await self._execution_engine.exit_position(symbol, "managed_exit")
            if result.accepted:
                await self._record_trade(symbol, event, result, now)
                self._state_machine.transition(symbol, BotState.COOLDOWN)

    async def _record_trade(
        self,
        symbol: str,
        event: FundingEvent,
        exit_result: Any,
        exit_time: datetime,
    ) -> None:
        """Record completed trade to journal."""
        snap = self._locked_snapshots.get(symbol)
        entry_result = self._entry_results.get(symbol)
        if snap is None or entry_result is None:
            return

        decision, ls, ss = self._locked_decisions.get(symbol, (DecisionType.NO_TRADE, 0, 0))
        pos = None  # already closed

        entry_price = entry_result.fill_price
        exit_price = exit_result.fill_price
        size = entry_result.fill_qty
        side_mult = 1 if decision == DecisionType.LONG_AT_ROLLOVER else -1
        pnl_usd = (exit_price - entry_price) * size * side_mult
        risk_usd = self._daily_stats.get("starting_equity", 10_000) * float(
            self._cfg.get("risk", {}).get("risk_per_trade", 0.0025)
        )
        pnl_r = safe_div(pnl_usd, risk_usd)

        from core.models import TradeJournal
        import uuid as _uuid
        journal_entry = TradeJournal(
            trade_id=str(_uuid.uuid4()),
            symbol=symbol,
            exchange=event.exchange,
            funding_timestamp=event.funding_timestamp,
            mode=decision.value,
            long_score=ls,
            short_score=ss,
            decision=decision.value,
            entry_time=entry_result.order_id and exit_time or exit_time,
            exit_time=exit_time,
            entry_price=entry_price,
            exit_price=exit_price,
            size=size,
            pnl_usd=pnl_usd,
            pnl_r=pnl_r,
            exit_reason="managed_exit",
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
            slippage_realized_bps=entry_result.slippage_bps,
        )
        await self._journal.record_trade(journal_entry)
        self._daily_stats["pnl_usd"] = self._daily_stats.get("pnl_usd", 0.0) + pnl_usd
        self._daily_stats["n_trades"] = self._daily_stats.get("n_trades", 0) + 1
        if pnl_usd < 0:
            self._daily_stats["consecutive_losses"] = self._daily_stats.get("consecutive_losses", 0) + 1
        else:
            self._daily_stats["consecutive_losses"] = 0

    def _cleanup_symbol(self, symbol: str) -> None:
        self._upcoming_events.pop(symbol, None)
        self._armed_candidates.pop(symbol, None)
        self._locked_decisions.pop(symbol, None)
        self._locked_snapshots.pop(symbol, None)
        self._entry_results.pop(symbol, None)

    # -------------------------------------------------------------------------
    # Funding event refresh
    # -------------------------------------------------------------------------

    async def _refresh_upcoming_events(self, now: datetime) -> None:
        """Poll for next funding events. Binance pays every 8h."""
        for symbol in self._whitelist:
            if symbol in self._upcoming_events:
                event = self._upcoming_events[symbol]
                # Discard if past + 2 minutes
                if (now - event.funding_timestamp).total_seconds() > 120:
                    del self._upcoming_events[symbol]
                else:
                    continue
            try:
                fr = await self._venue_rest.get_funding_rate(symbol)
                next_ts_ms = fr.get("next_funding_time", 0)
                if next_ts_ms > 0:
                    next_dt = datetime.fromtimestamp(next_ts_ms / 1000, tz=timezone.utc)
                    self._upcoming_events[symbol] = FundingEvent(
                        symbol=symbol,
                        exchange=self._cfg.get("venue", {}).get("name", "binance"),
                        funding_timestamp=next_dt,
                    )
            except Exception as exc:
                logger.warning("Failed to get next funding time", symbol=symbol, error=str(exc))

    # -------------------------------------------------------------------------
    # Scanner
    # -------------------------------------------------------------------------

    def normalize_funding_to_pct(self, raw: float) -> float:
        """Normalize raw funding rate to percent. See feature_engine for logic."""
        return normalize_funding_to_pct(raw)

    async def scan_negative_funding_candidates(self, now_utc: datetime) -> List[ScanCandidate]:
        """
        Scan all active symbols for negative funding conditions.
        Returns sorted list of ScanCandidates meeting filter criteria.
        """
        scanner_cfg = self._cfg.get("scanner", {})
        funding_min = float(scanner_cfg.get("scan_funding_pct_min", -1.5))
        funding_max = float(scanner_cfg.get("scan_funding_pct_max", 0.0))
        top_n = int(scanner_cfg.get("top_n_scan_candidates", 3))

        rows = await self._load_universe_snapshot(now_utc)
        candidates: List[ScanCandidate] = []

        for row in rows:
            symbol = row.get("symbol", "")
            if self._whitelist and symbol not in self._whitelist:
                continue

            raw_funding = float(row.get("fundingRate", row.get("funding_rate", 0)))
            funding_pct = self.normalize_funding_to_pct(raw_funding)

            if not (funding_min <= funding_pct <= funding_max):
                continue

            exchange = row.get("exchange", self._cfg.get("venue", {}).get("name", "binance"))
            group = self._classify_group(symbol)

            # OI filter
            oi_usd = float(row.get("openInterest", row.get("oi_usd", 0)))
            min_oi = float(scanner_cfg.get("min_oi_usd", {}).get(group, 15_000_000))
            if oi_usd < min_oi:
                continue

            # Spread filter
            spread_bps = float(row.get("spread_bps", 99))
            max_spread = float(scanner_cfg.get("max_spread_bps_scan", {}).get(group, 6.0))
            if spread_bps > max_spread:
                continue

            # Slippage estimate
            slip_bps = float(row.get("slippage_bps", spread_bps * 1.5))
            max_slip = float(scanner_cfg.get("max_slippage_bps_scan", {}).get(group, 10))
            if slip_bps > max_slip:
                continue

            oi_delta_15m = float(row.get("oi_delta_15m", 0))
            funding_pct_7d = float(row.get("funding_percentile_7d", 0.5))
            funding_zscore = float(row.get("funding_zscore_24h", 0))

            score = self.compute_coin_selection_score({
                "funding_pct": funding_pct,
                "funding_percentile_7d": funding_pct_7d,
                "funding_zscore_24h": funding_zscore,
                "oi_usd": oi_usd,
                "oi_delta_15m": oi_delta_15m,
                "spread_bps": spread_bps,
                "group": group,
            })

            candidates.append(ScanCandidate(
                symbol=symbol,
                exchange=exchange,
                funding_pct=funding_pct,
                funding_percentile_7d=funding_pct_7d,
                funding_zscore_24h=funding_zscore,
                oi_usd=oi_usd,
                oi_delta_15m=oi_delta_15m,
                spread_bps=spread_bps,
                slippage_estimate_bps=slip_bps,
                coin_selection_score=score,
                shortlisted_at=now_utc,
                symbol_group=group,
            ))

        # Sort by score descending, then by most negative funding
        candidates.sort(key=lambda c: (-c.coin_selection_score, c.funding_pct))
        result = candidates[:top_n]
        logger.info("Scan results", n_candidates=len(result), symbols=[c.symbol for c in result])
        return result

    def compute_coin_selection_score(self, row: Dict) -> int:
        """
        Compute a coin selection priority score (higher = better candidate).
        Factors: funding extremity, OI delta, spread quality, z-score.
        """
        score = 0
        funding_pct = float(row.get("funding_pct", 0))
        funding_pct_7d = float(row.get("funding_percentile_7d", 0.5))
        funding_zscore = float(row.get("funding_zscore_24h", 0))
        oi_usd = float(row.get("oi_usd", 0))
        oi_delta_15m = float(row.get("oi_delta_15m", 0))
        spread_bps = float(row.get("spread_bps", 99))
        group = row.get("group", "ALT")

        # Funding extremity
        if funding_pct <= -0.5:
            score += 3
        elif funding_pct <= -0.2:
            score += 2
        elif funding_pct < 0:
            score += 1

        # Low funding percentile (historically rare)
        if funding_pct_7d <= 0.05:
            score += 3
        elif funding_pct_7d <= 0.10:
            score += 2
        elif funding_pct_7d <= 0.20:
            score += 1

        # Funding z-score extreme
        if funding_zscore <= -2.5:
            score += 2
        elif funding_zscore <= -1.8:
            score += 1

        # OI growing (crowded short building)
        if oi_delta_15m >= 0.02:
            score += 2
        elif oi_delta_15m >= 0.01:
            score += 1

        # Tight spread bonus
        max_spread = {"BTC": 2.5, "ETH": 3.5}.get(group, 6.0)
        if spread_bps <= max_spread * 0.5:
            score += 1

        return score

    async def _load_universe_snapshot(self, now_utc: datetime) -> List[Dict]:
        """Load current funding/OI snapshot from CoinGlass for all whitelisted symbols."""
        results = []
        for symbol in self._whitelist:
            cg_symbol = symbol.replace("USDT", "")
            try:
                fr_data = await self._cg.get_funding_rate_current(cg_symbol)
                if isinstance(fr_data, list):
                    for row in fr_data:
                        row["symbol"] = symbol
                        results.append(row)
                elif isinstance(fr_data, dict):
                    fr_data["symbol"] = symbol
                    results.append(fr_data)
            except Exception as exc:
                logger.warning("Universe snapshot fetch failed", symbol=symbol, error=str(exc))
                # Add a stub row so the symbol is still considered
                results.append({"symbol": symbol, "fundingRate": 0.0})
        return results

    def _classify_group(self, symbol: str) -> str:
        s = symbol.upper()
        if "BTC" in s:
            return "BTC"
        if "ETH" in s:
            return "ETH"
        return "ALT"
