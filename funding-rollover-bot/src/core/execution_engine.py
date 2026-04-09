"""
Execution engine — handles order entry, exit, and partial close logic.
Enforces slippage guards and integrates with venue REST adapter.
"""
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from core.models import DecisionType, FeatureSnapshot, FundingEvent, OpenPosition, OrderResult
from utils.logging_utils import get_logger
from utils.math_utils import safe_div

logger = get_logger(__name__)


def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


class ExecutionEngine:
    """
    Wraps venue REST calls with pre-trade guards and logging.
    """

    def __init__(
        self,
        venue_client: Any,
        risk_engine: Any,
        config: Dict,
        dry_run: bool = True,
    ) -> None:
        self._venue = venue_client
        self._risk = risk_engine
        self._cfg = config
        self._dry_run = dry_run
        # symbol -> OpenPosition
        self._positions: Dict[str, OpenPosition] = {}

    # -------------------------------------------------------------------------
    # Entry
    # -------------------------------------------------------------------------

    async def enter_at_rollover(
        self,
        event: FundingEvent,
        decision: DecisionType,
        feature_snapshot: FeatureSnapshot,
    ) -> OrderResult:
        """
        Place an entry order at the funding rollover moment.
        Enforces slippage guard and entry window guard.
        """
        symbol = event.symbol
        exchange = event.exchange

        if decision == DecisionType.NO_TRADE:
            return OrderResult(accepted=False, reject_reason="NO_TRADE decision")

        side = "BUY" if decision == DecisionType.LONG_AT_ROLLOVER else "SELL"
        group = self._risk.classify_symbol_group(symbol)

        # -- Slippage guard --
        max_slip = float(self._cfg.get("execution", {}).get("max_entry_slippage_bps", {}).get(group, 10))
        if feature_snapshot.spread_bps > max_slip:
            reason = f"spread {feature_snapshot.spread_bps:.1f} bps > max {max_slip} bps"
            logger.warning("Slippage guard blocked entry", symbol=symbol, reason=reason)
            return OrderResult(accepted=False, reject_reason=reason)

        # -- Spread guard --
        max_spread = float(self._cfg.get("filters", {}).get("max_spread_bps", {}).get(group, 6.0))
        if feature_snapshot.spread_bps > max_spread:
            reason = f"spread {feature_snapshot.spread_bps:.1f} bps exceeds filter {max_spread}"
            logger.warning("Spread filter blocked entry", symbol=symbol, reason=reason)
            return OrderResult(accepted=False, reject_reason=reason)

        # -- Compute size --
        equity = await self._venue.get_account_equity()
        stop_dist = self._risk.get_stop_distance_pct(group)
        notional = self._risk.compute_size(symbol, stop_dist, equity)
        mark_price = feature_snapshot.price_now
        if mark_price <= 0:
            try:
                mark_price = await self._venue.get_mark_price(symbol)
            except Exception:
                return OrderResult(accepted=False, reject_reason="cannot get mark price")

        qty = round(safe_div(notional, mark_price), 6)
        if qty <= 0:
            return OrderResult(accepted=False, reject_reason="computed qty is zero")

        # -- Get marketable limit price --
        try:
            limit_price = await self._venue.get_best_marketable_limit_price(symbol, side, max_slip)
        except Exception as exc:
            return OrderResult(accepted=False, reject_reason=f"price fetch error: {exc}")

        # -- Send order --
        logger.info(
            "Sending entry order",
            symbol=symbol,
            side=side,
            qty=qty,
            price=limit_price,
            dry_run=self._dry_run,
            decision=decision.value,
        )
        result = await self._venue.place_order(
            symbol=symbol,
            side=side,
            quantity=qty,
            price=limit_price,
            order_type="LIMIT",
            tif="IOC",
        )

        if result.accepted:
            pos = OpenPosition(
                symbol=symbol,
                exchange=exchange,
                side=side,
                entry_price=result.fill_price,
                size=result.fill_qty,
                entry_time=_now_utc(),
                group=group,
                mode=decision.value,
            )
            self._positions[symbol] = pos
            logger.info(
                "Entry filled",
                symbol=symbol,
                side=side,
                fill_price=result.fill_price,
                fill_qty=result.fill_qty,
                slippage_bps=result.slippage_bps,
            )
        else:
            logger.warning("Entry rejected", symbol=symbol, reason=result.reject_reason)

        return result

    # -------------------------------------------------------------------------
    # Exit
    # -------------------------------------------------------------------------

    async def exit_position(self, symbol: str, reason: str) -> OrderResult:
        """Full exit of open position."""
        pos = self._positions.get(symbol)
        if pos is None:
            return OrderResult(accepted=False, reject_reason="no tracked position")
        logger.info("Exiting position", symbol=symbol, reason=reason)
        result = await self._venue.close_position(symbol)
        if result.accepted:
            del self._positions[symbol]
            logger.info(
                "Exit filled",
                symbol=symbol,
                fill_price=result.fill_price,
                reason=reason,
            )
        else:
            logger.warning("Exit failed", symbol=symbol, reason=result.reject_reason)
        return result

    async def partial_close(self, symbol: str, pct: float, reason: str) -> OrderResult:
        """Close `pct` fraction of position (e.g., 0.5 for TP1)."""
        pos = self._positions.get(symbol)
        if pos is None:
            return OrderResult(accepted=False, reject_reason="no tracked position")
        logger.info("Partial close", symbol=symbol, pct=pct, reason=reason)
        result = await self._venue.partial_close(symbol, pct=pct)
        if result.accepted:
            # Update tracked size
            pos.size = pos.size * (1 - pct)
            if pos.size <= 0:
                del self._positions[symbol]
        return result

    def get_open_position(self, symbol: str) -> Optional[OpenPosition]:
        return self._positions.get(symbol)

    def has_open_position(self, symbol: str) -> bool:
        return symbol in self._positions
