"""
Risk engine — position sizing, drawdown guard, TP/SL management.
"""
import time
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

from core.models import OpenPosition
from utils.logging_utils import get_logger
from utils.math_utils import safe_div

logger = get_logger(__name__)


def _classify_symbol_group(symbol: str) -> str:
    s = symbol.upper()
    if "BTC" in s:
        return "BTC"
    if "ETH" in s:
        return "ETH"
    return "ALT"


class RiskEngine:
    """
    Handles position sizing, stop/TP management, and drawdown guards.
    """

    def __init__(self, config: Dict) -> None:
        self._cfg = config
        self._risk_cfg = config.get("risk", {})
        self._threshold_cfg = config.get("thresholds", {})

    # -------------------------------------------------------------------------
    # Position sizing
    # -------------------------------------------------------------------------

    def compute_size(
        self,
        symbol: str,
        stop_distance_pct: float,
        equity: float,
        risk_per_trade: Optional[float] = None,
        max_notional: Optional[float] = None,
    ) -> float:
        """
        Risk-based position sizing.

        size_usd = (equity * risk_per_trade) / stop_distance_pct
        Returns size in base units given mark price; caller must divide by price.
        Returns notional USD value.
        """
        if risk_per_trade is None:
            risk_per_trade = float(self._risk_cfg.get("risk_per_trade", 0.0025))
        risk_usd = equity * risk_per_trade
        if stop_distance_pct <= 0:
            logger.warning("Invalid stop_distance_pct", value=stop_distance_pct, symbol=symbol)
            return 0.0
        notional = risk_usd / stop_distance_pct
        if max_notional is not None:
            notional = min(notional, max_notional)
        logger.debug(
            "compute_size",
            symbol=symbol,
            equity=equity,
            risk_per_trade=risk_per_trade,
            stop_distance_pct=stop_distance_pct,
            notional=notional,
        )
        return notional

    # -------------------------------------------------------------------------
    # Open position management
    # -------------------------------------------------------------------------

    def manage_open_position(
        self,
        position: OpenPosition,
        current_price: float,
        now: datetime,
        cfg: Optional[Dict] = None,
    ) -> str:
        """
        Evaluate an open position and return "EXIT" or "HOLD".

        Checks (in order):
          1. Time stop — max hold seconds exceeded
          2. Stop loss
          3. TP1 (partial, sets tp1_done)
          4. TP2 (full exit)
        """
        cfg = cfg or self._cfg
        risk_cfg = cfg.get("risk", {})
        group = self.classify_symbol_group(position.symbol)

        # Time stop
        max_hold = self.get_max_hold_seconds(position.mode)
        elapsed = (now - position.entry_time).total_seconds()
        if elapsed >= max_hold:
            logger.info(
                "Time stop triggered",
                symbol=position.symbol,
                elapsed=elapsed,
                max_hold=max_hold,
            )
            return "EXIT"

        stop_dist = self.get_stop_distance_pct(group)
        tp1_dist = self.get_tp1_pct(group)
        tp2_dist = self.get_tp2_pct(group)

        if position.side == "BUY":
            pnl_pct = safe_div(current_price - position.entry_price, position.entry_price)
            if pnl_pct <= -stop_dist:
                logger.info("Stop loss hit (LONG)", symbol=position.symbol, pnl_pct=pnl_pct)
                return "EXIT"
            if not position.tp1_done and pnl_pct >= tp1_dist:
                position.tp1_done = True
                return "TP1"
            if pnl_pct >= tp2_dist:
                logger.info("TP2 hit (LONG)", symbol=position.symbol, pnl_pct=pnl_pct)
                return "EXIT"
        else:  # SELL
            pnl_pct = safe_div(position.entry_price - current_price, position.entry_price)
            if pnl_pct <= -stop_dist:
                logger.info("Stop loss hit (SHORT)", symbol=position.symbol, pnl_pct=pnl_pct)
                return "EXIT"
            if not position.tp1_done and pnl_pct >= tp1_dist:
                position.tp1_done = True
                return "TP1"
            if pnl_pct >= tp2_dist:
                logger.info("TP2 hit (SHORT)", symbol=position.symbol, pnl_pct=pnl_pct)
                return "EXIT"

        return "HOLD"

    # -------------------------------------------------------------------------
    # Drawdown guard
    # -------------------------------------------------------------------------

    def check_drawdown_guard(self, daily_stats: Dict) -> bool:
        """
        Return True (trading should be DISABLED) if:
          - daily loss exceeds daily_max_loss_pct
          - consecutive losses >= max_consecutive_losses
        """
        max_daily_loss = float(self._risk_cfg.get("daily_max_loss_pct", 0.015))
        max_consec = int(self._risk_cfg.get("max_consecutive_losses", 2))

        equity = float(daily_stats.get("starting_equity", 1.0))
        daily_pnl = float(daily_stats.get("pnl_usd", 0.0))
        daily_pnl_pct = safe_div(daily_pnl, equity)
        consec_losses = int(daily_stats.get("consecutive_losses", 0))

        if daily_pnl_pct <= -max_daily_loss:
            logger.warning(
                "Daily loss limit hit",
                daily_pnl_pct=daily_pnl_pct,
                limit=max_daily_loss,
            )
            return True
        if consec_losses >= max_consec:
            logger.warning(
                "Consecutive loss limit hit",
                consecutive_losses=consec_losses,
                limit=max_consec,
            )
            return True
        return False

    def market_regime_broke(self, symbol: str, features: Optional[Dict] = None) -> bool:
        """
        Detect a sudden regime break.
        Returns True if ATR spike or price gap suggests market structure broke.
        """
        if features is None:
            return False
        atr_bps = float(features.get("atr_1m_bps", 0))
        group = self.classify_symbol_group(symbol)
        max_atr = float(self._cfg.get("filters", {}).get("max_atr_1m_bps", {}).get(group, 35.0))
        if atr_bps > max_atr * 2.5:
            logger.warning("Regime break detected via ATR spike", symbol=symbol, atr_bps=atr_bps)
            return True
        return False

    # -------------------------------------------------------------------------
    # Config helpers
    # -------------------------------------------------------------------------

    def classify_symbol_group(self, symbol: str) -> str:
        return _classify_symbol_group(symbol)

    def get_stop_distance_pct(self, symbol_group: str) -> float:
        return float(self._risk_cfg.get("stop_distance_pct", {}).get(symbol_group, 0.002))

    def get_tp1_pct(self, symbol_group: str) -> float:
        return float(self._risk_cfg.get("tp1_pct", {}).get(symbol_group, 0.0024))

    def get_tp2_pct(self, symbol_group: str) -> float:
        return float(self._risk_cfg.get("tp2_pct", {}).get(symbol_group, 0.004))

    def get_max_hold_seconds(self, mode: str) -> float:
        return float(self._risk_cfg.get("max_hold_seconds", {}).get(mode, 45))
