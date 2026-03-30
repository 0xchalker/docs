"""
Trade journal writer — persists completed trades and feature snapshots.
"""
from datetime import datetime, date, timezone
from typing import Any, Dict, List, Optional

from core.models import FeatureSnapshot, TradeJournal
from utils.logging_utils import get_logger

logger = get_logger(__name__)


class JournalWriter:
    """
    Writes trade records and feature snapshots to SQLite storage.
    """

    def __init__(self, store: Any) -> None:
        self._store = store

    async def record_trade(self, entry: TradeJournal) -> None:
        """Persist a completed trade to storage."""
        logger.info(
            "Recording trade",
            trade_id=entry.trade_id,
            symbol=entry.symbol,
            mode=entry.mode,
            pnl_usd=entry.pnl_usd,
            pnl_r=entry.pnl_r,
            exit_reason=entry.exit_reason,
        )
        await self._store.insert_trade(entry)

    async def record_feature_snapshot(
        self,
        snapshot: FeatureSnapshot,
        label: str,
    ) -> None:
        """Persist a FeatureSnapshot with a descriptive label (e.g., 'T-10s', 'entry')."""
        logger.debug(
            "Recording feature snapshot",
            symbol=snapshot.symbol,
            label=label,
            timestamp=snapshot.timestamp.isoformat(),
        )
        await self._store.insert_feature_snapshot(snapshot, label)

    async def get_daily_stats(self, trade_date: date) -> Dict:
        """Return aggregated daily stats: pnl, n_trades, n_wins, n_losses."""
        trades = await self._store.query_trades(
            symbol=None,
            start=datetime(trade_date.year, trade_date.month, trade_date.day, tzinfo=timezone.utc),
            end=datetime(trade_date.year, trade_date.month, trade_date.day, 23, 59, 59, tzinfo=timezone.utc),
        )
        pnl = sum(t.pnl_usd for t in trades)
        n_wins = sum(1 for t in trades if t.pnl_usd > 0)
        n_losses = sum(1 for t in trades if t.pnl_usd <= 0)
        return {
            "date": trade_date.isoformat(),
            "pnl_usd": pnl,
            "n_trades": len(trades),
            "n_wins": n_wins,
            "n_losses": n_losses,
            "win_rate": n_wins / max(len(trades), 1),
        }

    async def get_recent_trades(self, n: int = 20) -> List[TradeJournal]:
        """Return the N most recent trades from storage."""
        trades = await self._store.query_trades(symbol=None, start=None, end=None)
        return list(reversed(trades))[:n]
