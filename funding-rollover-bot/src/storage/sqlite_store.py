"""
SQLite storage backend using aiosqlite.
Tables: trades, feature_snapshots, scan_candidates, daily_stats
"""
import json
from datetime import datetime, timezone, date
from typing import List, Optional, Any

import aiosqlite

from core.models import TradeJournal, FeatureSnapshot
from utils.logging_utils import get_logger

logger = get_logger(__name__)

_DB_PATH = "funding_bot.db"

_CREATE_TRADES = """
CREATE TABLE IF NOT EXISTS trades (
    trade_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    exchange TEXT NOT NULL,
    funding_timestamp TEXT,
    mode TEXT,
    long_score INTEGER,
    short_score INTEGER,
    decision TEXT,
    entry_time TEXT,
    exit_time TEXT,
    entry_price REAL,
    exit_price REAL,
    size REAL,
    pnl_usd REAL,
    pnl_r REAL,
    exit_reason TEXT,
    funding_now REAL,
    funding_oi_weighted_now REAL,
    funding_percentile_7d REAL,
    funding_zscore_24h REAL,
    oi_delta_5m REAL,
    oi_delta_15m REAL,
    price_delta_1m REAL,
    price_delta_5m REAL,
    liq_long_5m_usd REAL,
    liq_short_5m_usd REAL,
    taker_buy_sell_ratio_1m REAL,
    taker_buy_sell_ratio_5m REAL,
    spread_bps REAL,
    atr_1m_bps REAL,
    slippage_realized_bps REAL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
"""

_CREATE_FEATURE_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS feature_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    exchange TEXT NOT NULL,
    label TEXT,
    snapshot_timestamp TEXT,
    data_json TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
"""

_CREATE_SCAN_CANDIDATES = """
CREATE TABLE IF NOT EXISTS scan_candidates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    exchange TEXT,
    funding_pct REAL,
    funding_percentile_7d REAL,
    oi_usd REAL,
    oi_delta_15m REAL,
    coin_selection_score INTEGER,
    shortlisted_at TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
"""

_CREATE_DAILY_STATS = """
CREATE TABLE IF NOT EXISTS daily_stats (
    trade_date TEXT PRIMARY KEY,
    pnl_usd REAL,
    n_trades INTEGER,
    n_wins INTEGER,
    n_losses INTEGER
);
"""


class SQLiteStore:
    """Async SQLite persistence layer."""

    def __init__(self, db_path: str = _DB_PATH) -> None:
        self._db_path = db_path
        self._conn: Optional[aiosqlite.Connection] = None

    async def initialize(self) -> None:
        """Open connection and create tables."""
        self._conn = await aiosqlite.connect(self._db_path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL;")
        await self._conn.execute(_CREATE_TRADES)
        await self._conn.execute(_CREATE_FEATURE_SNAPSHOTS)
        await self._conn.execute(_CREATE_SCAN_CANDIDATES)
        await self._conn.execute(_CREATE_DAILY_STATS)
        await self._conn.commit()
        logger.info("SQLiteStore initialized", db_path=self._db_path)

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()

    async def _ensure_connected(self) -> None:
        if self._conn is None:
            await self.initialize()

    # -------------------------------------------------------------------------
    # Trades
    # -------------------------------------------------------------------------

    async def insert_trade(self, journal: TradeJournal) -> None:
        await self._ensure_connected()
        sql = """
        INSERT OR REPLACE INTO trades (
            trade_id, symbol, exchange, funding_timestamp, mode, long_score, short_score,
            decision, entry_time, exit_time, entry_price, exit_price, size,
            pnl_usd, pnl_r, exit_reason, funding_now, funding_oi_weighted_now,
            funding_percentile_7d, funding_zscore_24h, oi_delta_5m, oi_delta_15m,
            price_delta_1m, price_delta_5m, liq_long_5m_usd, liq_short_5m_usd,
            taker_buy_sell_ratio_1m, taker_buy_sell_ratio_5m, spread_bps,
            atr_1m_bps, slippage_realized_bps
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        await self._conn.execute(sql, (
            journal.trade_id, journal.symbol, journal.exchange,
            journal.funding_timestamp.isoformat() if journal.funding_timestamp else None,
            journal.mode, journal.long_score, journal.short_score,
            journal.decision,
            journal.entry_time.isoformat() if journal.entry_time else None,
            journal.exit_time.isoformat() if journal.exit_time else None,
            journal.entry_price, journal.exit_price, journal.size,
            journal.pnl_usd, journal.pnl_r, journal.exit_reason,
            journal.funding_now, journal.funding_oi_weighted_now,
            journal.funding_percentile_7d, journal.funding_zscore_24h,
            journal.oi_delta_5m, journal.oi_delta_15m,
            journal.price_delta_1m, journal.price_delta_5m,
            journal.liq_long_5m_usd, journal.liq_short_5m_usd,
            journal.taker_buy_sell_ratio_1m, journal.taker_buy_sell_ratio_5m,
            journal.spread_bps, journal.atr_1m_bps, journal.slippage_realized_bps,
        ))
        await self._conn.commit()

    async def query_trades(
        self,
        symbol: Optional[str],
        start: Optional[datetime],
        end: Optional[datetime],
    ) -> List[TradeJournal]:
        await self._ensure_connected()
        conditions = []
        params: List[Any] = []
        if symbol:
            conditions.append("symbol = ?")
            params.append(symbol)
        if start:
            conditions.append("entry_time >= ?")
            params.append(start.isoformat())
        if end:
            conditions.append("entry_time <= ?")
            params.append(end.isoformat())

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        sql = f"SELECT * FROM trades {where} ORDER BY entry_time DESC"

        async with self._conn.execute(sql, params) as cursor:
            rows = await cursor.fetchall()

        result = []
        for row in rows:
            result.append(_row_to_trade_journal(dict(row)))
        return result

    async def get_daily_pnl(self, trade_date: date) -> float:
        await self._ensure_connected()
        date_str = trade_date.isoformat()
        sql = "SELECT SUM(pnl_usd) as total FROM trades WHERE DATE(entry_time) = ?"
        async with self._conn.execute(sql, (date_str,)) as cursor:
            row = await cursor.fetchone()
        if row and row[0] is not None:
            return float(row[0])
        return 0.0

    # -------------------------------------------------------------------------
    # Feature snapshots
    # -------------------------------------------------------------------------

    async def insert_feature_snapshot(self, snapshot: FeatureSnapshot, label: str) -> None:
        await self._ensure_connected()
        import dataclasses
        data = dataclasses.asdict(snapshot)
        # Convert datetime objects to strings
        for k, v in data.items():
            if isinstance(v, datetime):
                data[k] = v.isoformat()
        sql = """
        INSERT INTO feature_snapshots (symbol, exchange, label, snapshot_timestamp, data_json)
        VALUES (?, ?, ?, ?, ?)
        """
        await self._conn.execute(sql, (
            snapshot.symbol, snapshot.exchange, label,
            snapshot.timestamp.isoformat(),
            json.dumps(data),
        ))
        await self._conn.commit()


def _row_to_trade_journal(row: dict) -> TradeJournal:
    """Convert a dict row from SQLite to a TradeJournal dataclass."""
    def _dt(s):
        if s is None:
            return datetime.now(tz=timezone.utc)
        try:
            return datetime.fromisoformat(s).replace(tzinfo=timezone.utc) if s else datetime.now(tz=timezone.utc)
        except ValueError:
            return datetime.now(tz=timezone.utc)

    return TradeJournal(
        trade_id=row.get("trade_id", ""),
        symbol=row.get("symbol", ""),
        exchange=row.get("exchange", ""),
        funding_timestamp=_dt(row.get("funding_timestamp")),
        mode=row.get("mode", ""),
        long_score=int(row.get("long_score", 0) or 0),
        short_score=int(row.get("short_score", 0) or 0),
        decision=row.get("decision", ""),
        entry_time=_dt(row.get("entry_time")),
        exit_time=_dt(row.get("exit_time")),
        entry_price=float(row.get("entry_price", 0) or 0),
        exit_price=float(row.get("exit_price", 0) or 0),
        size=float(row.get("size", 0) or 0),
        pnl_usd=float(row.get("pnl_usd", 0) or 0),
        pnl_r=float(row.get("pnl_r", 0) or 0),
        exit_reason=row.get("exit_reason", ""),
        funding_now=float(row.get("funding_now", 0) or 0),
        funding_oi_weighted_now=float(row.get("funding_oi_weighted_now", 0) or 0),
        funding_percentile_7d=float(row.get("funding_percentile_7d", 0) or 0),
        funding_zscore_24h=float(row.get("funding_zscore_24h", 0) or 0),
        oi_delta_5m=float(row.get("oi_delta_5m", 0) or 0),
        oi_delta_15m=float(row.get("oi_delta_15m", 0) or 0),
        price_delta_1m=float(row.get("price_delta_1m", 0) or 0),
        price_delta_5m=float(row.get("price_delta_5m", 0) or 0),
        liq_long_5m_usd=float(row.get("liq_long_5m_usd", 0) or 0),
        liq_short_5m_usd=float(row.get("liq_short_5m_usd", 0) or 0),
        taker_buy_sell_ratio_1m=float(row.get("taker_buy_sell_ratio_1m", 1) or 1),
        taker_buy_sell_ratio_5m=float(row.get("taker_buy_sell_ratio_5m", 1) or 1),
        spread_bps=float(row.get("spread_bps", 0) or 0),
        atr_1m_bps=float(row.get("atr_1m_bps", 0) or 0),
        slippage_realized_bps=float(row.get("slippage_realized_bps", 0) or 0),
    )
