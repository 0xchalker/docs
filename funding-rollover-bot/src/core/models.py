from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List
from enum import Enum


class DecisionType(Enum):
    LONG_AT_ROLLOVER = "LONG_AT_ROLLOVER"
    SHORT_AT_ROLLOVER = "SHORT_AT_ROLLOVER"
    NO_TRADE = "NO_TRADE"


class BotState(Enum):
    FLAT = "FLAT"
    WARMUP = "WARMUP"
    PRE_FUNDING_SCAN = "PRE_FUNDING_SCAN"
    ARMED = "ARMED"
    DECISION_LOCKED = "DECISION_LOCKED"
    ENTERING = "ENTERING"
    IN_POSITION = "IN_POSITION"
    EXITING = "EXITING"
    COOLDOWN = "COOLDOWN"


class SymbolGroup(Enum):
    BTC = "BTC"
    ETH = "ETH"
    ALT = "ALT"


@dataclass
class FundingEvent:
    symbol: str
    exchange: str
    funding_timestamp: datetime
    previous_funding_timestamp: Optional[datetime] = None


@dataclass
class FeatureSnapshot:
    symbol: str
    exchange: str
    timestamp: datetime
    # Funding features
    funding_now: float = 0.0
    funding_oi_weighted_now: float = 0.0
    funding_percentile_7d: float = 0.5
    funding_zscore_24h: float = 0.0
    # OI features
    oi_now: float = 0.0
    oi_5m_ago: float = 0.0
    oi_15m_ago: float = 0.0
    oi_delta_5m: float = 0.0
    oi_delta_15m: float = 0.0
    # Price features
    price_now: float = 0.0
    price_1m_ago: float = 0.0
    price_5m_ago: float = 0.0
    price_delta_1m: float = 0.0
    price_delta_5m: float = 0.0
    high_5m: float = 0.0
    low_5m: float = 0.0
    high_15m: float = 0.0
    low_15m: float = 0.0
    dist_to_15m_low: float = 0.0
    dist_to_5m_high: float = 0.0
    # Liquidation features
    liq_long_1m_usd: float = 0.0
    liq_short_1m_usd: float = 0.0
    liq_long_5m_usd: float = 0.0
    liq_short_5m_usd: float = 0.0
    liq_ratio_short_over_long_5m: float = 0.0
    # Taker flow features
    taker_buy_sell_ratio_1m: float = 1.0
    taker_buy_sell_ratio_5m: float = 1.0
    # Execution quality
    spread_bps: float = 0.0
    atr_1m_bps: float = 0.0
    slippage_estimate_bps: float = 0.0
    # Timing
    minutes_to_funding: float = 0.0
    seconds_to_funding: float = 0.0
    # Freshness
    is_stale: bool = False


@dataclass
class ScanCandidate:
    symbol: str
    exchange: str
    funding_pct: float
    funding_percentile_7d: float
    funding_zscore_24h: float
    oi_usd: float
    oi_delta_15m: float
    spread_bps: float
    slippage_estimate_bps: float
    coin_selection_score: int
    shortlisted_at: datetime
    symbol_group: str = "ALT"


@dataclass
class OrderResult:
    accepted: bool
    order_id: str = ""
    fill_price: float = 0.0
    fill_qty: float = 0.0
    slippage_bps: float = 0.0
    reject_reason: str = ""


@dataclass
class OpenPosition:
    symbol: str
    exchange: str
    side: str  # BUY or SELL
    entry_price: float
    size: float
    entry_time: datetime
    tp1_done: bool = False
    group: str = "ALT"
    mode: str = "LONG_AT_ROLLOVER"


@dataclass
class TradeJournal:
    trade_id: str
    symbol: str
    exchange: str
    funding_timestamp: datetime
    mode: str
    long_score: int
    short_score: int
    decision: str
    entry_time: datetime
    exit_time: datetime
    entry_price: float
    exit_price: float
    size: float
    pnl_usd: float
    pnl_r: float
    exit_reason: str
    funding_now: float
    funding_oi_weighted_now: float
    funding_percentile_7d: float
    funding_zscore_24h: float
    oi_delta_5m: float
    oi_delta_15m: float
    price_delta_1m: float
    price_delta_5m: float
    liq_long_5m_usd: float
    liq_short_5m_usd: float
    taker_buy_sell_ratio_1m: float
    taker_buy_sell_ratio_5m: float
    spread_bps: float
    atr_1m_bps: float
    slippage_realized_bps: float
