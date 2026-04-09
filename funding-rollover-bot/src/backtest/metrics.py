"""
Backtest metrics computation and reporting.
"""
import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from core.models import TradeJournal
from utils.math_utils import safe_div
from utils.logging_utils import get_logger

logger = get_logger(__name__)


@dataclass
class BacktestResult:
    n_trades: int = 0
    n_wins: int = 0
    n_losses: int = 0
    win_rate: float = 0.0
    total_pnl_usd: float = 0.0
    total_pnl_r: float = 0.0
    avg_pnl_usd: float = 0.0
    avg_r: float = 0.0
    expectancy: float = 0.0
    profit_factor: float = 0.0
    max_drawdown: float = 0.0
    max_drawdown_r: float = 0.0
    realized_slippage_avg_bps: float = 0.0
    sharpe_ratio: float = 0.0
    trades: List[TradeJournal] = field(default_factory=list)
    segments: Dict[str, "BacktestResult"] = field(default_factory=dict)


class BacktestMetrics:
    """Compute and report backtest performance metrics."""

    @staticmethod
    def compute(trades: List[TradeJournal]) -> BacktestResult:
        """Compute full metrics from a list of TradeJournal entries."""
        result = BacktestResult(trades=trades)

        if not trades:
            return result

        result.n_trades = len(trades)
        pnl_list = [t.pnl_usd for t in trades]
        r_list = [t.pnl_r for t in trades]

        result.n_wins = sum(1 for p in pnl_list if p > 0)
        result.n_losses = sum(1 for p in pnl_list if p <= 0)
        result.win_rate = safe_div(result.n_wins, result.n_trades)
        result.total_pnl_usd = sum(pnl_list)
        result.total_pnl_r = sum(r_list)
        result.avg_pnl_usd = safe_div(result.total_pnl_usd, result.n_trades)
        result.avg_r = safe_div(result.total_pnl_r, result.n_trades)

        gross_profit = sum(p for p in pnl_list if p > 0)
        gross_loss = abs(sum(p for p in pnl_list if p < 0))
        result.profit_factor = safe_div(gross_profit, max(gross_loss, 1e-9))

        avg_win = safe_div(gross_profit, max(result.n_wins, 1))
        avg_loss = safe_div(gross_loss, max(result.n_losses, 1))
        result.expectancy = (result.win_rate * avg_win) - ((1 - result.win_rate) * avg_loss)

        # Max drawdown from equity curve
        equity_curve = []
        running = 0.0
        for p in pnl_list:
            running += p
            equity_curve.append(running)
        peak = 0.0
        max_dd = 0.0
        for eq in equity_curve:
            if eq > peak:
                peak = eq
            dd = peak - eq
            if dd > max_dd:
                max_dd = dd
        result.max_drawdown = max_dd

        # Max drawdown in R
        r_curve = []
        running_r = 0.0
        for r in r_list:
            running_r += r
            r_curve.append(running_r)
        peak_r = 0.0
        max_dd_r = 0.0
        for eq in r_curve:
            if eq > peak_r:
                peak_r = eq
            dd = peak_r - eq
            if dd > max_dd_r:
                max_dd_r = dd
        result.max_drawdown_r = max_dd_r

        # Slippage
        slip_list = [t.slippage_realized_bps for t in trades]
        result.realized_slippage_avg_bps = safe_div(sum(slip_list), result.n_trades)

        # Sharpe (per-trade, rough proxy)
        if len(r_list) > 1:
            import numpy as np
            r_arr = [float(x) for x in r_list]
            mu = sum(r_arr) / len(r_arr)
            var = sum((x - mu) ** 2 for x in r_arr) / (len(r_arr) - 1)
            std = math.sqrt(var)
            result.sharpe_ratio = safe_div(mu, std)

        return result

    @staticmethod
    def segment_by(trades: List[TradeJournal], by: str) -> Dict[str, BacktestResult]:
        """
        Segment trades by a field and compute metrics for each segment.

        `by` can be: "mode", "symbol", "funding_regime"
        """
        groups: Dict[str, List[TradeJournal]] = {}
        for t in trades:
            if by == "mode":
                key = t.mode
            elif by == "symbol":
                key = t.symbol
            elif by == "funding_regime":
                # Bucket by funding percentile
                if t.funding_percentile_7d <= 0.10:
                    key = "extreme_negative"
                elif t.funding_percentile_7d <= 0.25:
                    key = "very_negative"
                else:
                    key = "mildly_negative"
            else:
                key = str(getattr(t, by, "unknown"))
            groups.setdefault(key, []).append(t)

        return {k: BacktestMetrics.compute(v) for k, v in groups.items()}

    @staticmethod
    def print_report(result: BacktestResult) -> None:
        """Print a formatted backtest report to stdout."""
        print("=" * 60)
        print("BACKTEST REPORT")
        print("=" * 60)
        print(f"  Trades         : {result.n_trades}")
        print(f"  Wins / Losses  : {result.n_wins} / {result.n_losses}")
        print(f"  Win Rate       : {result.win_rate * 100:.1f}%")
        print(f"  Total PnL      : ${result.total_pnl_usd:,.2f}")
        print(f"  Avg PnL        : ${result.avg_pnl_usd:.2f}")
        print(f"  Total R        : {result.total_pnl_r:.2f}R")
        print(f"  Avg R          : {result.avg_r:.3f}R")
        print(f"  Expectancy     : ${result.expectancy:.2f} / trade")
        print(f"  Profit Factor  : {result.profit_factor:.2f}")
        print(f"  Max Drawdown   : ${result.max_drawdown:,.2f}")
        print(f"  Max DD (R)     : {result.max_drawdown_r:.2f}R")
        print(f"  Sharpe         : {result.sharpe_ratio:.2f}")
        print(f"  Avg Slip (bps) : {result.realized_slippage_avg_bps:.2f}")

        if result.segments:
            print()
            print("SEGMENTS:")
            for seg_name, seg in result.segments.items():
                print(f"  [{seg_name}] n={seg.n_trades} wr={seg.win_rate*100:.0f}% "
                      f"totalR={seg.total_pnl_r:.2f} pf={seg.profit_factor:.2f}")
        print("=" * 60)
