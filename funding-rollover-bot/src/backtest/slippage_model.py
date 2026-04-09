"""
Slippage model for backtesting.
Estimates and applies market impact slippage.
"""
import math
from utils.math_utils import safe_div


class SlippageModel:
    """
    Simple market impact slippage model.

    Slippage = base_spread_component + size_impact_component
    """

    def __init__(
        self,
        base_multiplier: float = 0.5,
        size_impact_factor: float = 0.1,
    ) -> None:
        """
        base_multiplier: fraction of half-spread charged as base slippage
        size_impact_factor: additional slippage per $1M of size (in bps)
        """
        self._base_mult = base_multiplier
        self._size_impact = size_impact_factor

    def estimate_slippage_bps(
        self,
        symbol: str,
        side: str,
        size_usd: float,
        spread_bps: float,
        atr_bps: float = 0.0,
    ) -> float:
        """
        Estimate total slippage in basis points.

        Components:
          1. Half-spread cost (we always pay the spread)
          2. Size impact: larger orders move the market more
          3. Volatility component: higher ATR = more volatile fills

        Returns total one-way slippage in bps (always >= 0).
        """
        # Half-spread as base cost
        half_spread = spread_bps * self._base_mult

        # Size impact: proportional to sqrt(size_usd / 1M)
        size_m = size_usd / 1_000_000.0
        size_impact = self._size_impact * math.sqrt(max(size_m, 0.0))

        # Volatility component: fraction of ATR
        vol_component = atr_bps * 0.05

        total = half_spread + size_impact + vol_component
        return max(total, 0.0)

    def apply_slippage(self, price: float, side: str, slippage_bps: float) -> float:
        """
        Apply slippage to a price in the unfavorable direction.

        BUY  -> fill_price = price * (1 + slippage_bps/10000)
        SELL -> fill_price = price * (1 - slippage_bps/10000)
        """
        adj = slippage_bps / 10_000.0
        if side.upper() == "BUY":
            return price * (1 + adj)
        else:
            return price * (1 - adj)
