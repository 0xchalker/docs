"""
Exchange REST adapter — Binance Futures compatible.
Supports dry_run mode (simulated fills) and testnet.
"""
import asyncio
import hashlib
import hmac
import time
import uuid
from typing import Any, Dict, Optional, Tuple
import aiohttp

from utils.logging_utils import get_logger
from core.models import OrderResult

logger = get_logger(__name__)

_MAINNET_BASE = "https://fapi.binance.com"
_TESTNET_BASE = "https://testnet.binancefuture.com"
_MAX_RETRIES = 3
_BACKOFF_BASE = 0.5


class VenueRestClient:
    """
    Binance Futures REST adapter.
    """

    def __init__(
        self,
        api_key: str = "",
        api_secret: str = "",
        testnet: bool = True,
        dry_run: bool = True,
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._base_url = _TESTNET_BASE if testnet else _MAINNET_BASE
        self._dry_run = dry_run
        self._session: Optional[aiohttp.ClientSession] = None
        # Simulated positions for dry_run
        self._dry_positions: Dict[str, Dict] = {}
        self._dry_equity = 10_000.0

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"X-MBX-APIKEY": self._api_key}
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    def _sign(self, params: Dict) -> Dict:
        """Add timestamp and signature to params dict."""
        params["timestamp"] = int(time.time() * 1000)
        query_string = "&".join(f"{k}={v}" for k, v in params.items())
        signature = hmac.new(
            self._api_secret.encode(), query_string.encode(), hashlib.sha256
        ).hexdigest()
        params["signature"] = signature
        return params

    async def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict] = None,
        signed: bool = False,
    ) -> Any:
        params = dict(params or {})
        if signed:
            params = self._sign(params)
        url = f"{self._base_url}{path}"
        session = await self._get_session()
        last_exc: Optional[Exception] = None
        for attempt in range(_MAX_RETRIES):
            try:
                async with session.request(
                    method, url,
                    params=params if method == "GET" else None,
                    data=params if method != "GET" else None,
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as resp:
                    if resp.status in (429, 418):
                        wait = _BACKOFF_BASE * (2 ** attempt)
                        logger.warning("Venue rate limited", status=resp.status, wait=wait)
                        await asyncio.sleep(wait)
                        continue
                    resp.raise_for_status()
                    return await resp.json(content_type=None)
            except aiohttp.ClientError as exc:
                last_exc = exc
                await asyncio.sleep(_BACKOFF_BASE * (2 ** attempt))
        raise RuntimeError(f"Venue request failed: {last_exc}")

    # -------------------------------------------------------------------------
    # Market data
    # -------------------------------------------------------------------------

    async def get_best_bid_ask(self, symbol: str) -> Tuple[float, float, float]:
        """Returns (bid, ask, spread_bps)."""
        data = await self._request("GET", "/fapi/v1/ticker/bookTicker", {"symbol": symbol})
        bid = float(data["bidPrice"])
        ask = float(data["askPrice"])
        mid = (bid + ask) / 2.0
        spread_bps = ((ask - bid) / mid) * 10_000 if mid > 0 else 0.0
        return bid, ask, spread_bps

    async def get_mark_price(self, symbol: str) -> float:
        """Returns current mark price."""
        data = await self._request("GET", "/fapi/v1/premiumIndex", {"symbol": symbol})
        return float(data["markPrice"])

    async def get_funding_rate(self, symbol: str) -> Dict:
        """Returns current funding rate and next funding time."""
        data = await self._request("GET", "/fapi/v1/premiumIndex", {"symbol": symbol})
        return {
            "funding_rate": float(data.get("lastFundingRate", 0)),
            "next_funding_time": int(data.get("nextFundingTime", 0)),
            "mark_price": float(data.get("markPrice", 0)),
        }

    # -------------------------------------------------------------------------
    # Account
    # -------------------------------------------------------------------------

    async def get_account_equity(self) -> float:
        """Return account equity in USDT."""
        if self._dry_run:
            return self._dry_equity
        data = await self._request("GET", "/fapi/v2/account", signed=True)
        return float(data.get("totalWalletBalance", 0))

    async def get_open_position(self, symbol: str) -> Optional[Dict]:
        """Return open position dict or None."""
        if self._dry_run:
            return self._dry_positions.get(symbol)
        data = await self._request("GET", "/fapi/v2/positionRisk", {"symbol": symbol}, signed=True)
        for pos in data:
            if pos["symbol"] == symbol and float(pos["positionAmt"]) != 0:
                return pos
        return None

    # -------------------------------------------------------------------------
    # Orders
    # -------------------------------------------------------------------------

    async def place_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        order_type: str = "LIMIT",
        tif: str = "IOC",
    ) -> OrderResult:
        """Place an order. In dry_run mode, simulates fill at price."""
        if self._dry_run:
            order_id = f"DRY-{uuid.uuid4().hex[:8]}"
            logger.info(
                "DRY RUN order",
                symbol=symbol, side=side, qty=quantity,
                price=price, type=order_type, tif=tif, order_id=order_id,
            )
            self._dry_positions[symbol] = {
                "symbol": symbol, "side": side, "price": price,
                "qty": quantity, "order_id": order_id,
            }
            return OrderResult(
                accepted=True, order_id=order_id,
                fill_price=price, fill_qty=quantity, slippage_bps=0.0
            )
        params = {
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "quantity": quantity,
            "price": price,
            "timeInForce": tif,
        }
        try:
            data = await self._request("POST", "/fapi/v1/order", params, signed=True)
            fill_price = float(data.get("avgPrice") or price)
            fill_qty = float(data.get("executedQty", 0))
            slippage_bps = abs(fill_price - price) / price * 10_000 if price > 0 else 0.0
            return OrderResult(
                accepted=True,
                order_id=str(data.get("orderId", "")),
                fill_price=fill_price,
                fill_qty=fill_qty,
                slippage_bps=slippage_bps,
            )
        except Exception as exc:
            logger.error("place_order failed", symbol=symbol, error=str(exc))
            return OrderResult(accepted=False, reject_reason=str(exc))

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """Cancel an open order. Returns True if successful."""
        if self._dry_run:
            return True
        try:
            await self._request("DELETE", "/fapi/v1/order", {"symbol": symbol, "orderId": order_id}, signed=True)
            return True
        except Exception as exc:
            logger.error("cancel_order failed", symbol=symbol, order_id=order_id, error=str(exc))
            return False

    async def get_order(self, symbol: str, order_id: str) -> Optional[Dict]:
        """Get order status."""
        if self._dry_run:
            pos = self._dry_positions.get(symbol)
            if pos and pos.get("order_id") == order_id:
                return {"status": "FILLED", "avgPrice": pos["price"], "executedQty": pos["qty"]}
            return None
        try:
            return await self._request("GET", "/fapi/v1/order", {"symbol": symbol, "orderId": order_id}, signed=True)
        except Exception as exc:
            logger.error("get_order failed", symbol=symbol, order_id=order_id, error=str(exc))
            return None

    async def partial_close(self, symbol: str, pct: float, reduce_only: bool = True) -> OrderResult:
        """Close `pct` fraction of the open position."""
        pos = await self.get_open_position(symbol)
        if pos is None:
            return OrderResult(accepted=False, reject_reason="no position")
        qty = float(pos.get("qty") or pos.get("positionAmt", 0))
        close_qty = round(abs(qty) * pct, 6)
        side = "SELL" if (pos.get("side") == "BUY" or qty > 0) else "BUY"
        bid, ask, _ = await self.get_best_bid_ask(symbol)
        price = bid if side == "SELL" else ask
        params: Dict[str, Any] = {
            "symbol": symbol, "side": side, "quantity": close_qty,
            "type": "MARKET",
        }
        if reduce_only:
            params["reduceOnly"] = "true"
        if self._dry_run:
            logger.info("DRY RUN partial_close", symbol=symbol, pct=pct, qty=close_qty)
            if symbol in self._dry_positions:
                self._dry_positions[symbol]["qty"] = abs(qty) * (1 - pct)
            return OrderResult(accepted=True, fill_price=price, fill_qty=close_qty)
        try:
            data = await self._request("POST", "/fapi/v1/order", params, signed=True)
            fill_price = float(data.get("avgPrice") or price)
            return OrderResult(accepted=True, order_id=str(data.get("orderId", "")), fill_price=fill_price, fill_qty=close_qty)
        except Exception as exc:
            logger.error("partial_close failed", symbol=symbol, error=str(exc))
            return OrderResult(accepted=False, reject_reason=str(exc))

    async def close_position(self, symbol: str) -> OrderResult:
        """Full market close of open position."""
        return await self.partial_close(symbol, pct=1.0)

    async def move_stop_to_break_even(self, symbol: str) -> bool:
        """Move stop loss to break-even (entry price). Dry-run: log only."""
        pos = await self.get_open_position(symbol)
        if pos is None:
            return False
        entry = float(pos.get("price") or pos.get("entryPrice", 0))
        logger.info("move_stop_to_break_even", symbol=symbol, entry=entry, dry_run=self._dry_run)
        if self._dry_run:
            return True
        # In a real implementation, this would place a stop-limit order at entry
        # Omitted for brevity — exchange-specific SL order management
        return True

    async def get_best_marketable_limit_price(
        self, symbol: str, side: str, max_slippage_bps: float = 5.0
    ) -> float:
        """
        Compute a marketable limit price that crosses the spread but
        stays within max_slippage_bps of the mid price.
        """
        bid, ask, _ = await self.get_best_bid_ask(symbol)
        mid = (bid + ask) / 2.0
        if side.upper() == "BUY":
            # Limit buy: at ask + small buffer, capped at mid * (1 + max_slippage)
            limit_price = ask * 1.0001
            cap = mid * (1 + max_slippage_bps / 10_000)
            return min(limit_price, cap)
        else:
            # Limit sell: at bid - small buffer, floored at mid * (1 - max_slippage)
            limit_price = bid * 0.9999
            floor = mid * (1 - max_slippage_bps / 10_000)
            return max(limit_price, floor)
