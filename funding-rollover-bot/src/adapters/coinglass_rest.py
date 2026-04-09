"""
CoinGlass REST API adapter.
Handles authentication, rate limiting, retries, and response caching.
"""
import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import aiohttp

from utils.logging_utils import get_logger

logger = get_logger(__name__)

_DEFAULT_BASE_URL = "https://open-api-v3.coinglass.com"
_MAX_RETRIES = 4
_BACKOFF_BASE = 1.5  # seconds
_RATE_LIMIT_CALLS = 10
_RATE_LIMIT_WINDOW = 1.0  # seconds
_CACHE_TTL = 15.0  # seconds for most endpoints


class CoinGlassRestClient:
    """
    Async REST client for CoinGlass Open API v3.
    All public methods return parsed JSON dicts/lists.
    """

    def __init__(self, api_key: str, base_url: str = _DEFAULT_BASE_URL) -> None:
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._session: Optional[aiohttp.ClientSession] = None
        # Simple in-memory LRU-like cache: key -> (timestamp, value)
        self._cache: Dict[str, tuple] = {}
        # Rate-limit state
        self._call_times: List[float] = []

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={
                    "CG-API-KEY": self._api_key,
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                }
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    def _cache_key(self, path: str, params: Dict) -> str:
        param_str = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        return f"{path}?{param_str}"

    def _get_cached(self, key: str, ttl: float = _CACHE_TTL) -> Optional[Any]:
        entry = self._cache.get(key)
        if entry is None:
            return None
        ts, value = entry
        if time.monotonic() - ts < ttl:
            return value
        del self._cache[key]
        return None

    def _set_cached(self, key: str, value: Any) -> None:
        self._cache[key] = (time.monotonic(), value)

    async def _enforce_rate_limit(self) -> None:
        """Ensure we don't exceed _RATE_LIMIT_CALLS per _RATE_LIMIT_WINDOW."""
        now = time.monotonic()
        self._call_times = [t for t in self._call_times if now - t < _RATE_LIMIT_WINDOW]
        if len(self._call_times) >= _RATE_LIMIT_CALLS:
            sleep_for = _RATE_LIMIT_WINDOW - (now - self._call_times[0]) + 0.01
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
        self._call_times.append(time.monotonic())

    async def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict] = None,
        use_cache: bool = True,
        cache_ttl: float = _CACHE_TTL,
    ) -> Any:
        """Internal request with retry + exponential backoff + caching."""
        params = params or {}
        cache_key = self._cache_key(path, params)

        if use_cache and method.upper() == "GET":
            cached = self._get_cached(cache_key, cache_ttl)
            if cached is not None:
                return cached

        url = f"{self._base_url}{path}"
        session = await self._get_session()
        last_exc: Optional[Exception] = None

        for attempt in range(_MAX_RETRIES):
            await self._enforce_rate_limit()
            try:
                async with session.request(method, url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 429:
                        wait = _BACKOFF_BASE ** (attempt + 1)
                        logger.warning("CoinGlass rate limited, backing off", wait=wait, attempt=attempt)
                        await asyncio.sleep(wait)
                        continue
                    if resp.status >= 500:
                        wait = _BACKOFF_BASE ** attempt
                        logger.warning("CoinGlass server error", status=resp.status, wait=wait)
                        await asyncio.sleep(wait)
                        continue
                    resp.raise_for_status()
                    data = await resp.json(content_type=None)
                    if use_cache and method.upper() == "GET":
                        self._set_cached(cache_key, data)
                    return data
            except aiohttp.ClientError as exc:
                last_exc = exc
                wait = _BACKOFF_BASE ** attempt
                logger.warning("CoinGlass request error", error=str(exc), attempt=attempt, wait=wait)
                await asyncio.sleep(wait)

        raise RuntimeError(f"CoinGlass request failed after {_MAX_RETRIES} attempts: {last_exc}")

    # -------------------------------------------------------------------------
    # Public API methods
    # -------------------------------------------------------------------------

    async def get_supported_coins(self) -> List[Dict]:
        """GET /api/futures/supported-coins"""
        data = await self._request("GET", "/api/futures/supported-coins", cache_ttl=3600)
        return data.get("data", data) if isinstance(data, dict) else data

    async def get_supported_exchange_pairs(self) -> List[Dict]:
        """GET /api/futures/supported-exchange-pairs"""
        data = await self._request("GET", "/api/futures/supported-exchange-pairs", cache_ttl=3600)
        return data.get("data", data) if isinstance(data, dict) else data

    async def get_funding_rate_history(
        self,
        symbol: str,
        exchange: str,
        interval: str = "h1",
        start: Optional[int] = None,
        end: Optional[int] = None,
    ) -> List[Dict]:
        """GET /api/futures/funding-rate/history"""
        params: Dict[str, Any] = {"symbol": symbol, "exchange": exchange, "interval": interval}
        if start:
            params["startTime"] = start
        if end:
            params["endTime"] = end
        data = await self._request("GET", "/api/futures/funding-rate/history", params=params, cache_ttl=60)
        return data.get("data", data) if isinstance(data, dict) else data

    async def get_oi_weighted_funding_history(
        self,
        symbol: str,
        exchange: str,
        interval: str = "h1",
        start: Optional[int] = None,
        end: Optional[int] = None,
    ) -> List[Dict]:
        """GET /api/futures/funding-rate/oi-weight-history"""
        params: Dict[str, Any] = {"symbol": symbol, "exchange": exchange, "interval": interval}
        if start:
            params["startTime"] = start
        if end:
            params["endTime"] = end
        data = await self._request("GET", "/api/futures/funding-rate/oi-weight-history", params=params, cache_ttl=60)
        return data.get("data", data) if isinstance(data, dict) else data

    async def get_oi_history(
        self,
        symbol: str,
        exchange: str,
        interval: str = "m1",
        start: Optional[int] = None,
        end: Optional[int] = None,
    ) -> List[Dict]:
        """GET /api/futures/open-interest/history"""
        params: Dict[str, Any] = {"symbol": symbol, "exchange": exchange, "interval": interval}
        if start:
            params["startTime"] = start
        if end:
            params["endTime"] = end
        data = await self._request("GET", "/api/futures/open-interest/history", params=params, cache_ttl=20)
        return data.get("data", data) if isinstance(data, dict) else data

    async def get_aggregated_oi_history(
        self,
        symbol: str,
        interval: str = "m1",
        start: Optional[int] = None,
        end: Optional[int] = None,
    ) -> List[Dict]:
        """GET /api/futures/open-interest/aggregated-history"""
        params: Dict[str, Any] = {"symbol": symbol, "interval": interval}
        if start:
            params["startTime"] = start
        if end:
            params["endTime"] = end
        data = await self._request("GET", "/api/futures/open-interest/aggregated-history", params=params, cache_ttl=20)
        return data.get("data", data) if isinstance(data, dict) else data

    async def get_liquidation_history(
        self,
        symbol: str,
        exchange: str,
        interval: str = "m1",
        start: Optional[int] = None,
        end: Optional[int] = None,
    ) -> List[Dict]:
        """GET /api/futures/liquidation/history"""
        params: Dict[str, Any] = {"symbol": symbol, "exchange": exchange, "interval": interval}
        if start:
            params["startTime"] = start
        if end:
            params["endTime"] = end
        data = await self._request("GET", "/api/futures/liquidation/history", params=params, cache_ttl=15)
        return data.get("data", data) if isinstance(data, dict) else data

    async def get_liquidation_order(self, symbol: str, exchange: str) -> List[Dict]:
        """GET /api/futures/liquidation/order"""
        params = {"symbol": symbol, "exchange": exchange}
        data = await self._request("GET", "/api/futures/liquidation/order", params=params, cache_ttl=5)
        return data.get("data", data) if isinstance(data, dict) else data

    async def get_taker_buy_sell_history(
        self,
        symbol: str,
        exchange: str,
        interval: str = "m1",
        start: Optional[int] = None,
        end: Optional[int] = None,
    ) -> List[Dict]:
        """GET /api/futures/v2/taker-buy-sell-volume/history"""
        params: Dict[str, Any] = {"symbol": symbol, "exchange": exchange, "interval": interval}
        if start:
            params["startTime"] = start
        if end:
            params["endTime"] = end
        data = await self._request(
            "GET", "/api/futures/v2/taker-buy-sell-volume/history", params=params, cache_ttl=15
        )
        return data.get("data", data) if isinstance(data, dict) else data

    async def get_funding_rate_current(self, symbol: str) -> List[Dict]:
        """Return current funding rates across all exchanges for a symbol."""
        params = {"symbol": symbol}
        data = await self._request(
            "GET", "/api/futures/funding-rate/current", params=params, cache_ttl=10
        )
        return data.get("data", data) if isinstance(data, dict) else data
