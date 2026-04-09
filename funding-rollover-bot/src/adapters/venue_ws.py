"""
Exchange WebSocket adapter — Binance Futures compatible.
Handles book ticker, mark price, and user data streams.
"""
import asyncio
import json
from typing import Any, Callable, Dict, Optional, List
import aiohttp

from utils.logging_utils import get_logger

logger = get_logger(__name__)

_MAINNET_WS = "wss://fstream.binance.com"
_TESTNET_WS = "wss://stream.binancefuture.com"
_RECONNECT_DELAY_BASE = 1.0
_RECONNECT_DELAY_MAX = 60.0
_LISTEN_KEY_REFRESH_INTERVAL = 1800  # 30 minutes


class VenueWebSocket:
    """
    Binance Futures WebSocket adapter.
    Supports combined streams for book ticker, mark price, and user data.
    """

    def __init__(
        self,
        api_key: str = "",
        testnet: bool = True,
    ) -> None:
        self._api_key = api_key
        self._base_ws = _TESTNET_WS if testnet else _MAINNET_WS
        self._base_rest = "https://testnet.binancefuture.com" if testnet else "https://fapi.binance.com"
        self._running = False
        # stream_name -> callback
        self._stream_callbacks: Dict[str, Callable] = {}
        self._subscribed_streams: List[str] = []
        self._listen_key: Optional[str] = None
        self._ws: Optional[Any] = None
        self._reconnect_delay = _RECONNECT_DELAY_BASE

    async def _get_listen_key(self) -> Optional[str]:
        """Obtain a user data stream listen key from the REST API."""
        if not self._api_key:
            return None
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self._base_rest}/fapi/v1/listenKey",
                    headers={"X-MBX-APIKEY": self._api_key},
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    data = await resp.json()
                    return data.get("listenKey")
        except Exception as exc:
            logger.error("Failed to get listen key", error=str(exc))
            return None

    async def _refresh_listen_key_loop(self) -> None:
        """Keep the listen key alive by refreshing it every 30 minutes."""
        while self._running and self._listen_key:
            await asyncio.sleep(_LISTEN_KEY_REFRESH_INTERVAL)
            try:
                async with aiohttp.ClientSession() as session:
                    await session.put(
                        f"{self._base_rest}/fapi/v1/listenKey",
                        headers={"X-MBX-APIKEY": self._api_key},
                        params={"listenKey": self._listen_key},
                        timeout=aiohttp.ClientTimeout(total=5),
                    )
            except Exception as exc:
                logger.warning("Failed to refresh listen key", error=str(exc))

    def _build_stream_url(self) -> str:
        streams = list(self._subscribed_streams)
        if self._listen_key:
            streams.append(self._listen_key)
        if not streams:
            return f"{self._base_ws}/ws/dummy"
        if len(streams) == 1:
            return f"{self._base_ws}/ws/{streams[0]}"
        return f"{self._base_ws}/stream?streams=" + "/".join(streams)

    async def connect(self) -> None:
        """Connect and start the receive loop with auto-reconnect."""
        self._running = True
        asyncio.create_task(self._connect_loop())

    async def _connect_loop(self) -> None:
        while self._running:
            url = self._build_stream_url()
            try:
                import websockets as ws_lib
                async with ws_lib.connect(url, ping_interval=20, close_timeout=5) as ws:
                    self._ws = ws
                    self._reconnect_delay = _RECONNECT_DELAY_BASE
                    logger.info("Venue WebSocket connected", url=url)
                    await self._recv_loop(ws)
            except Exception as exc:
                logger.warning("Venue WS disconnected", error=str(exc), reconnect_in=self._reconnect_delay)
            if not self._running:
                break
            await asyncio.sleep(self._reconnect_delay)
            self._reconnect_delay = min(self._reconnect_delay * 2, _RECONNECT_DELAY_MAX)

    async def _recv_loop(self, ws: Any) -> None:
        async for raw in ws:
            try:
                msg = json.loads(raw)
                # Combined stream messages have a "stream" wrapper
                stream_name = msg.get("stream", "")
                data = msg.get("data", msg)
                # Route by stream name or event type
                cb = self._stream_callbacks.get(stream_name)
                if cb is None:
                    # Try event type fallback
                    event_type = data.get("e", "")
                    cb = self._stream_callbacks.get(event_type)
                if cb:
                    result = cb(data)
                    if asyncio.iscoroutine(result):
                        await result
            except json.JSONDecodeError:
                pass
            except Exception as exc:
                logger.error("Venue WS dispatch error", error=str(exc))

    def _add_stream(self, stream_name: str, callback: Callable) -> None:
        if stream_name not in self._subscribed_streams:
            self._subscribed_streams.append(stream_name)
        self._stream_callbacks[stream_name] = callback

    async def subscribe_book_ticker(self, symbol: str, callback: Callable) -> None:
        """Subscribe to best bid/ask updates for a symbol."""
        stream = f"{symbol.lower()}@bookTicker"
        self._add_stream(stream, callback)
        logger.info("Subscribed book ticker", symbol=symbol)

    async def subscribe_mark_price(self, symbol: str, callback: Callable) -> None:
        """Subscribe to mark price and funding rate updates."""
        stream = f"{symbol.lower()}@markPrice@1s"
        self._add_stream(stream, callback)
        logger.info("Subscribed mark price", symbol=symbol)

    async def subscribe_user_data(self, callback: Callable) -> None:
        """Subscribe to user data stream (fills, order updates)."""
        self._listen_key = await self._get_listen_key()
        if self._listen_key:
            self._stream_callbacks["ORDER_TRADE_UPDATE"] = callback
            self._stream_callbacks["ACCOUNT_UPDATE"] = callback
            asyncio.create_task(self._refresh_listen_key_loop())
            logger.info("Subscribed user data stream")
        else:
            logger.warning("User data stream unavailable (no API key or listen key failed)")

    async def disconnect(self) -> None:
        self._running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        logger.info("Venue WebSocket disconnected")
