"""
CoinGlass WebSocket adapter.
Handles liquidation and trade order streams with auto-reconnect.
"""
import asyncio
import json
import time
from typing import Any, Callable, Dict, List, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from utils.logging_utils import get_logger

logger = get_logger(__name__)

_WS_BASE_URL = "wss://open-api-v3.coinglass.com/ws"
_RECONNECT_DELAY_BASE = 1.0
_RECONNECT_DELAY_MAX = 60.0
_HEARTBEAT_INTERVAL = 20.0


class CoinGlassWebSocket:
    """
    WebSocket client for CoinGlass real-time data streams.
    Supports liquidation orders and futures trade order streams.
    """

    def __init__(self, api_key: str, base_url: str = _WS_BASE_URL) -> None:
        self._api_key = api_key
        self._base_url = base_url
        self._ws: Optional[Any] = None
        self._running = False
        self._subscriptions: List[Dict] = []
        # topic -> callback
        self._callbacks: Dict[str, Callable] = {}
        self._reconnect_delay = _RECONNECT_DELAY_BASE
        self._recv_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None

    async def connect(self) -> None:
        """Connect to CoinGlass WebSocket and start receive loop."""
        self._running = True
        await self._connect_loop()

    async def _connect_loop(self) -> None:
        while self._running:
            try:
                logger.info("Connecting to CoinGlass WebSocket", url=self._base_url)
                async with websockets.connect(
                    self._base_url,
                    extra_headers={"CG-API-KEY": self._api_key},
                    ping_interval=None,  # we handle pings manually
                    close_timeout=5,
                ) as ws:
                    self._ws = ws
                    self._reconnect_delay = _RECONNECT_DELAY_BASE
                    logger.info("CoinGlass WebSocket connected")
                    # Re-subscribe on reconnect
                    for sub in self._subscriptions:
                        await self._send(sub)
                    # Start heartbeat
                    hb_task = asyncio.create_task(self._heartbeat_loop(ws))
                    try:
                        await self._recv_loop(ws)
                    finally:
                        hb_task.cancel()
                        try:
                            await hb_task
                        except asyncio.CancelledError:
                            pass
            except (ConnectionClosed, OSError, asyncio.TimeoutError) as exc:
                logger.warning("CoinGlass WS disconnected", error=str(exc), reconnect_in=self._reconnect_delay)
            except Exception as exc:
                logger.error("CoinGlass WS unexpected error", error=str(exc))
            if not self._running:
                break
            await asyncio.sleep(self._reconnect_delay)
            self._reconnect_delay = min(self._reconnect_delay * 2, _RECONNECT_DELAY_MAX)

    async def _recv_loop(self, ws: Any) -> None:
        async for raw in ws:
            try:
                msg = json.loads(raw)
                await self._dispatch(msg)
            except json.JSONDecodeError:
                logger.warning("CoinGlass WS invalid JSON", raw=str(raw)[:200])
            except Exception as exc:
                logger.error("CoinGlass WS dispatch error", error=str(exc))

    async def _heartbeat_loop(self, ws: Any) -> None:
        while True:
            await asyncio.sleep(_HEARTBEAT_INTERVAL)
            try:
                await ws.ping()
            except Exception:
                break

    async def _send(self, payload: Dict) -> None:
        if self._ws and not self._ws.closed:
            await self._ws.send(json.dumps(payload))

    async def _dispatch(self, msg: Dict) -> None:
        topic = msg.get("topic") or msg.get("channel") or msg.get("type", "")
        cb = self._callbacks.get(topic)
        if cb:
            try:
                result = cb(msg)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as exc:
                logger.error("CoinGlass WS callback error", topic=topic, error=str(exc))

    def _register_subscription(self, sub: Dict, topic: str, callback: Callable) -> None:
        self._subscriptions.append(sub)
        self._callbacks[topic] = callback

    async def subscribe_liquidation_orders(
        self, symbols: List[str], callback: Callable
    ) -> None:
        """Subscribe to real-time liquidation order feed for given symbols."""
        for symbol in symbols:
            topic = f"liquidation_order:{symbol}"
            sub = {"op": "subscribe", "topic": topic}
            self._register_subscription(sub, topic, callback)
            await self._send(sub)
        logger.info("Subscribed to CoinGlass liquidation orders", symbols=symbols)

    async def subscribe_futures_trade_orders(
        self, symbols: List[str], callback: Callable
    ) -> None:
        """Subscribe to large futures trade order feed for given symbols."""
        for symbol in symbols:
            topic = f"futures_trade:{symbol}"
            sub = {"op": "subscribe", "topic": topic}
            self._register_subscription(sub, topic, callback)
            await self._send(sub)
        logger.info("Subscribed to CoinGlass futures trade orders", symbols=symbols)

    async def disconnect(self) -> None:
        """Gracefully disconnect the WebSocket."""
        self._running = False
        if self._ws and not self._ws.closed:
            await self._ws.close()
        logger.info("CoinGlass WebSocket disconnected")
