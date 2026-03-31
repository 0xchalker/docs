#!/usr/bin/env python3
"""
Funding Rollover Scalp Bot - Web Dashboard Backend
FastAPI server exposing REST endpoints and WebSocket for the UI.
"""
import argparse
import asyncio
import json
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from typing import Any, Optional

import uvicorn
import yaml
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_HERE = Path(__file__).parent
_DB_PATH = _HERE.parent / "data" / "bot.db"
_CONFIG_PATH = _HERE.parent / "config" / "strategy.yaml"
_STATIC_PATH = _HERE / "static"

# ---------------------------------------------------------------------------
# In-memory state (simulated bot state when running separately from bot)
# ---------------------------------------------------------------------------

_app_state: dict[str, Any] = {
    "bot_running": False,
    "dry_run": True,
    "start_time": None,
    "ws_clients": [],
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _get_db_connection() -> Optional[sqlite3.Connection]:
    """Return a sqlite3 connection or None if DB doesn't exist."""
    if not _DB_PATH.exists():
        return None
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _load_config() -> dict:
    """Load strategy.yaml or return defaults."""
    defaults = {
        "dry_run": True,
        "whitelist_symbols": ["BTCUSDT", "ETHUSDT"],
        "scanner": {},
        "thresholds": {},
        "risk": {},
    }
    if not _CONFIG_PATH.exists():
        return defaults
    try:
        with open(_CONFIG_PATH) as f:
            raw = yaml.safe_load(f) or {}
        strategy = raw.get("strategy", {})
        return {
            "dry_run": strategy.get("dry_run", True),
            "whitelist_symbols": strategy.get("whitelist_symbols", ["BTCUSDT", "ETHUSDT"]),
            "scanner": raw.get("scanner", {}),
            "thresholds": raw.get("thresholds", {}),
            "risk": {
                k: v for k, v in raw.get("risk", {}).items()
                if k not in ("api_key", "api_secret")
            },
            "execution": raw.get("execution", {}),
            "filters": raw.get("filters", {}),
        }
    except Exception:
        return defaults


def _seconds_to_next_funding() -> dict[str, float]:
    """Calculate seconds to next 8h funding for each tracked symbol."""
    cfg = _load_config()
    symbols = cfg.get("whitelist_symbols", [])
    now = datetime.now(tz=timezone.utc)
    # Funding happens at 00:00, 08:00, 16:00 UTC
    funding_hours = [0, 8, 16]
    result = {}
    for sym in symbols:
        next_times = []
        for h in funding_hours:
            candidate = now.replace(hour=h, minute=0, second=0, microsecond=0)
            if candidate <= now:
                candidate += timedelta(days=1)
            next_times.append(candidate)
        next_funding = min(next_times)
        result[sym] = (next_funding - now).total_seconds()
    return result


def _query_trades_raw(
    limit: int = 50,
    symbol: str = "",
    start: str = "",
    end: str = "",
    offset: int = 0,
) -> tuple[list[dict], int]:
    """Query trades from SQLite. Returns (rows, total_count)."""
    conn = _get_db_connection()
    if conn is None:
        return [], 0
    try:
        conditions = []
        params: list[Any] = []
        if symbol:
            conditions.append("symbol = ?")
            params.append(symbol)
        if start:
            conditions.append("entry_time >= ?")
            params.append(start)
        if end:
            conditions.append("entry_time <= ?")
            params.append(end)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        count_row = conn.execute(
            f"SELECT COUNT(*) FROM trades {where}", params
        ).fetchone()
        total = count_row[0] if count_row else 0

        rows = conn.execute(
            f"SELECT * FROM trades {where} ORDER BY entry_time DESC LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()

        result = [dict(r) for r in rows]
        return result, total
    finally:
        conn.close()


def _compute_metrics() -> dict:
    """Compute performance metrics from trade history."""
    conn = _get_db_connection()
    empty_overall = {
        "total_trades": 0,
        "win_rate": 0.0,
        "profit_factor": 0.0,
        "expectancy_r": 0.0,
        "total_pnl_usd": 0.0,
        "max_drawdown_usd": 0.0,
        "avg_hold_seconds": 0.0,
    }
    empty_today = {"trades": 0, "pnl_usd": 0.0, "wins": 0, "losses": 0}
    empty_by_mode = {
        "LONG_AT_ROLLOVER": {"trades": 0, "pnl_usd": 0.0, "win_rate": 0.0},
        "SHORT_AT_ROLLOVER": {"trades": 0, "pnl_usd": 0.0, "win_rate": 0.0},
    }

    if conn is None:
        return {
            "overall": empty_overall,
            "today": empty_today,
            "by_mode": empty_by_mode,
            "pnl_series": [],
        }

    try:
        rows = conn.execute(
            "SELECT * FROM trades ORDER BY entry_time ASC"
        ).fetchall()
        trades = [dict(r) for r in rows]

        if not trades:
            return {
                "overall": empty_overall,
                "today": empty_today,
                "by_mode": empty_by_mode,
                "pnl_series": [],
            }

        total = len(trades)
        wins = [t for t in trades if (t.get("pnl_usd") or 0) > 0]
        losses = [t for t in trades if (t.get("pnl_usd") or 0) < 0]
        total_pnl = sum(t.get("pnl_usd") or 0 for t in trades)
        gross_profit = sum(t.get("pnl_usd") or 0 for t in wins)
        gross_loss = abs(sum(t.get("pnl_usd") or 0 for t in losses))
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0.0
        win_rate = len(wins) / total if total > 0 else 0.0
        expectancy_r = sum(t.get("pnl_r") or 0 for t in trades) / total if total > 0 else 0.0

        # Hold time
        hold_secs = []
        for t in trades:
            try:
                et = t.get("entry_time")
                xt = t.get("exit_time")
                if et and xt:
                    e = datetime.fromisoformat(et)
                    x = datetime.fromisoformat(xt)
                    hold_secs.append((x - e).total_seconds())
            except Exception:
                pass
        avg_hold = sum(hold_secs) / len(hold_secs) if hold_secs else 0.0

        # Max drawdown
        cumulative = 0.0
        peak = 0.0
        max_dd = 0.0
        for t in trades:
            cumulative += t.get("pnl_usd") or 0
            if cumulative > peak:
                peak = cumulative
            dd = peak - cumulative
            if dd > max_dd:
                max_dd = dd

        # Today
        today_str = date.today().isoformat()
        today_trades = [
            t for t in trades
            if (t.get("entry_time") or "").startswith(today_str)
        ]
        today_wins = [t for t in today_trades if (t.get("pnl_usd") or 0) > 0]
        today_losses = [t for t in today_trades if (t.get("pnl_usd") or 0) <= 0]

        # By mode
        by_mode = {}
        for mode in ["LONG_AT_ROLLOVER", "SHORT_AT_ROLLOVER"]:
            mode_trades = [t for t in trades if t.get("mode") == mode]
            mode_wins = [t for t in mode_trades if (t.get("pnl_usd") or 0) > 0]
            by_mode[mode] = {
                "trades": len(mode_trades),
                "pnl_usd": sum(t.get("pnl_usd") or 0 for t in mode_trades),
                "win_rate": len(mode_wins) / len(mode_trades) if mode_trades else 0.0,
            }

        # PnL series (daily)
        from collections import defaultdict
        daily: dict[str, float] = defaultdict(float)
        for t in trades:
            et = t.get("entry_time") or ""
            d = et[:10] if len(et) >= 10 else "unknown"
            daily[d] += t.get("pnl_usd") or 0

        cumul = 0.0
        pnl_series = []
        for d in sorted(daily.keys()):
            cumul += daily[d]
            pnl_series.append({
                "date": d,
                "cumulative_pnl": round(cumul, 4),
                "daily_pnl": round(daily[d], 4),
            })

        return {
            "overall": {
                "total_trades": total,
                "win_rate": round(win_rate, 4),
                "profit_factor": round(profit_factor, 4),
                "expectancy_r": round(expectancy_r, 4),
                "total_pnl_usd": round(total_pnl, 4),
                "max_drawdown_usd": round(max_dd, 4),
                "avg_hold_seconds": round(avg_hold, 2),
            },
            "today": {
                "trades": len(today_trades),
                "pnl_usd": round(sum(t.get("pnl_usd") or 0 for t in today_trades), 4),
                "wins": len(today_wins),
                "losses": len(today_losses),
            },
            "by_mode": by_mode,
            "pnl_series": pnl_series,
        }
    finally:
        conn.close()


def _query_scanner() -> dict:
    """Get latest scan candidates from DB."""
    conn = _get_db_connection()
    if conn is None:
        return {"candidates": [], "scanned_at": _now_iso()}
    try:
        rows = conn.execute(
            """
            SELECT sc.*
            FROM scan_candidates sc
            INNER JOIN (
                SELECT symbol, MAX(created_at) as max_created
                FROM scan_candidates
                GROUP BY symbol
            ) latest ON sc.symbol = latest.symbol AND sc.created_at = latest.max_created
            ORDER BY coin_selection_score DESC
            LIMIT 20
            """
        ).fetchall()

        candidates = []
        scanned_at = _now_iso()
        for r in rows:
            d = dict(r)
            candidates.append({
                "symbol": d.get("symbol", ""),
                "exchange": d.get("exchange", "binance"),
                "funding_pct": d.get("funding_pct", 0.0),
                "funding_percentile_7d": d.get("funding_percentile_7d", 0.5),
                "funding_zscore_24h": d.get("funding_zscore_24h", 0.0),
                "oi_usd": d.get("oi_usd", 0.0),
                "oi_delta_15m": d.get("oi_delta_15m", 0.0),
                "spread_bps": d.get("spread_bps", 0.0),
                "slippage_estimate_bps": d.get("slippage_estimate_bps", 0.0),
                "coin_selection_score": d.get("coin_selection_score", 0),
                "shortlisted_at": d.get("shortlisted_at") or d.get("created_at") or _now_iso(),
            })
            if d.get("shortlisted_at") or d.get("created_at"):
                scanned_at = d.get("shortlisted_at") or d.get("created_at")

        return {"candidates": candidates, "scanned_at": scanned_at}
    finally:
        conn.close()


def _query_signals() -> dict:
    """Get latest signal scores from feature_snapshots."""
    conn = _get_db_connection()
    if conn is None:
        return {"signals": {}}
    try:
        rows = conn.execute(
            """
            SELECT fs.*
            FROM feature_snapshots fs
            INNER JOIN (
                SELECT symbol, MAX(created_at) as max_created
                FROM feature_snapshots
                WHERE label = 'signal'
                GROUP BY symbol
            ) latest ON fs.symbol = latest.symbol AND fs.created_at = latest.max_created
            """
        ).fetchall()

        signals = {}
        for r in rows:
            d = dict(r)
            data_json = {}
            try:
                data_json = json.loads(d.get("data_json") or "{}")
            except Exception:
                pass

            sym = d.get("symbol", "")
            signals[sym] = {
                "long_score": data_json.get("long_score", 0),
                "short_score": data_json.get("short_score", 0),
                "decision": data_json.get("decision", "NO_TRADE"),
                "locked_at": d.get("snapshot_timestamp") or _now_iso(),
                "features": {
                    "funding_now": data_json.get("funding_now", 0.0),
                    "funding_oi_weighted_now": data_json.get("funding_oi_weighted_now", 0.0),
                    "funding_zscore_24h": data_json.get("funding_zscore_24h", 0.0),
                    "oi_delta_5m": data_json.get("oi_delta_5m", 0.0),
                    "oi_delta_15m": data_json.get("oi_delta_15m", 0.0),
                    "price_delta_5m": data_json.get("price_delta_5m", 0.0),
                    "taker_buy_sell_ratio_5m": data_json.get("taker_buy_sell_ratio_5m", 1.0),
                    "spread_bps": data_json.get("spread_bps", 0.0),
                },
            }
        return {"signals": signals}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Lifespan & WebSocket broadcaster
# ---------------------------------------------------------------------------


async def _ws_broadcast(data: dict) -> None:
    clients = _app_state.get("ws_clients", [])
    dead = []
    for ws in clients:
        try:
            await ws.send_text(json.dumps(data))
        except Exception:
            dead.append(ws)
    for ws in dead:
        try:
            clients.remove(ws)
        except ValueError:
            pass


async def _tick_loop() -> None:
    """Push a tick event every second to all WebSocket clients."""
    cfg = _load_config()
    symbols = cfg.get("whitelist_symbols", [])
    states = {sym: "FLAT" for sym in symbols}

    while True:
        await asyncio.sleep(1)
        if not _app_state.get("ws_clients"):
            continue

        countdowns = _seconds_to_next_funding()
        payload = {
            "type": "tick",
            "timestamp": _now_iso(),
            "states": states,
            "seconds_to_next_funding": countdowns,
            "active_signals": {},
        }
        await _ws_broadcast(payload)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_tick_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="Funding Scalp Bot Dashboard", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(_STATIC_PATH)), name="static")


@app.get("/")
async def root():
    return FileResponse(str(_STATIC_PATH / "index.html"))


# ---------------------------------------------------------------------------
# REST Endpoints
# ---------------------------------------------------------------------------


@app.get("/api/status")
async def get_status():
    cfg = _load_config()
    symbols = cfg.get("whitelist_symbols", [])
    uptime = 0
    if _app_state.get("start_time"):
        uptime = (datetime.now(tz=timezone.utc) - _app_state["start_time"]).total_seconds()

    return {
        "bot_running": _app_state.get("bot_running", False),
        "dry_run": cfg.get("dry_run", True),
        "symbols": symbols,
        "states": {sym: "FLAT" for sym in symbols},
        "uptime_seconds": round(uptime, 1),
        "last_updated": _now_iso(),
    }


@app.get("/api/scanner")
async def get_scanner():
    return _query_scanner()


@app.get("/api/signals")
async def get_signals():
    return _query_signals()


@app.get("/api/positions")
async def get_positions():
    # Positions would come from the running bot process in production.
    # Here we read the most recent open trades (those without exit_time) as a proxy.
    conn = _get_db_connection()
    if conn is None:
        return {"positions": []}
    try:
        rows = conn.execute(
            "SELECT * FROM trades WHERE exit_time IS NULL ORDER BY entry_time DESC LIMIT 20"
        ).fetchall()
        positions = []
        for r in rows:
            d = dict(r)
            entry = d.get("entry_price") or 0.0
            mark = entry  # no live mark price without exchange connection
            size = d.get("size") or 0.0
            side = "BUY" if (d.get("mode") or "") == "LONG_AT_ROLLOVER" else "SELL"
            hold = 0
            try:
                et = d.get("entry_time")
                if et:
                    e = datetime.fromisoformat(et)
                    hold = int((datetime.now(tz=timezone.utc) - e.replace(tzinfo=timezone.utc)).total_seconds())
            except Exception:
                pass

            positions.append({
                "symbol": d.get("symbol", ""),
                "side": side,
                "size": size,
                "entry_price": entry,
                "mark_price": mark,
                "pnl_usd": d.get("pnl_usd") or 0.0,
                "pnl_pct": ((mark - entry) / entry * 100) if entry else 0.0,
                "entry_time": d.get("entry_time") or _now_iso(),
                "hold_seconds": hold,
                "mode": d.get("mode") or "",
                "tp1_done": False,
            })
        return {"positions": positions}
    finally:
        conn.close()


@app.get("/api/trades")
async def get_trades(
    limit: int = Query(50, ge=1, le=500),
    symbol: str = Query(""),
    start: str = Query(""),
    end: str = Query(""),
    page: int = Query(0, ge=0),
):
    offset = page * limit
    trades, total = _query_trades_raw(
        limit=limit, symbol=symbol, start=start, end=end, offset=offset
    )
    return {"trades": trades, "total": total}


@app.get("/api/metrics")
async def get_metrics():
    return _compute_metrics()


@app.get("/api/config")
async def get_config():
    cfg = _load_config()
    # Sanitize: remove any API keys
    sanitized = {
        k: v for k, v in cfg.items()
        if k not in ("api_key", "api_secret", "coinglass")
    }
    return sanitized


@app.post("/api/control")
async def post_control(body: dict):
    action = body.get("action", "")
    if action == "start":
        _app_state["bot_running"] = True
        _app_state["start_time"] = datetime.now(tz=timezone.utc)
        await _ws_broadcast({"type": "state_change", "event": "bot_started", "timestamp": _now_iso()})
        return {"ok": True, "message": "Bot started (UI simulation)"}
    elif action == "stop":
        _app_state["bot_running"] = False
        _app_state["start_time"] = None
        await _ws_broadcast({"type": "state_change", "event": "bot_stopped", "timestamp": _now_iso()})
        return {"ok": True, "message": "Bot stopped (UI simulation)"}
    elif action == "toggle_dry_run":
        # Cannot truly toggle without restarting the bot process; reflect in UI state
        _app_state["dry_run"] = not _app_state.get("dry_run", True)
        return {"ok": True, "message": f"Dry run toggled to {_app_state['dry_run']} (UI only)"}
    else:
        return {"ok": False, "message": f"Unknown action: {action}"}


# ---------------------------------------------------------------------------
# WebSocket
# ---------------------------------------------------------------------------


@app.websocket("/ws/live")
async def websocket_live(ws: WebSocket):
    await ws.accept()
    _app_state.setdefault("ws_clients", []).append(ws)
    try:
        # Send immediate status on connect
        countdowns = _seconds_to_next_funding()
        cfg = _load_config()
        symbols = cfg.get("whitelist_symbols", [])
        await ws.send_text(json.dumps({
            "type": "tick",
            "timestamp": _now_iso(),
            "states": {sym: "FLAT" for sym in symbols},
            "seconds_to_next_funding": countdowns,
            "active_signals": {},
        }))
        while True:
            # Keep alive: wait for client messages (ping/pong or disconnect)
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        try:
            _app_state["ws_clients"].remove(ws)
        except ValueError:
            pass


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def run_server(host: str = "0.0.0.0", port: int = 8080) -> None:
    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Funding Scalp Bot Dashboard Server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    run_server(host=args.host, port=args.port)
