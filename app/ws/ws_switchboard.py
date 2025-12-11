#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — WS Switchboard v4.3 (schema-hardened, executions-enabled, market-bus aware, multi-account)

Purpose
-------
Single source of truth for live WebSocket feeds, normalized for:
    • AI Snapshot Builder (Phase 3)
    • AI Memory (Phase 4)
    • Executor v3 (Phase 7)
    • Multi-account orchestration (Phase 8–9)
    • TP/SL Manager (stable TP ladders depend on this)

Scope
-----
- Connect to Bybit v5 PRIVATE WS for ONE unified account.
- Authenticate and subscribe to:
    • "position"
    • "execution"
- Connect to Bybit v5 PUBLIC WS (linear) for a list of symbols.
- Maintain local state files:
    • state/positions_bus.json          (consumed by position_bus)
    • state/orderbook_bus.json          (shared orderbook snapshots + updated_ms)
    • state/trades_bus.json             (freshness marker + recent trades per symbol)
    • state/public_trades.jsonl         (tick-by-tick trades)
    • state/ws_executions.jsonl         (user executions)
    • state/ws_switchboard_heartbeat_<ACCOUNT_LABEL>.txt  (heartbeat)
"""

from __future__ import annotations

import json
import os
import threading
import time
import hmac
import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import websocket  # type: ignore

# ---------------------------------------------------------------------------
# ROOT + .env loading
# ---------------------------------------------------------------------------

try:
    from app.core.config import settings
    ROOT: Path = settings.ROOT  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

# Load .env so BYBIT_* vars exist in this process
try:  # pragma: no cover
    from dotenv import load_dotenv  # type: ignore

    load_dotenv(ROOT / ".env")
except Exception:
    # Fall back to system env only
    pass

from app.core.logger import get_logger

LOG = get_logger("ws_switchboard")

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------

STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

POSITIONS_BUS_PATH: Path = STATE_DIR / "positions_bus.json"
ORDERBOOK_BUS_PATH: Path = STATE_DIR / "orderbook_bus.json"
TRADES_BUS_PATH: Path = STATE_DIR / "trades_bus.json"
PUBLIC_TRADES_PATH: Path = STATE_DIR / "public_trades.jsonl"
EXECUTIONS_PATH: Path = STATE_DIR / "ws_executions.jsonl"

# Max trades per symbol to keep in trades_bus.json (for market_bus.get_recent_trades)
TRADES_BUS_MAX_PER_SYMBOL: int = 200

DEFAULT_PUBLIC_SYMBOLS: List[str] = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "LINKUSDT",
    "AVAXUSDT",
    "OPUSDT",
    "DOGEUSDT",
    "INJUSDT",
    "XRPUSDT",
    "ADAUSDT",
]


def _now_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------------------------------------------------------
# Canonical position schema helpers
# ---------------------------------------------------------------------------

def _safe_float(x: Any) -> float:
    """Safely convert anything to float, fallback = 0."""
    try:
        return float(x)
    except Exception:
        return 0.0


def normalize_position(raw: dict, account_label: str) -> dict:
    """
    Normalize a raw Bybit position row into Flashback's strict schema.

    Output:
        {
          "symbol": "BTCUSDT",
          "side": "Buy",
          "size": 0.25,
          "avgPrice": 43200.5,
          "stopLoss": 0.0,
          "sub_uid": "12345",
          "account_label": "<ACCOUNT_LABEL>",
          "category": "linear",
        }
    """
    symbol = str(raw.get("symbol", "")).upper()
    side = str(raw.get("side", "")).title()  # "Buy"/"Sell"

    size = _safe_float(raw.get("size", 0))
    avg = _safe_float(raw.get("avgPrice", raw.get("entryPrice", 0)))

    sl_raw = (
        raw.get("stopLoss")
        or raw.get("stopLossPrice")
        or raw.get("slPrice")
        or 0
    )
    stop_loss = _safe_float(sl_raw)

    sub_uid = (
        raw.get("sub_uid")
        or raw.get("subAccountId")
        or raw.get("accountId")
        or raw.get("subId")
        or ""
    )
    sub_uid = str(sub_uid)

    return {
        "symbol": symbol,
        "side": side,
        "size": size,
        "avgPrice": avg,
        "stopLoss": stop_loss,
        "sub_uid": sub_uid,
        "account_label": account_label,
        "category": "linear",
    }


# ---------------------------------------------------------------------------
# State writers (simplified for Windows)
# ---------------------------------------------------------------------------

def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    """
    Simplified JSON writer for Windows.

    No tmp+replace games, just:
        - ensure parent dir
        - write JSON directly

    If something holds the file with an exclusive lock, we'll log the error.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(payload, separators=(",", ":"))
    try:
        path.write_text(data, encoding="utf-8")
    except Exception as e:
        LOG.error("Error writing JSON to %s: %s", path, e)


def _append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, separators=(",", ":")) + "\n")


def _load_json(path: Path) -> Dict[str, Any]:
    """
    Small tolerant JSON loader for internal state files.
    """
    try:
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {}
        return data
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# API key loading & WS auth signing
# ---------------------------------------------------------------------------

def _load_api_creds(account_label: str) -> Tuple[Optional[str], Optional[str], str]:
    """
    Resolve API key/secret for this ACCOUNT_LABEL.

    Priority for ACCOUNT_LABEL == "main":
        1) BYBIT_MAIN_WEBSOCKET_KEY / BYBIT_MAIN_WEBSOCKET_SECRET  (WS-dedicated)
        2) BYBIT_API_KEY / BYBIT_API_SECRET                        (canonical pair)
        3) BYBIT_MAIN_API_KEY / BYBIT_MAIN_API_SECRET

    Priority for other labels (e.g. "flashback01"):
        1) BYBIT_<LABEL_UPPER>_API_KEY / _API_SECRET   (e.g. BYBIT_FLASHBACK01_API_KEY)
        2) BYBIT_API_KEY / BYBIT_API_SECRET            (global fallback)
    """
    label_upper = account_label.upper()

    candidates: List[Tuple[str, str]] = []

    if account_label == "main":
        candidates.extend(
            [
                ("BYBIT_MAIN_WEBSOCKET_KEY", "BYBIT_MAIN_WEBSOCKET_SECRET"),
                ("BYBIT_API_KEY", "BYBIT_API_SECRET"),
                ("BYBIT_MAIN_API_KEY", "BYBIT_MAIN_API_SECRET"),
            ]
        )
    else:
        candidates.append(
            (f"BYBIT_{label_upper}_API_KEY", f"BYBIT_{label_upper}_API_SECRET")
        )
        candidates.append(("BYBIT_API_KEY", "BYBIT_API_SECRET"))

    for key_env, sec_env in candidates:
        key = os.getenv(key_env)
        sec = os.getenv(sec_env)
        if key and sec:
            LOG.info(
                "Using Bybit API creds from env: %s / %s",
                key_env,
                sec_env,
            )
            return key, sec, key_env

    return None, None, ""


def _build_ws_auth_payload(api_key: str, api_secret: str) -> Dict[str, Any]:
    """
    Build Bybit v5 PRIVATE WS auth payload.

    Per docs:
        expires = future timestamp (ms), as STRING
        signed_string = "GET/realtime" + expires
        signature = HMAC_SHA256(secret, signed_string)

    Payload:
        {
          "op": "auth",
          "args": [api_key, expires, signature]
        }
    """
    # Give ourselves ~60 seconds of TTL instead of 10ms paranoia
    expires_ms = _now_ms() + 60_000
    expires_str = str(expires_ms)

    signed_string = f"GET/realtime{expires_str}"
    signature = hmac.new(
        api_secret.encode("utf-8"),
        signed_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    LOG.info(
        "[PRIVATE] WS auth build: expires_ms=%s, signed_string=%s...",
        expires_ms,
        signed_string[:40],
    )
    LOG.info("[PRIVATE] WS auth signature prefix=%s...", signature[:10])

    payload = {
        "op": "auth",
        "args": [api_key, expires_str, signature],
    }
    LOG.info("[PRIVATE] WS auth payload to send (sanitized): %s", payload)
    return payload


# ---------------------------------------------------------------------------
# PRIVATE WS handlers
# ---------------------------------------------------------------------------

def _handle_private_message(
    msg: Dict[str, Any],
    account_label: str,
) -> None:
    # Auth / subscribe acks
    if msg.get("op") == "auth":
        LOG.info("[PRIVATE] Auth response raw: %s", msg)
        success = msg.get("success")
        ret_msg = msg.get("ret_msg")
        if not success:
            LOG.error("[PRIVATE] Auth FAILED: %s (raw=%s)", ret_msg, msg)
        else:
            LOG.info("[PRIVATE] Auth success: %s", ret_msg)
        return

    if msg.get("op") == "subscribe":
        LOG.info("[PRIVATE] Subscribe response raw: %s", msg)
        success = msg.get("success")
        ret_msg = msg.get("ret_msg")
        if not success:
            LOG.error("[PRIVATE] Subscribe FAILED: %s (raw=%s)", ret_msg, msg)
        else:
            LOG.info("[PRIVATE] Subscribe OK: %s", ret_msg)
        return

    topic = msg.get("topic")
    if not topic:
        return

    if topic == "position":
        now_ms = _now_ms()
        raw_data = msg.get("data") or []
        if isinstance(raw_data, dict):
            raw_data = [raw_data]

        # Normalize all positions into canonical schema
        norm_positions: List[dict] = []
        for p in raw_data:
            try:
                norm = normalize_position(p, account_label=account_label)
                if not norm["symbol"]:
                    continue
                # Skip zero-size rows to keep TP/SL manager & position_bus clean
                if norm["size"] <= 0:
                    continue
                norm_positions.append(norm)
            except Exception as e:
                LOG.error("[PRIVATE] Error normalizing position row %s: %s", p, e)

        # Merge into existing positions_bus.json so multiple ACCOUNT_LABELs can coexist
        existing = _load_json(POSITIONS_BUS_PATH)
        labels = existing.get("labels")
        if not isinstance(labels, dict):
            labels = {}
        labels[account_label] = {
            "category": "linear",
            "positions": norm_positions,
        }
        existing["labels"] = labels
        existing["version"] = 2
        existing["updated_ms"] = now_ms

        _atomic_write_json(POSITIONS_BUS_PATH, existing)
        LOG.debug(
            "[PRIVATE] Updated positions (normalized) for %s (count=%d)",
            account_label,
            len(norm_positions),
        )
        return

    if topic == "execution":
        data = msg.get("data") or []
        if isinstance(data, dict):
            data = [data]

        for row in data:
            row_out = {
                "version": 1,
                "account_label": account_label,
                "received_ms": _now_ms(),
                "execution": row,
            }
            _append_jsonl(EXECUTIONS_PATH, row_out)
        LOG.debug(
            "[PRIVATE] Appended %d executions for %s",
            len(data),
            account_label,
        )
        return


def _run_private_ws(
    url: str,
    account_label: str,
    api_key: str,
    api_secret: str,
    stop_event: threading.Event,
) -> None:
    """
    PRIVATE WS loop.

    Important fix:
        - Auth payload is built INSIDE on_open so every reconnect gets a fresh
          timestamp + signature. This prevents "Params Error" after ping/pong
          timeouts.
    """

    def on_open(ws: websocket.WebSocketApp) -> None:  # type: ignore
        LOG.info("[PRIVATE] WS connection opened, authenticating with Bybit...")
        try:
            auth_payload = _build_ws_auth_payload(api_key, api_secret)
        except Exception as e:
            LOG.error("[PRIVATE] Failed to build auth payload: %s", e)
            return
        ws.send(json.dumps(auth_payload))
        LOG.info("[PRIVATE] Auth payload sent.")
        sub = {
            "op": "subscribe",
            "args": ["position", "execution"],
        }
        ws.send(json.dumps(sub))
        LOG.info(
            "[PRIVATE] Subscribe payload for 'position' + 'execution' sent.",
        )

    def on_message(ws: websocket.WebSocketApp, message: str) -> None:  # type: ignore
        try:
            msg = json.loads(message)
        except Exception:
            LOG.error("[PRIVATE] WS received non-JSON message: %s", message)
            return

        LOG.debug("[PRIVATE] WS message: %s", msg)
        _handle_private_message(msg, account_label)

    def on_error(ws: websocket.WebSocketApp, error: Any) -> None:  # type: ignore
        LOG.error("[PRIVATE] WS error: %s", error)

    def on_close(
        ws: websocket.WebSocketApp,  # type: ignore
        status_code: Any,
        msg: Any,
    ) -> None:
        LOG.warning("[PRIVATE] WS closed: code=%s msg=%s", status_code, msg)

    while not stop_event.is_set():
        try:
            ws = websocket.WebSocketApp(
                url=url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=15, ping_timeout=10)
        except Exception as e:
            LOG.exception("[PRIVATE] WS run_forever threw exception: %s", e)

        if stop_event.is_set():
            break

        LOG.warning("[PRIVATE] WS disconnected, retrying in 5s...")
        time.sleep(5)


# ---------------------------------------------------------------------------
# PUBLIC WS handlers
# ---------------------------------------------------------------------------

def _handle_public_message(msg: Dict[str, Any]) -> None:
    topic = msg.get("topic")
    if not topic:
        return

    # Orderbook updates → orderbook_bus.json (market_bus-compatible)
    if topic.startswith("orderbook."):
        now_ms = _now_ms()

        data = msg.get("data") or {}
        if not isinstance(data, dict):
            data = {}
        symbol = data.get("s") or topic.split(".")[-1]

        # Bybit orderbook payload uses "b" / "a" for bids/asks
        bids = data.get("b") or []
        asks = data.get("a") or []

        # Exchange timestamp if present; else fall back to msg["ts"] or now
        ts_raw = data.get("ts") or msg.get("ts") or now_ms
        try:
            ts_ms = int(ts_raw)
        except Exception:
            ts_ms = now_ms

        existing = _load_json(ORDERBOOK_BUS_PATH)
        symbols_block = existing.get("symbols")
        if not isinstance(symbols_block, dict):
            symbols_block = {}

        symbols_block[symbol] = {
            "bids": bids,
            "asks": asks,
            "ts_ms": ts_ms,
        }

        existing["symbols"] = symbols_block
        existing["version"] = 1
        existing["updated_ms"] = now_ms

        # Optional backward-compat "books" key for any legacy code
        books_block = existing.get("books")
        if not isinstance(books_block, dict):
            books_block = {}
        books_block[symbol] = {
            "version": 1,
            "updated_ms": now_ms,
            "symbol": symbol,
            "orderbook": data,
        }
        existing["books"] = books_block

        _atomic_write_json(ORDERBOOK_BUS_PATH, existing)

        LOG.debug(
            "[PUBLIC] Updated orderbook for %s (bids=%d, asks=%d)",
            symbol,
            len(bids),
            len(asks),
        )
        return

    # Public trades → public_trades.jsonl + trades_bus.json (market_bus-compatible)
    if topic.startswith("publicTrade."):
        now_ms = _now_ms()
        symbol = topic.split(".")[-1]
        trades = msg.get("data") or []
        if isinstance(trades, dict):
            trades = [trades]

        # Append tick-level trades
        for t in trades:
            row_out = {
                "version": 1,
                "received_ms": now_ms,
                "symbol": symbol,
                "trade": t,
            }
            _append_jsonl(PUBLIC_TRADES_PATH, row_out)

        # Update trades_bus.json structure for market_bus.get_recent_trades
        existing = _load_json(TRADES_BUS_PATH)
        symbols_block = existing.get("symbols")
        if not isinstance(symbols_block, dict):
            symbols_block = {}

        sym_block = symbols_block.get(symbol)
        if not isinstance(sym_block, dict):
            sym_block = {}

        existing_trades = sym_block.get("trades")
        if not isinstance(existing_trades, list):
            existing_trades = []

        # Extend with new trades and cap size
        combined = existing_trades + [t for t in trades if isinstance(t, dict)]
        if len(combined) > TRADES_BUS_MAX_PER_SYMBOL:
            combined = combined[-TRADES_BUS_MAX_PER_SYMBOL:]

        sym_block["trades"] = combined
        symbols_block[symbol] = sym_block

        existing["symbols"] = symbols_block
        existing["version"] = 1
        existing["updated_ms"] = now_ms

        _atomic_write_json(TRADES_BUS_PATH, existing)

        LOG.debug(
            "[PUBLIC] Appended %d public trades for %s (trades_bus updated, total=%d)",
            len(trades),
            symbol,
            len(combined),
        )
        return


def _run_public_ws(
    url: str,
    symbols: List[str],
    stop_event: threading.Event,
) -> None:
    topics: List[str] = []
    for s in symbols:
        topics.append(f"orderbook.50.{s}")
        topics.append(f"publicTrade.{s}")

    sub_payload = {
        "op": "subscribe",
        "args": topics,
    }

    def on_open(ws: websocket.WebSocketApp) -> None:  # type: ignore
        LOG.info("[PUBLIC] WS connection opened, subscribing...")
        ws.send(json.dumps(sub_payload))
        LOG.info(
            "[PUBLIC] Subscribe payload sent for topics: %s",
            topics,
        )

    def on_message(ws: websocket.WebSocketApp, message: str) -> None:  # type: ignore
        try:
            msg = json.loads(message)
        except Exception:
            LOG.error("[PUBLIC] WS received non-JSON message: %s", message)
            return

        LOG.debug("[PUBLIC] WS message: %s", msg)
        _handle_public_message(msg)

    def on_error(ws: websocket.WebSocketApp, error: Any) -> None:  # type: ignore
        LOG.error("[PUBLIC] WS error: %s", error)

    def on_close(
        ws: websocket.WebSocketApp,  # type: ignore
        status_code: Any,
        msg: Any,
    ) -> None:
        LOG.warning("[PUBLIC] WS closed: code=%s msg=%s", status_code, msg)

    while not stop_event.is_set():
        try:
            ws = websocket.WebSocketApp(
                url=url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            LOG.exception("[PUBLIC] WS run_forever threw exception: %s", e)

        if stop_event.is_set():
            break

        LOG.warning("[PUBLIC] WS disconnected, retrying in 5s...")
        time.sleep(5)


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------

def _heartbeat_loop(
    account_label: str,
    heartbeat_path: Path,
    interval_sec: int,
    stop_event: threading.Event,
) -> None:
    LOG.info(
        "Starting WS heartbeat loop (interval=%ss, file=%s)",
        interval_sec,
        heartbeat_path,
    )
    while not stop_event.is_set():
        try:
            heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
            heartbeat_path.write_text(str(int(time.time())), encoding="utf-8")
        except Exception as e:
            LOG.error("Error writing heartbeat file %s: %s", heartbeat_path, e)

        for _ in range(interval_sec):
            if stop_event.is_set():
                break
            time.sleep(1)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    account_label = os.getenv("ACCOUNT_LABEL", "main")

    private_url = os.getenv(
        "BYBIT_WS_PRIVATE_URL",
        "wss://stream.bybit.com/v5/private",
    )
    public_url = os.getenv(
        "BYBIT_WS_PUBLIC_URL",
        "wss://stream.bybit.com/v5/public/linear",
    )

    symbols_env = os.getenv("WS_PUBLIC_SYMBOLS", "")
    if symbols_env.strip():
        public_symbols = [s.strip() for s in symbols_env.split(",") if s.strip()]
    else:
        public_symbols = DEFAULT_PUBLIC_SYMBOLS

    heartbeat_file = STATE_DIR / f"ws_switchboard_heartbeat_{account_label}.txt"
    heartbeat_interval = int(os.getenv("WS_HEARTBEAT_SECONDS", "20"))

    LOG.info("WS Switchboard ACCOUNT_LABEL set to: %s", account_label)
    LOG.info(
        "Starting WS Switchboard v4.3 (positions + orderbook + trades + executions, normalized, market-bus aware, multi-account safe)",
    )
    LOG.info("ACCOUNT_LABEL       : %s", account_label)
    LOG.info("PRIVATE WS endpoint : %s", private_url)
    LOG.info("PUBLIC  WS endpoint : %s", public_url)
    LOG.info("PUBLIC  WS symbols  : %s", public_symbols)
    LOG.info("HEARTBEAT file      : %s", heartbeat_file)
    LOG.info("HEARTBEAT interval  : %ss", heartbeat_interval)
    LOG.info("EXEC BUS path       : %s", EXECUTIONS_PATH)
    LOG.info("POSITIONS BUS path  : %s", POSITIONS_BUS_PATH)
    LOG.info("ORDERBOOK BUS path  : %s", ORDERBOOK_BUS_PATH)
    LOG.info("TRADES BUS path     : %s", TRADES_BUS_PATH)

    api_key, api_secret, source_env = _load_api_creds(account_label)
    if not api_key or not api_secret:
        LOG.error(
            "Missing Bybit API keys for PRIVATE WS. "
            "Tried BYBIT_MAIN_WEBSOCKET_KEY / BYBIT_MAIN_WEBSOCKET_SECRET, "
            "BYBIT_API_KEY / BYBIT_API_SECRET, BYBIT_MAIN_API_KEY / BYBIT_MAIN_API_SECRET "
            "for ACCOUNT_LABEL=%s.",
            account_label,
        )
        return

    LOG.info(
        "Resolved PRIVATE WS creds from %s for ACCOUNT_LABEL=%s",
        source_env,
        account_label,
    )

    stop_event = threading.Event()

    hb_thread = threading.Thread(
        target=_heartbeat_loop,
        name="ws_heartbeat",
        args=(account_label, heartbeat_file, heartbeat_interval, stop_event),
        daemon=True,
    )
    hb_thread.start()

    priv_thread = threading.Thread(
        target=_run_private_ws,
        name="ws_private",
        args=(private_url, account_label, api_key, api_secret, stop_event),
        daemon=True,
    )
    priv_thread.start()

    pub_thread = threading.Thread(
        target=_run_public_ws,
        name="ws_public",
        args=(public_url, public_symbols, stop_event),
        daemon=True,
    )
    pub_thread.start()

    LOG.info("[PRIVATE] WS loop started...")
    LOG.info("[PUBLIC]  WS loop started...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        LOG.info("WS Switchboard %s interrupted by user, exiting.", account_label)
    finally:
        stop_event.set()
        time.sleep(1)


if __name__ == "__main__":
    main()
