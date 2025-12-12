#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — WS Switchboard v4.8 (multi-account safe: public/private toggles + windows-safe writes + positions freshness)

Key fixes:
1) Windows file-lock collisions (WinError 5 / Errno 13):
   - Uses unique temp filenames and retries os.replace().

2) positions_bus stale when there are NO position events:
   - Adds a "touch" loop that updates positions_bus.updated_ms periodically, even if empty.
   - This makes health checks meaningful: WS alive != positions exist.

New env toggles:
- WS_ENABLE_PRIVATE=true/false  (default true)
- WS_ENABLE_PUBLIC=true/false   (default true)

Recommended:
- MAIN: WS_ENABLE_PRIVATE=true,  WS_ENABLE_PUBLIC=true
- SUBS: WS_ENABLE_PRIVATE=true,  WS_ENABLE_PUBLIC=false

New env:
- WS_POSITIONS_BUS_TOUCH_SEC=5  (default 5)  # keep positions_bus fresh
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
websocket.enableTrace(False)

from app.core.logger import get_logger

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
    pass

LOG = get_logger("ws_switchboard")

STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

POSITIONS_BUS_PATH: Path = STATE_DIR / "positions_bus.json"
ORDERBOOK_BUS_PATH: Path = STATE_DIR / "orderbook_bus.json"
TRADES_BUS_PATH: Path = STATE_DIR / "trades_bus.json"
PUBLIC_TRADES_PATH: Path = STATE_DIR / "public_trades.jsonl"
EXECUTIONS_PATH: Path = STATE_DIR / "ws_executions.jsonl"

TRADES_BUS_MAX_PER_SYMBOL: int = int(os.getenv("TRADES_BUS_MAX_PER_SYMBOL", "200"))

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

WS_PING_INTERVAL_SEC: int = int(os.getenv("WS_PING_INTERVAL_SEC", "20"))
WS_PING_TIMEOUT_SEC: int = int(os.getenv("WS_PING_TIMEOUT_SEC", "10"))

WS_RECONNECT_MIN_SEC: float = float(os.getenv("WS_RECONNECT_MIN_SEC", "3"))
WS_RECONNECT_MAX_SEC: float = float(os.getenv("WS_RECONNECT_MAX_SEC", "30"))

WS_DEBUG_ORDERBOOK: bool = os.getenv("WS_DEBUG_ORDERBOOK", "false").strip().lower() in ("1", "true", "yes", "y")
WS_DEBUG_ORDERBOOK_EVERY: int = int(os.getenv("WS_DEBUG_ORDERBOOK_EVERY", "200"))
_WS_ORDERBOOK_SEEN: int = 0


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def normalize_position(raw: dict, account_label: str) -> dict:
    symbol = str(raw.get("symbol", "")).upper()
    side = str(raw.get("side", "")).title()

    size = _safe_float(raw.get("size", 0))
    avg = _safe_float(raw.get("avgPrice", raw.get("entryPrice", 0)))

    sl_raw = raw.get("stopLoss") or raw.get("stopLossPrice") or raw.get("slPrice") or 0
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


# -------------------------
# Windows-safe atomic write
# -------------------------

def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    """
    Windows-safe best-effort atomic JSON write.

    Fixes:
      - Unique temp name (pid + time_ns) avoids collisions across processes
      - Retries os.replace (Windows can transiently deny)
      - Falls back to direct write if replace keeps failing
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)

    tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}.{time.time_ns()}")

    # Write temp
    try:
        tmp.write_text(data, encoding="utf-8")
    except Exception as e:
        # fallback direct
        try:
            path.write_text(data, encoding="utf-8")
        except Exception as e2:
            LOG.error("Error writing JSON to %s: %s / %s", path, e, e2)
        return

    # Replace with retry
    last_err: Optional[Exception] = None
    for _ in range(5):
        try:
            os.replace(str(tmp), str(path))
            return
        except Exception as e:
            last_err = e
            time.sleep(0.05)

    # Replace failed: fallback direct write
    try:
        path.write_text(data, encoding="utf-8")
    except Exception as e2:
        LOG.error("Error writing JSON to %s: %s / %s", path, last_err, e2)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass


def _append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, separators=(",", ":"), ensure_ascii=False) + "\n")


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _ensure_bus_files_exist() -> None:
    try:
        if not ORDERBOOK_BUS_PATH.exists():
            _atomic_write_json(ORDERBOOK_BUS_PATH, {"version": 1, "updated_ms": 0, "symbols": {}})
        if not TRADES_BUS_PATH.exists():
            _atomic_write_json(TRADES_BUS_PATH, {"version": 1, "updated_ms": 0, "symbols": {}})
        if not POSITIONS_BUS_PATH.exists():
            _atomic_write_json(POSITIONS_BUS_PATH, {"version": 2, "updated_ms": 0, "labels": {}})
    except Exception as e:
        LOG.error("Failed ensuring bus files exist: %s", e)


def _touch_positions_bus_forever(interval_sec: int, stop_event: threading.Event) -> None:
    """
    Keep positions_bus.json 'fresh' even when there are no position events.
    Prevents false stale alarms when WS is alive but positions are empty.
    """
    LOG.info("Starting positions_bus touch loop (interval=%ss)", interval_sec)

    while not stop_event.is_set():
        try:
            existing = _load_json(POSITIONS_BUS_PATH)
            if not isinstance(existing, dict):
                existing = {}

            existing.setdefault("version", 2)
            labels = existing.get("labels")
            if not isinstance(labels, dict):
                existing["labels"] = {}

            existing["updated_ms"] = _now_ms()
            _atomic_write_json(POSITIONS_BUS_PATH, existing)

        except Exception as e:
            LOG.error("positions_bus touch error: %s", e)

        for _ in range(max(1, interval_sec)):
            if stop_event.is_set():
                break
            time.sleep(1)


# -------------------------
# API creds + auth signing
# -------------------------

def _load_api_creds(account_label: str) -> Tuple[Optional[str], Optional[str], str]:
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
        candidates.append((f"BYBIT_{label_upper}_API_KEY", f"BYBIT_{label_upper}_API_SECRET"))
        candidates.append(("BYBIT_API_KEY", "BYBIT_API_SECRET"))

    for key_env, sec_env in candidates:
        key = os.getenv(key_env)
        sec = os.getenv(sec_env)
        if key and sec:
            LOG.info("[PRIVATE] Using Bybit API creds from env: %s / %s", key_env, sec_env)
            return key, sec, key_env

    return None, None, ""


def _build_ws_auth_payload(api_key: str, api_secret: str) -> Dict[str, Any]:
    expires_ms = _now_ms() + 60_000
    expires_str = str(expires_ms)

    signed_string = f"GET/realtime{expires_str}"
    signature = hmac.new(
        api_secret.encode("utf-8"),
        signed_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    return {"op": "auth", "args": [api_key, expires_str, signature]}


# -------------------------
# PRIVATE WS
# -------------------------

def _handle_private_message(msg: Dict[str, Any], account_label: str) -> None:
    if msg.get("op") == "auth":
        success = msg.get("success")
        ret_msg = msg.get("ret_msg")
        if not success:
            LOG.error("[PRIVATE] Auth FAILED: %s (raw=%s)", ret_msg, msg)
        else:
            LOG.info("[PRIVATE] Auth success: %s", ret_msg)
        return

    if msg.get("op") == "subscribe":
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

        norm_positions: List[dict] = []
        for p in raw_data:
            if not isinstance(p, dict):
                continue
            try:
                norm = normalize_position(p, account_label=account_label)
                if not norm["symbol"]:
                    continue
                if norm["size"] <= 0:
                    continue
                norm_positions.append(norm)
            except Exception as e:
                LOG.error("[PRIVATE] Error normalizing position row %s: %s", p, e)

        existing = _load_json(POSITIONS_BUS_PATH)
        labels = existing.get("labels")
        if not isinstance(labels, dict):
            labels = {}

        labels[account_label] = {"category": "linear", "positions": norm_positions}
        existing["labels"] = labels
        existing["version"] = 2
        existing["updated_ms"] = now_ms

        _atomic_write_json(POSITIONS_BUS_PATH, existing)
        return

    if topic == "execution":
        now_ms = _now_ms()
        data = msg.get("data") or []
        if isinstance(data, dict):
            data = [data]
        if not isinstance(data, list):
            return

        line = {
            "version": 1,
            "topic": "execution",
            "ts_ms": now_ms,
            "account_label": account_label,
            "data": [row for row in data if isinstance(row, dict)],
        }
        _append_jsonl(EXECUTIONS_PATH, line)
        return


def _run_private_ws(
    url: str,
    account_label: str,
    api_key: str,
    api_secret: str,
    stop_event: threading.Event,
) -> None:
    backoff = WS_RECONNECT_MIN_SEC

    def on_open(ws: websocket.WebSocketApp) -> None:  # type: ignore
        nonlocal backoff
        backoff = WS_RECONNECT_MIN_SEC
        LOG.info("[PRIVATE] WS opened, sending auth + subscribe...")

        auth_payload = _build_ws_auth_payload(api_key, api_secret)
        ws.send(json.dumps(auth_payload))
        ws.send(json.dumps({"op": "subscribe", "args": ["position", "execution"]}))

    def on_message(ws: websocket.WebSocketApp, message: str) -> None:  # type: ignore
        try:
            msg = json.loads(message)
        except Exception:
            LOG.error("[PRIVATE] WS received non-JSON message: %s", message)
            return
        _handle_private_message(msg, account_label)

    def on_error(ws: websocket.WebSocketApp, error: Any) -> None:  # type: ignore
        LOG.error("[PRIVATE] WS error: %s", str(error))

    def on_close(ws: websocket.WebSocketApp, status_code: Any, msg: Any) -> None:  # type: ignore
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
            ws.run_forever(
                ping_interval=WS_PING_INTERVAL_SEC,
                ping_timeout=WS_PING_TIMEOUT_SEC,
                reconnect=0,
            )
        except Exception as e:
            LOG.exception("[PRIVATE] WS run_forever threw exception: %s", e)

        if stop_event.is_set():
            break

        LOG.warning("[PRIVATE] WS disconnected, retrying in %.1fs...", backoff)
        time.sleep(backoff)
        backoff = min(backoff * 1.6, WS_RECONNECT_MAX_SEC)


# -------------------------
# PUBLIC WS
# -------------------------

def _handle_public_message(msg: Dict[str, Any]) -> None:
    global _WS_ORDERBOOK_SEEN

    topic = msg.get("topic")
    if not topic:
        return

    if topic.startswith("orderbook."):
        _WS_ORDERBOOK_SEEN += 1
        if WS_DEBUG_ORDERBOOK and (_WS_ORDERBOOK_SEEN % max(1, WS_DEBUG_ORDERBOOK_EVERY) == 0):
            try:
                LOG.info("[PUBLIC][DBG] orderbook msg sample=%s", str(msg)[:600])
            except Exception:
                pass

        now_ms = _now_ms()
        data = msg.get("data") or {}
        if not isinstance(data, dict):
            data = {}

        symbol_raw = data.get("s") or topic.split(".")[-1]
        symbol = str(symbol_raw).upper()

        bids = data.get("b") or []
        asks = data.get("a") or []
        if not isinstance(bids, list):
            bids = []
        if not isinstance(asks, list):
            asks = []

        ts_raw = data.get("ts") or msg.get("ts") or now_ms
        try:
            ts_ms = int(ts_raw)
        except Exception:
            ts_ms = now_ms

        existing = _load_json(ORDERBOOK_BUS_PATH)
        symbols_block = existing.get("symbols")
        if not isinstance(symbols_block, dict):
            symbols_block = {}

        symbols_block[symbol] = {"bids": bids, "asks": asks, "ts_ms": ts_ms}
        existing["symbols"] = symbols_block
        existing["version"] = 1
        existing["updated_ms"] = now_ms

        _atomic_write_json(ORDERBOOK_BUS_PATH, existing)
        return

    if topic.startswith("publicTrade."):
        now_ms = _now_ms()
        symbol = str(topic.split(".")[-1]).upper()

        trades = msg.get("data") or []
        if isinstance(trades, dict):
            trades = [trades]
        if not isinstance(trades, list):
            return

        clean_trades: List[Dict[str, Any]] = []
        for t in trades:
            if not isinstance(t, dict):
                continue
            clean_trades.append(t)
            _append_jsonl(PUBLIC_TRADES_PATH, {"version": 1, "received_ms": now_ms, "symbol": symbol, "trade": t})

        if not clean_trades:
            return

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

        combined = existing_trades + clean_trades
        if len(combined) > TRADES_BUS_MAX_PER_SYMBOL:
            combined = combined[-TRADES_BUS_MAX_PER_SYMBOL:]

        sym_block["trades"] = combined
        symbols_block[symbol] = sym_block

        existing["symbols"] = symbols_block
        existing["version"] = 1
        existing["updated_ms"] = now_ms

        _atomic_write_json(TRADES_BUS_PATH, existing)
        return


def _run_public_ws(url: str, symbols: List[str], stop_event: threading.Event) -> None:
    symbols_clean = [str(s).strip().upper() for s in symbols if str(s).strip()]
    topics: List[str] = []
    for s in symbols_clean:
        topics.append(f"orderbook.50.{s}")
        topics.append(f"publicTrade.{s}")

    sub_payload = {"op": "subscribe", "args": topics}
    backoff = WS_RECONNECT_MIN_SEC

    def on_open(ws: websocket.WebSocketApp) -> None:  # type: ignore
        nonlocal backoff
        backoff = WS_RECONNECT_MIN_SEC
        LOG.info("[PUBLIC] WS opened, subscribing (%d topics)...", len(topics))
        ws.send(json.dumps(sub_payload))

    def on_message(ws: websocket.WebSocketApp, message: str) -> None:  # type: ignore
        try:
            msg = json.loads(message)
        except Exception:
            LOG.error("[PUBLIC] WS received non-JSON message: %s", message)
            return
        _handle_public_message(msg)

    def on_error(ws: websocket.WebSocketApp, error: Any) -> None:  # type: ignore
        LOG.error("[PUBLIC] WS error: %s", str(error))

    def on_close(ws: websocket.WebSocketApp, status_code: Any, msg: Any) -> None:  # type: ignore
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
            ws.run_forever(
                ping_interval=WS_PING_INTERVAL_SEC,
                ping_timeout=WS_PING_TIMEOUT_SEC,
                reconnect=0,
            )
        except Exception as e:
            LOG.exception("[PUBLIC] WS run_forever threw exception: %s", e)

        if stop_event.is_set():
            break

        LOG.warning("[PUBLIC] WS disconnected, retrying in %.1fs...", backoff)
        time.sleep(backoff)
        backoff = min(backoff * 1.6, WS_RECONNECT_MAX_SEC)


# -------------------------
# Heartbeat
# -------------------------

def _heartbeat_loop(account_label: str, heartbeat_path: Path, interval_sec: int, stop_event: threading.Event) -> None:
    LOG.info("Starting WS heartbeat loop (interval=%ss, file=%s)", interval_sec, heartbeat_path)
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


# -------------------------
# Main
# -------------------------

def main() -> None:
    account_label = os.getenv("ACCOUNT_LABEL", "main").strip() or "main"

    enable_private = os.getenv("WS_ENABLE_PRIVATE", "true").strip().lower() in ("1", "true", "yes", "y")
    enable_public = os.getenv("WS_ENABLE_PUBLIC", "true").strip().lower() in ("1", "true", "yes", "y")

    private_url = os.getenv("BYBIT_WS_PRIVATE_URL", "wss://stream.bybit.com/v5/private")
    public_url = os.getenv("BYBIT_WS_PUBLIC_URL", "wss://stream.bybit.com/v5/public/linear")

    symbols_env = os.getenv("WS_PUBLIC_SYMBOLS", "")
    if symbols_env.strip():
        public_symbols = [s.strip().upper() for s in symbols_env.split(",") if s.strip()]
    else:
        public_symbols = DEFAULT_PUBLIC_SYMBOLS

    heartbeat_file = STATE_DIR / f"ws_switchboard_heartbeat_{account_label}.txt"
    heartbeat_interval = int(os.getenv("WS_HEARTBEAT_SECONDS", "20"))

    touch_interval = int(os.getenv("WS_POSITIONS_BUS_TOUCH_SEC", "5"))

    LOG.info("Starting WS Switchboard v4.8")
    LOG.info("ACCOUNT_LABEL        : %s", account_label)
    LOG.info("WS_ENABLE_PRIVATE    : %s", enable_private)
    LOG.info("WS_ENABLE_PUBLIC     : %s", enable_public)
    LOG.info("PRIVATE WS endpoint  : %s", private_url)
    LOG.info("PUBLIC  WS endpoint  : %s", public_url)
    LOG.info("PUBLIC  WS symbols   : %s", public_symbols)
    LOG.info("HEARTBEAT file       : %s", heartbeat_file)
    LOG.info("HEARTBEAT interval   : %ss", heartbeat_interval)
    LOG.info("POSITIONS touch sec  : %ss", touch_interval)
    LOG.info("EXEC BUS path        : %s", EXECUTIONS_PATH)
    LOG.info("POSITIONS BUS path   : %s", POSITIONS_BUS_PATH)
    LOG.info("ORDERBOOK BUS path   : %s", ORDERBOOK_BUS_PATH)
    LOG.info("TRADES BUS path      : %s", TRADES_BUS_PATH)

    _ensure_bus_files_exist()

    stop_event = threading.Event()

    hb_thread = threading.Thread(
        target=_heartbeat_loop,
        name="ws_heartbeat",
        args=(account_label, heartbeat_file, heartbeat_interval, stop_event),
        daemon=True,
    )
    hb_thread.start()

    # Keep positions_bus fresh even when no positions are open
    touch_thread = threading.Thread(
        target=_touch_positions_bus_forever,
        name="bus_touch_positions",
        args=(touch_interval, stop_event),
        daemon=True,
    )
    touch_thread.start()

    priv_thread = None
    if enable_private:
        api_key, api_secret, source_env = _load_api_creds(account_label)
        if not api_key or not api_secret:
            LOG.error(
                "Missing Bybit API keys for PRIVATE WS for ACCOUNT_LABEL=%s. "
                "Tried BYBIT_MAIN_WEBSOCKET_KEY/SECRET, BYBIT_API_KEY/SECRET, BYBIT_MAIN_API_KEY/SECRET (main) "
                "or BYBIT_<LABEL>_API_KEY/SECRET (subs).",
                account_label,
            )
        else:
            LOG.info("Resolved PRIVATE WS creds from %s for ACCOUNT_LABEL=%s", source_env, account_label)
            priv_thread = threading.Thread(
                target=_run_private_ws,
                name="ws_private",
                args=(private_url, account_label, api_key, api_secret, stop_event),
                daemon=True,
            )
            priv_thread.start()

    pub_thread = None
    if enable_public:
        pub_thread = threading.Thread(
            target=_run_public_ws,
            name="ws_public",
            args=(public_url, public_symbols, stop_event),
            daemon=True,
        )
        pub_thread.start()

    LOG.info("WS threads started. (private=%s, public=%s)", bool(priv_thread), bool(pub_thread))

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
