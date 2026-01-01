#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — WS Switchboard v5.2

What changed vs v5.1:
1) Self-rotating WS logs (proactive):
   - Automatically rotates:
       state/public_trades.jsonl
       state/ws_executions.jsonl
   - Prevents multi-hundred-MB bloat if health_check isn't run.
   - Policy via env:
       WS_LOG_ROTATE_ENABLED=true/false (default true)
       WS_LOG_ROTATE_WARN_MB=50
       WS_LOG_ROTATE_CAP_MB=150
       WS_LOG_ROTATE_KEEP=3
       WS_LOG_ROTATE_EVERY_SEC=30

Retains:
- Log symmetry + cleanliness:
    [PRIVATE] WS CONNECTED
    [PUBLIC] WS CONNECTED
- Strategy-driven public symbols
- Delta-safe positions merge
- Always-valid buses even when empty
- Windows-safe atomic writes
- positions_bus touch loop to avoid false stale alarms

v5.2 FIX (critical):
- Avoid circular imports: ws_switchboard must NOT import flashback_common or notifier_bot at import time.
  This file now:
    • Sends Telegram notifications directly (env-driven) without depending on notifier_bot/flashback_common.
    • Attempts to import get_equity_usdt only at runtime, inside a try/except (safe).
"""

from __future__ import annotations

import json
import os
import threading
import time
import hmac
import hashlib
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import websocket  # type: ignore
import requests

from app.core.logger import get_logger

websocket.enableTrace(False)

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

# Track whether each WS branch is connected
_ws_private_ready = False
_ws_public_ready = False
_already_notified = False


# -------------------------
# Telegram sending (NO circular imports)
# -------------------------

TG_HTTP_TIMEOUT = float(os.getenv("TG_HTTP_TIMEOUT", "6"))

def _tg_env_pair_for_label(account_label: str, main: bool = False) -> Tuple[str, str]:
    """
    Map label -> (token, chat_id).
    Uses the same env keys as notifier_bot.py.

    main/master:
      TG_TOKEN_MAIN / TG_CHAT_MAIN

    subs:
      flashback01 -> TG_TOKEN_SUB_1 / TG_CHAT_SUB_1
      ...
      flashback10 -> TG_TOKEN_SUB_10 / TG_CHAT_SUB_10
    """
    if main:
        return os.getenv("TG_TOKEN_MAIN", ""), os.getenv("TG_CHAT_MAIN", "")

    lab = (account_label or "").strip().lower()
    if lab == "main":
        return os.getenv("TG_TOKEN_MAIN", ""), os.getenv("TG_CHAT_MAIN", "")

    # flashback01..flashback10 mapping
    if lab.startswith("flashback") and len(lab) == len("flashback00"):
        suffix = lab.replace("flashback", "")
        if suffix.isdigit():
            n = int(suffix)
            if 1 <= n <= 10:
                return os.getenv(f"TG_TOKEN_SUB_{n}", ""), os.getenv(f"TG_CHAT_SUB_{n}", "")

    # fallback: main channel
    return os.getenv("TG_TOKEN_MAIN", ""), os.getenv("TG_CHAT_MAIN", "")


def _tg_send_raw(text: str, account_label: str, also_main: bool = False) -> None:
    """
    Fire-and-forget Telegram send, import-safe.
    Will NOT crash the process if Telegram flakes or env is missing.
    """
    def _send_one(token: str, chat_id: str, msg: str) -> None:
        if not token or not chat_id:
            return
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        try:
            requests.post(
                url,
                json={"chat_id": chat_id, "text": msg, "disable_web_page_preview": True},
                timeout=TG_HTTP_TIMEOUT,
            )
        except Exception as e:
            LOG.debug("[TG] send failed: %r", e)

    # per-label send
    token, chat_id = _tg_env_pair_for_label(account_label, main=False)
    _send_one(token, chat_id, text)

    # optional also send to main/master
    if also_main:
        mtoken, mchat = _tg_env_pair_for_label(account_label, main=True)
        _send_one(mtoken, mchat, text)


def _maybe_get_equity_usdt_safe() -> str:
    """
    Attempt to fetch equity using existing project helper if it can be imported safely.
    If circular import exists elsewhere, this will fail safely and return 'unknown'.
    """
    try:
        # Delayed import to avoid circulars at module import time
        from app.core.flashback_common import get_equity_usdt  # type: ignore
        bal = get_equity_usdt()
        return str(bal)
    except Exception:
        return "unknown"


def _maybe_send_online_notification(account_label: str) -> None:
    """
    Once both private & public WS streams are connected,
    send a Telegram notification via the subaccount bot (if configured),
    and optionally also via the global/master bot.
    This only runs once per process start.
    """
    global _ws_private_ready, _ws_public_ready, _already_notified

    # Only send once
    if _already_notified:
        return

    # Only when both streams are ready
    if not (_ws_private_ready and _ws_public_ready):
        return

    balance = _maybe_get_equity_usdt_safe()

    msg_sub = f"🚀 WS ONLINE — {account_label}\n💰 Balance: {balance} USDT"
    msg_main = f"📡 {account_label} is ONLINE — balance ≈ {balance} USDT"

    # Send to sub channel (or fallback main)
    _tg_send_raw(msg_sub, account_label, also_main=False)

    # Also send to master/main if desired (kept as behavior parity)
    _tg_send_raw(msg_main, account_label, also_main=True)

    _already_notified = True


# ---------------------------------------------------------------------------
# Suppress third-party websocket spam logger ("Websocket connected")
# ---------------------------------------------------------------------------
try:
    logging.getLogger("websocket").setLevel(logging.WARNING)
except Exception:
    pass

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


def _env_bool(name: str, default: str = "false") -> bool:
    raw = os.getenv(name, default)
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")


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
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}.{time.time_ns()}")

    try:
        tmp.write_text(data, encoding="utf-8")
    except Exception as e:
        try:
            path.write_text(data, encoding="utf-8")
        except Exception as e2:
            LOG.error("Error writing JSON to %s: %s / %s", path, e, e2)
        return

    last_err: Optional[Exception] = None
    for _ in range(5):
        try:
            os.replace(str(tmp), str(path))
            return
        except Exception as e:
            last_err = e
            time.sleep(0.05)

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


def _ensure_label_block(existing: Dict[str, Any], account_label: str) -> Dict[str, Any]:
    if not isinstance(existing, dict):
        existing = {}
    existing.setdefault("version", 2)
    labels = existing.get("labels")
    if not isinstance(labels, dict):
        labels = {}
    if account_label not in labels or not isinstance(labels.get(account_label), dict):
        labels[account_label] = {"category": "linear", "positions": []}
    else:
        labels[account_label].setdefault("category", "linear")
        if not isinstance(labels[account_label].get("positions"), list):
            labels[account_label]["positions"] = []
    existing["labels"] = labels
    return existing


def _touch_positions_bus_forever(interval_sec: int, account_label: str, stop_event: threading.Event) -> None:
    LOG.info("Starting positions_bus touch loop (interval=%ss)", interval_sec)

    while not stop_event.is_set():
        try:
            existing = _load_json(POSITIONS_BUS_PATH)
            existing = _ensure_label_block(existing, account_label)
            existing["updated_ms"] = _now_ms()
            _atomic_write_json(POSITIONS_BUS_PATH, existing)
        except Exception as e:
            LOG.error("positions_bus touch error: %s", e)

        for _ in range(max(1, interval_sec)):
            if stop_event.is_set():
                break
            time.sleep(1)


# -------------------------
# Log rotation (self-healing)
# -------------------------

def _file_size_mb(path: Path) -> float:
    try:
        if not path.exists():
            return 0.0
        return float(path.stat().st_size) / (1024.0 * 1024.0)
    except Exception:
        return 0.0


def _rotate_file(path: Path, keep: int) -> bool:
    """
    Rotate:
      foo.jsonl -> foo.jsonl.1
      foo.jsonl.1 -> foo.jsonl.2 ... up to keep
    Then create empty foo.jsonl.
    Returns True if rotated, False if not.
    """
    try:
        if not path.exists():
            return False

        keep = int(keep)
        if keep < 1:
            keep = 1

        # delete oldest
        oldest = path.with_name(f"{path.name}.{keep}")
        try:
            if oldest.exists():
                oldest.unlink()
        except Exception:
            pass

        # shift down
        for i in range(keep - 1, 0, -1):
            src = path.with_name(f"{path.name}.{i}")
            dst = path.with_name(f"{path.name}.{i+1}")
            if src.exists():
                try:
                    os.replace(str(src), str(dst))
                except Exception:
                    pass

        # move current to .1
        dst1 = path.with_name(f"{path.name}.1")
        try:
            os.replace(str(path), str(dst1))
        except Exception:
            # last resort: copy-then-truncate
            try:
                data = path.read_bytes()
                dst1.write_bytes(data)
                path.write_text("", encoding="utf-8")
                return True
            except Exception:
                return False

        # create new empty file
        try:
            path.write_text("", encoding="utf-8")
        except Exception:
            pass

        return True
    except Exception:
        return False


def _log_rotate_loop(stop_event: threading.Event) -> None:
    """
    Periodically checks and rotates logs to prevent runaway file sizes.
    """
    enabled = _env_bool("WS_LOG_ROTATE_ENABLED", "true")
    if not enabled:
        LOG.info("WS log rotation disabled (WS_LOG_ROTATE_ENABLED=false).")
        return

    warn_mb = float(os.getenv("WS_LOG_ROTATE_WARN_MB", "50") or "50")
    cap_mb = float(os.getenv("WS_LOG_ROTATE_CAP_MB", "150") or "150")
    keep = int(os.getenv("WS_LOG_ROTATE_KEEP", "3") or "3")
    every = int(os.getenv("WS_LOG_ROTATE_EVERY_SEC", "30") or "30")
    if every < 5:
        every = 5

    LOG.info(
        "WS log rotation enabled (warn>%.2fMB cap>%.2fMB keep=%d every=%ss)",
        warn_mb, cap_mb, keep, every
    )

    while not stop_event.is_set():
        try:
            for p in (PUBLIC_TRADES_PATH, EXECUTIONS_PATH):
                sz = _file_size_mb(p)
                if sz >= warn_mb:
                    LOG.warning("WS log size warning: %s size=%.2f MB", p.name, sz)
                if sz >= cap_mb:
                    rotated = _rotate_file(p, keep=keep)
                    if rotated:
                        LOG.warning("WS log rotated: %s (%.2f MB) -> %s.1", p.name, sz, p.name)
                    else:
                        LOG.error("WS log rotation FAILED for %s (%.2f MB)", p.name, sz)
        except Exception as e:
            LOG.error("WS log rotation loop error: %s", e)

        for _ in range(every):
            if stop_event.is_set():
                break
            time.sleep(1)


# -------------------------
# Strategy-driven public symbols
# -------------------------

def _load_public_symbols_from_strategies(account_label: str) -> List[str]:
    if not _env_bool("WS_PUBLIC_FROM_STRATEGIES", "true"):
        return []

    strat_path = ROOT / "config" / "strategies.yaml"
    if not strat_path.exists():
        return []

    try:
        import yaml  # type: ignore
    except Exception:
        LOG.warning("PyYAML not installed; cannot read strategies.yaml for public symbols. Falling back.")
        return []

    try:
        cfg = yaml.safe_load(strat_path.read_text(encoding="utf-8")) or {}
    except Exception as e:
        LOG.warning("Failed to parse strategies.yaml for public symbols: %s", e)
        return []

    subs = cfg.get("subaccounts") or []
    if not isinstance(subs, list):
        return []

    only_this = _env_bool("WS_PUBLIC_ONLY_THIS_LABEL", "false")
    include_main = _env_bool("WS_PUBLIC_INCLUDE_MAIN", "true")

    wanted_labels = {account_label}
    if include_main:
        wanted_labels.add("main")

    symbols: List[str] = []
    for s in subs:
        if not isinstance(s, dict):
            continue
        if not bool(s.get("enabled", True)):
            continue

        label = str(s.get("account_label") or "").strip()
        if only_this and label not in wanted_labels:
            continue

        sym_list = s.get("symbols") or []
        if not isinstance(sym_list, list):
            continue

        for sym in sym_list:
            sym_u = str(sym).strip().upper()
            if sym_u:
                symbols.append(sym_u)

    # de-dupe while preserving order
    seen = set()
    out: List[str] = []
    for sym in symbols:
        if sym not in seen:
            seen.add(sym)
            out.append(sym)

    sort_mode = os.getenv("WS_PUBLIC_SYMBOLS_SORT", "none").strip().lower()
    if sort_mode == "alpha":
        out = sorted(out)

    return out


def _resolve_public_symbols(account_label: str) -> List[str]:
    symbols_env = os.getenv("WS_PUBLIC_SYMBOLS", "")
    if symbols_env.strip():
        syms = [s.strip().upper() for s in symbols_env.split(",") if s.strip()]
    else:
        syms = _load_public_symbols_from_strategies(account_label) or DEFAULT_PUBLIC_SYMBOLS

    max_n = int(os.getenv("WS_PUBLIC_MAX_SYMBOLS", "50") or "50")
    if max_n <= 0:
        max_n = 50
    if len(syms) > max_n:
        syms = syms[:max_n]
    return syms


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
        if not isinstance(raw_data, list):
            raw_data = []

        existing = _load_json(POSITIONS_BUS_PATH)
        existing = _ensure_label_block(existing, account_label)

        labels = existing["labels"]
        current_positions = labels[account_label].get("positions") or []
        if not isinstance(current_positions, list):
            current_positions = []

        pos_map: Dict[str, dict] = {}
        for p in current_positions:
            if isinstance(p, dict):
                sym = str(p.get("symbol", "")).upper()
                if sym:
                    pos_map[sym] = p

        for p in raw_data:
            if not isinstance(p, dict):
                continue
            try:
                norm = normalize_position(p, account_label=account_label)
                sym = norm.get("symbol", "")
                if not sym:
                    continue
                if _safe_float(norm.get("size", 0)) <= 0:
                    pos_map.pop(sym, None)
                    continue
                pos_map[sym] = norm
            except Exception as e:
                LOG.error("[PRIVATE] Error normalizing position row %s: %s", p, e)

        labels[account_label] = {"category": "linear", "positions": list(pos_map.values())}
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
        LOG.info("[PRIVATE] WS CONNECTED")
        LOG.info("[PRIVATE] WS opened, sending auth + subscribe...")

        auth_payload = _build_ws_auth_payload(api_key, api_secret)
        ws.send(json.dumps(auth_payload))
        ws.send(json.dumps({"op": "subscribe", "args": ["position", "execution"]}))

        global _ws_private_ready
        _ws_private_ready = True
        _maybe_send_online_notification(account_label)

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


def _run_public_ws(
    url: str,
    symbols: List[str],
    stop_event: threading.Event,
    account_label: str,
) -> None:
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

        LOG.info("[PUBLIC] WS CONNECTED")
        LOG.info("[PUBLIC] WS opened, subscribing (%d topics)...", len(topics))
        ws.send(json.dumps(sub_payload))

        global _ws_public_ready
        _ws_public_ready = True
        _maybe_send_online_notification(account_label)

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

    public_symbols = _resolve_public_symbols(account_label)

    heartbeat_file = STATE_DIR / f"ws_switchboard_heartbeat_{account_label}.txt"
    heartbeat_interval = int(os.getenv("WS_HEARTBEAT_SECONDS", "20"))

    touch_interval = int(os.getenv("WS_POSITIONS_BUS_TOUCH_SEC", "5"))

    LOG.info("Starting WS Switchboard v5.2")
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

    # Heartbeat writer
    hb_thread = threading.Thread(
        target=_heartbeat_loop,
        name="ws_heartbeat",
        args=(account_label, heartbeat_file, heartbeat_interval, stop_event),
        daemon=True,
    )
    hb_thread.start()

    # Bus-touch loop to prevent false stale alarms
    touch_thread = threading.Thread(
        target=_touch_positions_bus_forever,
        name="bus_touch_positions",
        args=(touch_interval, account_label, stop_event),
        daemon=True,
    )
    touch_thread.start()

    # NEW: log rotation loop (proactive)
    rotate_thread = threading.Thread(
        target=_log_rotate_loop,
        name="ws_log_rotate",
        args=(stop_event,),
        daemon=True,
    )
    rotate_thread.start()

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
            args=(public_url, public_symbols, stop_event, account_label),
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
