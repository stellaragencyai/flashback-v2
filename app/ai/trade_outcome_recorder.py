#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Trade Outcome Recorder v1.3 (WS executions → AI outcomes)

Purpose
-------
Stream consumer for the WS executions bus:
    state/ws_executions.jsonl

For every execution event (Bybit v5 private "execution" topic) it:
    • Normalizes the row (symbol, side, qty, price, value, fee, etc.).
    • Derives:
        - trade_id   : orderLinkId (fallback: orderId)
        - account    : inferred from:
              (a) row fields
              (b) wrapper fields (ws_switchboard)
              (c) subs mapping (sub_uid -> label)
              (d) pending_setups registry (best-effort)
        - cashflow_usd : per-fill cashflow approximation (NOT full trade PnL)
        - exit_reason  : execType / exec_type / "execution"
    • Builds OutcomeRecord via ai_events_spine.build_outcome_record(...)
    • Publishes via ai_events_spine.publish_ai_event(...)

IMPORTANT
---------
This is STILL a per-execution (per-fill) logger. The pnl_usd here is a
cashflow approximation. Useful for learning signals and debugging,
but not a final "trade PnL engine".

v1.3 Upgrade Summary
--------------------
- Fixed execId dedupe (deque maxlen auto-evict bug with set).
- Propagate wrapper fields (account_label, etc.) into yielded rows.
- Pending setups cache: TTL-first, mtime optional (Windows-safe).
- Better exec_id fallback key (execId/orderId/ts) to reduce duplicates.
"""

from __future__ import annotations

import json
import time
from collections import deque
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

try:
    from app.core.log import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging
    import sys

    def get_logger(name: str) -> "logging.Logger":  # type: ignore
        logger_ = logging.getLogger(name)
        if not logger_.handlers:
            handler = logging.StreamHandler(sys.stdout)
            fmt = logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
            handler.setFormatter(fmt)
            logger_.addHandler(handler)
        logger_.setLevel(logging.INFO)
        return logger_

log = get_logger("trade_outcome_recorder")

# ---------------------------------------------------------------------------
# Core config / paths
# ---------------------------------------------------------------------------

try:
    from app.core.config import settings
    ROOT: Path = settings.ROOT  # type: ignore[attr-defined]
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

EXEC_BUS_PATH: Path = STATE_DIR / "ws_executions.jsonl"
CURSOR_PATH: Path = STATE_DIR / "trade_outcome_recorder.cursor"

AI_EVENTS_DIR: Path = STATE_DIR / "ai_events"
AI_EVENTS_DIR.mkdir(parents=True, exist_ok=True)

PENDING_SETUPS_PATH: Path = AI_EVENTS_DIR / "pending_setups.json"

# ---------------------------------------------------------------------------
# Heartbeat + alert helpers
# ---------------------------------------------------------------------------

try:
    from app.core.flashback_common import record_heartbeat, alert_bot_error  # type: ignore
except Exception:  # pragma: no cover
    def record_heartbeat(name: str) -> None:
        return None

    def alert_bot_error(bot_name: str, msg: str, level: str = "ERROR") -> None:
        if level.upper() == "ERROR":
            log.error("[%s] %s", bot_name, msg)
        else:
            log.warning("[%s] %s", bot_name, msg)

# ---------------------------------------------------------------------------
# Subs mapping: sub_uid → account_label
# ---------------------------------------------------------------------------

_SUB_UID_TO_LABEL: Dict[str, str] = {}

try:
    from app.core.subs import all_subs as load_subs  # type: ignore
    subs_cfg = load_subs()
    for lbl, cfg in (subs_cfg or {}).items():
        uid_raw = (
            cfg.get("sub_uid")
            or cfg.get("uid")
            or cfg.get("memberId")
            or cfg.get("user_id")
            or cfg.get("subAccountId")
        )
        if uid_raw is None:
            continue
        uid = str(uid_raw).strip()
        if uid:
            _SUB_UID_TO_LABEL[uid] = str(lbl)
except Exception:
    _SUB_UID_TO_LABEL = {}

# ---------------------------------------------------------------------------
# AI events (OutcomeRecord)
# ---------------------------------------------------------------------------

try:
    from app.ai.ai_events_spine import build_outcome_record, publish_ai_event  # type: ignore
except Exception as e:  # pragma: no cover
    raise RuntimeError(f"ai_events_spine is required for trade_outcome_recorder: {e}")

# ---------------------------------------------------------------------------
# Cursor helpers
# ---------------------------------------------------------------------------

def _load_cursor() -> int:
    if not CURSOR_PATH.exists():
        return 0
    try:
        val = CURSOR_PATH.read_text(encoding="utf-8").strip()
        return int(val or "0")
    except Exception:
        return 0


def _save_cursor(pos: int) -> None:
    try:
        CURSOR_PATH.write_text(str(pos), encoding="utf-8")
    except Exception as e:
        log.warning("failed to save cursor=%s: %r", pos, e)

# ---------------------------------------------------------------------------
# Pending setups helpers (for strategy/account inference)
# ---------------------------------------------------------------------------

_PENDING_CACHE: Dict[str, Any] = {}
_PENDING_MTIME: float = 0.0
_PENDING_LAST_LOAD_MS: int = 0

def _now_ms() -> int:
    return int(time.time() * 1000)


def _load_pending_setups_cached(max_age_ms: int = 1500) -> Dict[str, Any]:
    """
    TTL-first cache (Windows-safe). mtime is used as a bonus signal only.
    """
    global _PENDING_CACHE, _PENDING_MTIME, _PENDING_LAST_LOAD_MS

    now = _now_ms()
    if (now - _PENDING_LAST_LOAD_MS) < max_age_ms and _PENDING_CACHE:
        return _PENDING_CACHE

    _PENDING_LAST_LOAD_MS = now

    if not PENDING_SETUPS_PATH.exists():
        _PENDING_CACHE = {}
        _PENDING_MTIME = 0.0
        return _PENDING_CACHE

    try:
        st = PENDING_SETUPS_PATH.stat()
        # If we have a cache AND file didn't change, we can keep it,
        # but TTL already expired so we still allow a reload attempt.
        # (mtime on Windows can be coarse; don't trust it as the only gate)
        txt = PENDING_SETUPS_PATH.read_text(encoding="utf-8")
        data = json.loads(txt or "{}")
        _PENDING_CACHE = data if isinstance(data, dict) else {}
        _PENDING_MTIME = st.st_mtime
        return _PENDING_CACHE
    except Exception:
        _PENDING_CACHE = {}
        return _PENDING_CACHE


def _setup_hint_for_trade_id(trade_id: str) -> Optional[Dict[str, Any]]:
    pending = _load_pending_setups_cached()
    hit = pending.get(str(trade_id))
    if isinstance(hit, dict):
        return hit
    return None

# ---------------------------------------------------------------------------
# Execution row normalization
# ---------------------------------------------------------------------------

def _iter_execution_rows(msg: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    """
    Handle multiple shapes:

    1) Direct Bybit push:
        {"topic":"execution","data":[{...},...], ...}

    2) ws_switchboard wrapped:
        {"ts_ms":..., "source":"PRIVATE_EXECUTION", "account_label":"...", "data":{...} or [...]}

    3) Already-flat:
        {"symbol":"BTCUSDT","execPrice":"...","execQty":"..."}
    """
    wrapper_account_label = msg.get("account_label") or msg.get("label")

    if "data" in msg:
        data = msg["data"]
        if isinstance(data, dict):
            row = dict(data)
            if wrapper_account_label and not row.get("account_label"):
                row["account_label"] = wrapper_account_label
            yield row
            return
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    row = dict(item)
                    if wrapper_account_label and not row.get("account_label"):
                        row["account_label"] = wrapper_account_label
                    yield row
            return

    if msg.get("topic") == "execution" and isinstance(msg.get("result"), dict):
        data = msg["result"].get("data") or []
        if isinstance(data, dict):
            row = dict(data)
            if wrapper_account_label and not row.get("account_label"):
                row["account_label"] = wrapper_account_label
            yield row
            return
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    row = dict(item)
                    if wrapper_account_label and not row.get("account_label"):
                        row["account_label"] = wrapper_account_label
                    yield row
            return

    if "symbol" in msg and ("execPrice" in msg or "execQty" in msg or "execValue" in msg):
        row = dict(msg)
        if wrapper_account_label and not row.get("account_label"):
            row["account_label"] = wrapper_account_label
        yield row
        return

    return


def _decimal(raw: Any, default: str = "0") -> Decimal:
    try:
        if raw in (None, "", "null"):
            return Decimal(default)
        return Decimal(str(raw))
    except Exception:
        return Decimal(default)


def _normalize_ts_ms(ts_raw: Any) -> int:
    try:
        v = int(str(ts_raw))
    except Exception:
        return _now_ms()

    if v > 0 and v < 10_000_000_000:  # likely seconds
        return v * 1000
    return v


def _compute_fill_cashflow(
    side: str,
    exec_price: Decimal,
    exec_qty: Decimal,
    exec_value: Decimal,
    exec_fee: Decimal,
) -> Decimal:
    """
    Per-fill cashflow approximation in USDT terms.

        Buy  : negative (cash out)  = -(exec_value + fee)
        Sell : positive (cash in)   =  (exec_value - fee)

    NOT full trade PnL.
    """
    s = (side or "").strip().lower()

    if exec_value <= 0 and exec_price > 0 and exec_qty > 0:
        exec_value = exec_price * exec_qty

    if s == "buy":
        return -(exec_value + exec_fee)
    if s == "sell":
        return exec_value - exec_fee

    return Decimal("0")


def _resolve_account_label(row: Dict[str, Any], trade_id: str) -> str:
    label = (
        row.get("account_label")
        or row.get("label")
        or row.get("account_label_slug")
        or ""
    )
    if label:
        return str(label)

    sub_uid_raw = (
        row.get("sub_uid")
        or row.get("subAccountId")
        or row.get("accountId")
        or row.get("subId")
        or row.get("uid")
        or row.get("memberId")
    )
    if sub_uid_raw not in (None, ""):
        uid = str(sub_uid_raw)
        return _SUB_UID_TO_LABEL.get(uid, f"sub_{uid}")

    hint = _setup_hint_for_trade_id(trade_id)
    if hint:
        hl = hint.get("account_label")
        if hl:
            return str(hl)

    return "main"


def _resolve_strategy_label(row: Dict[str, Any], trade_id: str) -> str:
    strategy_label = (
        row.get("strategy")
        or row.get("strategy_name")
        or row.get("strat_label")
        or ""
    )
    if strategy_label:
        return str(strategy_label)

    hint = _setup_hint_for_trade_id(trade_id)
    if hint:
        hs = hint.get("strategy")
        if hs:
            return str(hs)

    return "unknown_strategy"


def _resolve_symbol(row: Dict[str, Any], trade_id: str) -> Optional[str]:
    sym = row.get("symbol")
    if sym:
        return str(sym)

    hint = _setup_hint_for_trade_id(trade_id)
    if hint:
        hs = hint.get("symbol")
        if hs:
            return str(hs)
    return None


def _trade_id_from_row(row: Dict[str, Any]) -> str:
    trade_id = (
        row.get("orderLinkId")
        or row.get("order_link_id")
        or row.get("orderId")
        or row.get("order_id")
    )
    if trade_id:
        return str(trade_id)
    return f"exec_{row.get('symbol','UNKNOWN')}_{_now_ms()}"


def _exec_id_from_row(row: Dict[str, Any]) -> Optional[str]:
    # Prefer true exec id if present
    for k in ("execId", "exec_id", "executionId", "execution_id", "id"):
        v = row.get(k)
        if v not in (None, ""):
            return str(v)

    # Fallback: build a stable-ish composite key
    # (prevents duplicated processing when execId is missing)
    order_id = row.get("orderId") or row.get("order_id") or ""
    ts = row.get("execTime") or row.get("exec_time") or row.get("T") or row.get("ts") or ""
    sym = row.get("symbol") or ""
    px = row.get("execPrice") or row.get("price") or ""
    qty = row.get("execQty") or row.get("qty") or ""
    if order_id or ts:
        return f"fallback:{order_id}:{ts}:{sym}:{px}:{qty}"
    return None


def _build_outcome_from_exec_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    trade_id = _trade_id_from_row(row)
    symbol = _resolve_symbol(row, trade_id)
    if not symbol:
        return None

    side = row.get("side")
    exec_price = _decimal(row.get("execPrice") or row.get("price"))
    exec_qty = _decimal(row.get("execQty") or row.get("qty"))
    exec_value = _decimal(row.get("execValue") or row.get("value"))
    exec_fee = _decimal(row.get("execFee") or row.get("fee"))

    if exec_qty <= 0:
        return None

    account_label = _resolve_account_label(row, trade_id)
    strategy_label = _resolve_strategy_label(row, trade_id)

    cashflow = _compute_fill_cashflow(
        side=str(side or ""),
        exec_price=exec_price,
        exec_qty=exec_qty,
        exec_value=exec_value,
        exec_fee=exec_fee,
    )

    ts_exec_raw = (
        row.get("execTime")
        or row.get("exec_time")
        or row.get("T")
        or row.get("ts")
    )
    ts_exec_ms = _normalize_ts_ms(ts_exec_raw)

    exec_type_raw = (
        row.get("execType")
        or row.get("exec_type")
        or row.get("type")
        or "execution"
    )
    exec_type = f"EXEC_FILL::{exec_type_raw}"

    hint = _setup_hint_for_trade_id(trade_id) or {}
    setup_type = hint.get("setup_type")
    timeframe = hint.get("timeframe")
    ai_profile = hint.get("ai_profile")

    try:
        event = build_outcome_record(
            trade_id=str(trade_id),
            symbol=str(symbol),
            account_label=str(account_label),
            strategy=str(strategy_label),
            # Keep schema compatibility: pnl_usd is still required by your OutcomeRecord
            pnl_usd=float(cashflow),
            r_multiple=None,
            win=None,
            exit_reason=str(exec_type),
            extra={
    # Schema
    "schema_version": "outcome_from_exec_v1_4",

    # 🔐 Phase-4 lifecycle contract
    "lifecycle_stage": "EXECUTION_FILL",
    "lifecycle_role": "EXECUTION_EVENT",
    "is_final": False,
    "final_authority_expected": "trade_close_emitter",

    # Confidence & semantics
    "outcome_confidence": "LOW",
    "pnl_kind": "fill_cashflow",

    # Execution details
    "side": side,
    "exec_price": float(exec_price),
    "exec_qty": float(exec_qty),
    "exec_value": float(exec_value),
    "exec_fee": float(exec_fee),
    "cashflow_usd": float(cashflow),
    "ts_exec_ms": ts_exec_ms,

    # AI hints (non-binding)
    "setup_type_hint": setup_type,
    "timeframe_hint": timeframe,
    "ai_profile_hint": ai_profile,

    # Debug / audit
    "raw": row,
},

        )
    except Exception as e:
        log.warning("failed to build outcome_record for %s: %r", symbol, e)
        return None

    return event


# ---------------------------------------------------------------------------
# Main stream loop
# ---------------------------------------------------------------------------

# Rolling dedupe for execIds (prevents double-logging on restarts or tail weirdness)
_DEDUPE_MAX = 5000
_recent_exec_ids = set()
_recent_exec_ids_q: deque[str] = deque()  # manual eviction

def _dedupe_exec_id(exec_id: Optional[str]) -> bool:
    """
    Returns True if this exec_id is new (ok to process),
    False if it's a duplicate (skip).
    """
    if not exec_id:
        return True
    if exec_id in _recent_exec_ids:
        return False

    _recent_exec_ids.add(exec_id)
    _recent_exec_ids_q.append(exec_id)

    # Manual eviction to keep set + deque consistent
    while len(_recent_exec_ids_q) > _DEDUPE_MAX:
        old = _recent_exec_ids_q.popleft()
        _recent_exec_ids.discard(old)

    return True


def _process_bus_line(raw: bytes) -> int:
    """
    Process a single line from ws_executions.jsonl.
    Returns number of published events.
    """
    try:
        line = raw.decode("utf-8", errors="replace").strip()
    except Exception as e:
        log.warning("failed to decode bus line: %r", e)
        return 0

    if not line:
        return 0

    try:
        msg = json.loads(line)
    except Exception:
        log.warning("invalid JSON in ws_executions.jsonl: %r", line[:200])
        return 0

    published = 0
    for row in _iter_execution_rows(msg):
        exec_id = _exec_id_from_row(row)
        if not _dedupe_exec_id(exec_id):
            continue

        evt = _build_outcome_from_exec_row(row)
        if not evt:
            continue
        try:
            publish_ai_event(evt)
            published += 1
        except Exception as e:
            alert_bot_error(
                "trade_outcome_recorder",
                f"publish_ai_event failed for {row.get('symbol')}: {e}",
                "ERROR",
            )
    return published


def loop(poll_seconds: float = 0.25, cursor_flush_every: int = 50) -> None:
    """
    Main loop:
      • Tails state/ws_executions.jsonl using a cursor file.
      • Converts execution rows into OutcomeRecord events (fill cashflow).
      • Lets ai_events_spine attempt merge with pending setups.
    """
    log.info(
        "Trade Outcome Recorder starting (bus=%s, cursor=%s, poll=%.2fs, flush_every=%d)",
        EXEC_BUS_PATH,
        CURSOR_PATH,
        poll_seconds,
        cursor_flush_every,
    )

    pos = _load_cursor()
    log.info("Initial cursor position: %s", pos)

    since_flush = 0

    while True:
        record_heartbeat("trade_outcome_recorder")

        try:
            if not EXEC_BUS_PATH.exists():
                time.sleep(poll_seconds)
                continue

            size = EXEC_BUS_PATH.stat().st_size
            if pos > size:
                log.info("ws_executions.jsonl truncated (size=%s < cursor=%s), resetting to 0", size, pos)
                pos = 0
                _save_cursor(pos)

            with EXEC_BUS_PATH.open("rb") as f:
                f.seek(pos)
                for raw in f:
                    pos = f.tell()
                    _process_bus_line(raw)
                    since_flush += 1
                    if since_flush >= cursor_flush_every:
                        _save_cursor(pos)
                        since_flush = 0

            if since_flush > 0:
                _save_cursor(pos)
                since_flush = 0

            time.sleep(poll_seconds)

        except Exception as e:
            alert_bot_error("trade_outcome_recorder", f"loop error: {e}", "ERROR")
            time.sleep(1.0)


def main() -> None:
    try:
        loop()
    except KeyboardInterrupt:
        log.info("Trade Outcome Recorder stopped by user.")


if __name__ == "__main__":
    main()
