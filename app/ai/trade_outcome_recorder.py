#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Trade Outcome Recorder v1.0 (WS executions → AI outcomes)

Purpose
-------
Stream consumer for the WS executions bus:

    state/ws_executions.jsonl

For every execution event (Bybit v5 private "execution" topic) it:

    • Normalizes the row (symbol, side, qty, price, value, fee, etc.).
    • Derives:
        - trade_id   : orderLinkId (fallback: orderId)
        - account    : account_label from subs mapping or raw fields
        - pnl_usd    : per-fill cashflow approximation
        - exit_reason: execType / exec_type / "execution"
    • Builds an OutcomeRecord via ai_events_spine.build_outcome_record(...)
    • Publishes via ai_events_spine.publish_ai_event(...) to:

        state/ai_events/outcomes_raw.jsonl (raw)
        and, via spine, optionally to outcomes.jsonl (enriched)

Notes
-----
- This is a **per-execution logger**, not yet a "fully aggregated per-trade PnL engine".
- For now, pnl_usd is treated as *realized cashflow per fill*:
      Buy  → negative cashflow (cost + fee)
      Sell → positive cashflow (value - fee)
  which is a decent first input for AI models and expectancy stats.

Later upgrade (Phase 5+):
- Add proper trade aggregation (entry set vs TP/SL ladder closure).
- Join with setup_context via trade_id for exact R-multiple, etc.
"""

from __future__ import annotations

import json
import time
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

try:
    from app.core.logger import get_logger  # type: ignore
except Exception:
    try:
        from app.core.log import get_logger as _get_logger  # type: ignore

        import logging

        def get_logger(name: str) -> "logging.Logger":  # type: ignore
            return _get_logger(name)

    except Exception:
        import logging

        def get_logger(name: str) -> "logging.Logger":  # type: ignore
            logger_ = logging.getLogger(name)
            if not logger_.handlers:
                handler = logging.StreamHandler()
                fmt = logging.Formatter(
                    "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
                )
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

EXEC_BUS_PATH: Path = ROOT / "state" / "ws_executions.jsonl"
CURSOR_PATH: Path = ROOT / "state" / "trade_outcome_recorder.cursor"

EXEC_BUS_PATH.parent.mkdir(parents=True, exist_ok=True)
CURSOR_PATH.parent.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Heartbeat + alert helpers
# ---------------------------------------------------------------------------

try:
    from app.core.flashback_common import record_heartbeat, alert_bot_error
except Exception:
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
    # Expected to return mapping like { "flashback01": {..., "sub_uid": "524630315"} }
    from app.core.subs import all_subs as load_subs  # type: ignore

    subs_cfg = load_subs()
    for lbl, cfg in subs_cfg.items():
        uid_raw = (
            cfg.get("sub_uid")
            or cfg.get("uid")
            or cfg.get("memberId")
            or cfg.get("user_id")
            or cfg.get("subAccountId")
        )
        if uid_raw is None:
            continue
        uid = str(uid_raw)
        if not uid:
            continue
        _SUB_UID_TO_LABEL[uid] = str(lbl)
except Exception:
    # If subs registry not wired yet, we just fall back to "main"/generic labels.
    _SUB_UID_TO_LABEL = {}


def _resolve_account_label(row: Dict[str, Any]) -> str:
    """
    Try hard to infer an account label for this execution row.

    Priority:
        1. Explicit account_label on row.
        2. sub_uid via subs mapping → e.g. "flashback07".
        3. Fallback: "main" for no sub_uid.
        4. Fallback: "sub_<uid>" if no known label but uid present.
    """
    label = (
        row.get("account_label")
        or row.get("label")
        or row.get("account_label_slug")
        or ""
    )

    sub_uid_raw = (
        row.get("sub_uid")
        or row.get("subAccountId")
        or row.get("accountId")
        or row.get("subId")
        or row.get("uid")
    )

    if label:
        return str(label)

    if sub_uid_raw not in (None, ""):
        uid = str(sub_uid_raw)
        return _SUB_UID_TO_LABEL.get(uid, f"sub_{uid}")

    return "main"


# ---------------------------------------------------------------------------
# AI events (OutcomeRecord)
# ---------------------------------------------------------------------------

try:
    from app.ai.ai_events_spine import build_outcome_record, publish_ai_event
except Exception as e:  # pragma: no cover
    # If this fails, the whole recorder is pointless, so shout loudly.
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
# Execution row normalization
# ---------------------------------------------------------------------------

def _iter_execution_rows(msg: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    """
    Handle a variety of shapes:

    1) Direct Bybit "execution" push:
        {
          "topic": "execution",
          "data": [ {...}, {...} ],
          "ts": ...
        }

    2) ws_switchboard-wrapped:
        {
          "ts_ms": ...,
          "source": "PRIVATE_EXECUTION",
          "data": { ... } OR [ ... ]
        }

    3) Already-flat row:
        { "symbol": "BTCUSDT", "execPrice": "...", ... }
    """
    # Case 1: explicit "data" wrapper
    if "data" in msg:
        data = msg["data"]
        if isinstance(data, dict):
            yield data
            return
        if isinstance(data, list):
            for row in data:
                if isinstance(row, dict):
                    yield row
            return

    # Case 2: Bybit v5 topic with "topic": "execution" but data missing:
    if msg.get("topic") == "execution" and isinstance(msg.get("result"), dict):
        data = msg["result"].get("data") or []
        if isinstance(data, dict):
            yield data
            return
        if isinstance(data, list):
            for row in data:
                if isinstance(row, dict):
                    yield row
            return

    # Case 3: Already looks like a single row
    # Heuristic: presence of symbol + execPrice/execQty.
    if "symbol" in msg and (
        "execPrice" in msg or "execQty" in msg or "execValue" in msg
    ):
        yield msg
        return

    # If nothing matches, we ignore.
    return []


def _decimal(raw: Any, default: str = "0") -> Decimal:
    try:
        if raw in (None, "", "null"):
            return Decimal(default)
        return Decimal(str(raw))
    except Exception:
        return Decimal(default)


def _compute_fill_cashflow(
    side: str,
    exec_price: Decimal,
    exec_qty: Decimal,
    exec_value: Decimal,
    exec_fee: Decimal,
) -> Decimal:
    """
    Very simple per-fill cashflow approximation in USDT terms.

    Bybit's execValue is usually:
        execValue = execPrice * execQty (for linear USDT perf)
    but we accept whatever they send.

    We define:
        Buy  : negative (cash out)  = -(exec_value + fee)
        Sell : positive (cash in)   =  (exec_value - fee)

    This is NOT a full "trade PnL" engine. It's a per-fill realized
    cashflow that the AI layer can still use for outcome statistics.
    """
    s = (side or "").strip().lower()
    if exec_value <= 0 and exec_price > 0 and exec_qty > 0:
        exec_value = exec_price * exec_qty

    if s in ("buy",):
        return -(exec_value + exec_fee)
    if s in ("sell",):
        return exec_value - exec_fee

    # Unknown side → treat as 0 (better than spraying garbage)
    return Decimal("0")


def _build_outcome_from_exec_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Convert one Bybit execution row → OutcomeRecord payload via ai_events_spine.

    Returns the *event dict* ready to pass to publish_ai_event(), or None if skipped.
    """
    symbol = row.get("symbol")
    if not symbol:
        return None

    side = row.get("side")
    exec_price = _decimal(row.get("execPrice") or row.get("price"))
    exec_qty = _decimal(row.get("execQty") or row.get("qty"))
    exec_value = _decimal(row.get("execValue") or row.get("value"))
    exec_fee = _decimal(row.get("execFee") or row.get("fee"))

    if exec_qty <= 0:
        return None

    trade_id = (
        row.get("orderLinkId")
        or row.get("order_link_id")
        or row.get("orderId")
        or row.get("order_id")
    )
    if not trade_id:
        # We still want some ID; worst-case: timestamp + orderId fallback
        trade_id = f"exec_{symbol}_{int(time.time() * 1000)}"

    # Account / strat inference
    account_label = _resolve_account_label(row)
    strategy_label = (
        row.get("strategy")
        or row.get("strategy_name")
        or row.get("strat_label")
        or "unknown_strategy"
    )

    # Cashflow ≈ pnl_usd at fill-level
    pnl_usd_dec = _compute_fill_cashflow(
        side=str(side or ""),
        exec_price=exec_price,
        exec_qty=exec_qty,
        exec_value=exec_value,
        exec_fee=exec_fee,
    )

    # Execution timestamp
    ts_exec_raw = (
        row.get("execTime")
        or row.get("exec_time")
        or row.get("T")  # some feeds
        or row.get("ts")
    )
    try:
        ts_exec_ms = int(ts_exec_raw)
    except Exception:
        ts_exec_ms = int(time.time() * 1000)

    exec_type = (
        row.get("execType")
        or row.get("exec_type")
        or row.get("type")
        or "execution"
    )

    # OutcomeRecord: we don't yet know R-multiple or full trade win/loss.
    try:
        event = build_outcome_record(
            trade_id=str(trade_id),
            symbol=str(symbol),
            account_label=str(account_label),
            strategy=str(strategy_label),
            pnl_usd=float(pnl_us_dec := pnl_usd_dec),
            r_multiple=None,
            win=None,
            exit_reason=str(exec_type),
            extra={
                "schema_version": "outcome_from_exec_v2",  # upgraded schema tag
                "side": side,
                "exec_price": float(exec_price),
                "exec_qty": float(exec_qty),
                "exec_value": float(exec_value),
                "exec_fee": float(exec_fee),
                "cashflow_usd": float(pnl_us_dec),  # explicit cashflow
                "ts_exec_ms": ts_exec_ms,
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

def _process_bus_line(raw: bytes) -> None:
    """
    Process a single line from ws_executions.jsonl.
    """
    try:
        line = raw.decode("utf-8").strip()
    except Exception as e:
        log.warning("failed to decode bus line: %r", e)
        return

    if not line:
        return

    try:
        msg = json.loads(line)
    except Exception:
        log.warning("invalid JSON in ws_executions.jsonl: %r", line[:200])
        return

    for row in _iter_execution_rows(msg):
        evt = _build_outcome_from_exec_row(row)
        if not evt:
            continue
        try:
            publish_ai_event(evt)
        except Exception as e:
            alert_bot_error(
                "trade_outcome_recorder",
                f"publish_ai_event failed for {row.get('symbol')}: {e}",
                "ERROR",
            )


def loop(poll_seconds: float = 0.25) -> None:
    """
    Main loop:

        • Tails state/ws_executions.jsonl using a cursor file.
        • Converts each execution event into an OutcomeRecord.
        • Writes to ai_events_spine → state/ai_events/outcomes_raw.jsonl
          and enriched outcomes.jsonl when setups exist.
    """
    log.info(
        "Trade Outcome Recorder starting (bus=%s, cursor=%s, poll=%.2fs)",
        EXEC_BUS_PATH,
        CURSOR_PATH,
        poll_seconds,
    )

    pos = _load_cursor()
    log.info("Initial cursor position: %s", pos)

    while True:
        record_heartbeat("trade_outcome_recorder")

        try:
            if not EXEC_BUS_PATH.exists():
                time.sleep(poll_seconds)
                continue

            size = EXEC_BUS_PATH.stat().st_size
            if pos > size:
                log.info(
                    "ws_executions.jsonl truncated (size=%s < cursor=%s), resetting to 0",
                    size,
                    pos,
                )
                pos = 0
                _save_cursor(pos)

            with EXEC_BUS_PATH.open("rb") as f:
                f.seek(pos)
                for raw in f:
                    pos = f.tell()
                    _process_bus_line(raw)
                    _save_cursor(pos)

            time.sleep(poll_seconds)

        except Exception as e:
            alert_bot_error(
                "trade_outcome_recorder", f"loop error: {e}", "ERROR"
            )
            time.sleep(1.0)


def main() -> None:
    try:
        loop()
    except KeyboardInterrupt:
        log.info("Trade Outcome Recorder stopped by user.")


if __name__ == "__main__":
    main()
