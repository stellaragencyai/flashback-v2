#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Outcome Joiner / AI Outcome Logger

Purpose
-------
Read executions from WS execution bus and join them into
high-level trade outcomes for AI training:

    - Consume state/ws_executions.jsonl (written by ws_switchboard).
    - Group executions by trade_id (orderLinkId).
    - When a position is fully closed (or we decide it's "done enough"),
      emit an OutcomeRecord AI event via app.ai.ai_events:

        event_type="outcome_record"
        → state/ai_events/outcomes.jsonl

Notes
-----
This is a first-pass, simple implementation:

    - We look at each execution row.
    - We try to infer:
        * side (Buy/Sell)
        * symbol
        * realized PnL (if available)
        * orderLinkId (used as trade_id)
    - For now, we log a "partial" outcome whenever we see a fully
      filled order with a non-zero cumExecQty / cumExecValue.
    - You can refine the logic later (e.g. actual position PnL via REST).

This is *good enough* to start feeding the AI with:
    - trade_id
    - symbol
    - pnl_usd (approx)
    - win/loss flag (based on pnl > 0)
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import orjson

from app.core.config import settings

# Robust logger import
try:
    from app.core.logger import get_logger  # type: ignore
except Exception:
    try:
        from app.core.log import get_logger  # type: ignore
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

log = get_logger("outcome_joiner")

from app.ai.ai_events_spine import build_outcome_record, publish_ai_event  # noqa: E402


ROOT: Path = settings.ROOT
STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

EXEC_PATH: Path = STATE_DIR / "ws_executions.jsonl"
CURSOR_PATH: Path = STATE_DIR / "ws_executions.cursor"


def _load_cursor() -> int:
    if not CURSOR_PATH.exists():
        return 0
    try:
        return int(CURSOR_PATH.read_text().strip() or "0")
    except Exception:
        return 0


def _save_cursor(pos: int) -> None:
    try:
        CURSOR_PATH.write_text(str(pos))
    except Exception as e:
        log.warning("Failed to save executions cursor %s: %r", pos, e)


def _parse_exec_line(raw_line: bytes) -> Optional[Dict[str, Any]]:
    """
    Parse one JSONL line from ws_executions.jsonl.

    Expected format (from ws_switchboard):

        {
          "label": "<account_label_lower>",
          "ts": <epoch_ms>,
          "row": { ...raw Bybit exec row... }
        }
    """
    try:
        line = raw_line.decode("utf-8").strip()
    except Exception as e:
        log.warning("Failed to decode execution line: %r", e)
        return None

    if not line:
        return None

    try:
        obj = orjson.loads(line)
    except Exception as e:
        log.warning("Invalid JSON in ws_executions.jsonl: %r", e)
        return None

    if not isinstance(obj, dict):
        return None

    return obj


def _extract_basic_outcome(exec_obj: Dict[str, Any]) -> Optional[Tuple[str, str, str, float]]:
    """
    From one execution object, try to extract:

        trade_id  (orderLinkId)
        symbol
        account_label
        pnl_usd

    Returns:
        (trade_id, symbol, account_label, pnl_usd)
    or None if we can't infer enough.
    """
    label = str(exec_obj.get("label") or "main")
    row = exec_obj.get("row") or {}
    if not isinstance(row, dict):
        return None

    symbol = row.get("symbol")
    if not symbol:
        return None

    trade_id = (
        row.get("orderLinkId")
        or row.get("order_link_id")
        or row.get("orderLinkID")
        or row.get("orderId")
        or row.get("order_id")
    )
    if not trade_id:
        return None

    # Realized PnL is sometimes included in exec rows. Try multiple keys.
    pnl_raw = (
        row.get("execPnL")
        or row.get("realizedPnl")
        or row.get("realizedPnl")
        or 0
    )

    try:
        pnl_usd = float(pnl_raw or 0.0)
    except Exception:
        pnl_usd = 0.0

    return str(trade_id), str(symbol), label, pnl_usd


def _build_and_publish_outcome(exec_obj: Dict[str, Any]) -> None:
    extracted = _extract_basic_outcome(exec_obj)
    if not extracted:
        return

    trade_id, symbol, account_label, pnl_usd = extracted

    # Simple win flag: pnl > 0 → win, pnl < 0 → loss, 0 → None
    if pnl_usd > 0:
        win_flag: Optional[bool] = True
    elif pnl_usd < 0:
        win_flag = False
    else:
        win_flag = None

    # R-multiple is unknown for now (0 or None). You can backfill later from features.
    r_multiple = None

    # Strategy name is not directly in the exec row.
    # For now, we just label it as "unknown", and the join
    # for training can match trade_id back to setups/features.
    strategy_name = "unknown"

    # Build OutcomeRecord AI event
    ev = build_outcome_record(
        trade_id=trade_id,
        symbol=symbol,
        account_label=account_label,
        strategy=strategy_name,
        pnl_usd=pnl_usd,
        r_multiple=r_multiple,
        win=win_flag,
        exit_reason="exec_row",
        extra={
            "raw_exec": exec_obj,
        },
    )

    publish_ai_event(ev)
    log.info(
        "Logged outcome_record: trade_id=%s symbol=%s account=%s pnl_usd=%.4f win=%s",
        trade_id,
        symbol,
        account_label,
        pnl_usd,
        win_flag,
    )


def outcome_loop(poll_seconds: float = 0.5) -> None:
    """
    Main loop:

        - tail ws_executions.jsonl from last cursor
        - for each new line, parse + emit outcome_record
        - sleep briefly, repeat
    """
    pos = _load_cursor()
    log.info("Outcome joiner starting at cursor=%s", pos)

    while True:
        try:
            if not EXEC_PATH.exists():
                time.sleep(poll_seconds)
                continue

            file_size = EXEC_PATH.stat().st_size
            if pos > file_size:
                log.info(
                    "Executions file truncated (size=%s, cursor=%s). Resetting cursor to 0.",
                    file_size,
                    pos,
                )
                pos = 0
                _save_cursor(pos)

            with EXEC_PATH.open("rb") as f:
                f.seek(pos)
                for raw in f:
                    pos = f.tell()
                    exec_obj = _parse_exec_line(raw)
                    if not exec_obj:
                        continue

                    _build_and_publish_outcome(exec_obj)
                    _save_cursor(pos)

            time.sleep(poll_seconds)

        except KeyboardInterrupt:
            log.info("Outcome joiner stopped by user.")
            break
        except Exception as e:
            log.exception("Outcome loop error: %r; backing off 1s", e)
            time.sleep(1.0)


def main() -> None:
    outcome_loop()


if __name__ == "__main__":
    main()
