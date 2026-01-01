# -*- coding: utf-8 -*-
"""
Outcome Writer (v1)

Writes ONLY canonical v1 outcomes to:
  state/ai_events/outcomes.v1.jsonl

This is intentionally boring. Boring is reliable.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, Optional

from app.core.config import settings
from app.ai.outcome_contract import OUTCOME_SCHEMA_VERSION, validate_outcome_v1

OUT_PATH: Path = settings.ROOT / "state" / "ai_events" / "outcomes.v1.jsonl"
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

def _append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    with path.open("ab") as f:
        f.write(json.dumps(row, ensure_ascii=False).encode("utf-8") + b"\n")

def write_outcome_v1(row: Dict[str, Any]) -> None:
    validate_outcome_v1(row)
    _append_jsonl(OUT_PATH, row)

def write_outcome_from_paper_close(
    *,
    trade_id: str,
    symbol: str,
    entry_side: str,
    entry_qty: float,
    entry_px: float,
    opened_ts_ms: int,
    exit_px: float,
    exit_qty: float,
    closed_ts_ms: int,
    pnl_usd: float,
    fees_usd: float,
    account_label: str,
    timeframe: str,
    setup_type: str,
    mode: str,
    close_reason: str,
    client_trade_id: str = "",
    source_trade_id: str = "",
) -> Dict[str, Any]:
    # Exit side is the opposite
    es = (entry_side or "").strip().lower()
    if es in ("buy", "long"):
        exit_side = "Sell"
    elif es in ("sell", "short"):
        exit_side = "Buy"
    else:
        # unknown side: still write outcome, but keep it explicit
        exit_side = "Unknown"

    row: Dict[str, Any] = {
        "schema_version": OUTCOME_SCHEMA_VERSION,
        "event_type": "trade_outcome",
        "ts_ms": int(time.time() * 1000),

        "trade_id": str(trade_id),
        "client_trade_id": (str(client_trade_id) if client_trade_id else None),
        "source_trade_id": (str(source_trade_id) if source_trade_id else None),

        "symbol": str(symbol).upper(),

        "account_label": str(account_label),
        "timeframe": str(timeframe),
        "setup_type": str(setup_type),
        "mode": str(mode),

        "entry_side": str(entry_side),
        "entry_qty": float(entry_qty),
        "entry_px": float(entry_px),
        "opened_ts_ms": int(opened_ts_ms),

        "exit_side": str(exit_side),
        "exit_qty": float(exit_qty),
        "exit_px": float(exit_px),
        "closed_ts_ms": int(closed_ts_ms),

        "pnl_usd": float(pnl_usd),
        "fees_usd": float(fees_usd),

        "close_reason": str(close_reason),
    }

    write_outcome_v1(row)
    return row
