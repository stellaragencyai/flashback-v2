# -*- coding: utf-8 -*-
"""
Canonical Trade Outcome Contract (Outcome v1)

This schema is the single source of truth for outcome rows written to:
  state/ai_events/outcomes.v1.jsonl

Design goals:
- Required fields for joins (trade_id/symbol/timestamps)
- Required exit metrics (exit_px/exit_qty/pnl/fees)
- Fail-soft writer: validation lives here, not scattered across bots
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, List

OUTCOME_SCHEMA_VERSION = "outcome.v1"

# Required keys for a v1 outcome row
OUTCOME_REQUIRED = [
    "schema_version",
    "event_type",
    "trade_id",
    "symbol",
    "entry_side",
    "entry_qty",
    "entry_px",
    "opened_ts_ms",
    "exit_side",
    "exit_qty",
    "exit_px",
    "closed_ts_ms",
    "pnl_usd",
    "fees_usd",
]

def validate_outcome_v1(row: Dict[str, Any]) -> None:
    missing = [k for k in OUTCOME_REQUIRED if k not in row or row.get(k) is None]
    if missing:
        raise ValueError(f"outcome_missing_required={missing}")

    if str(row.get("schema_version")) != OUTCOME_SCHEMA_VERSION:
        raise ValueError(f"outcome_bad_schema_version={row.get('schema_version')!r}")

    if str(row.get("event_type")) != "trade_outcome":
        raise ValueError(f"outcome_bad_event_type={row.get('event_type')!r}")

    # Basic type sanity (fail fast, not perfect)
    for k in ("entry_qty","entry_px","exit_qty","exit_px","pnl_usd","fees_usd"):
        try:
            float(row.get(k))
        except Exception:
            raise ValueError(f"outcome_bad_number:{k}={row.get(k)!r}")

    for k in ("opened_ts_ms","closed_ts_ms"):
        try:
            int(row.get(k))
        except Exception:
            raise ValueError(f"outcome_bad_int:{k}={row.get(k)!r}")

    sym = str(row.get("symbol") or "").strip()
    if not sym:
        raise ValueError("outcome_empty_symbol")

    tid = str(row.get("trade_id") or "").strip()
    if not tid:
        raise ValueError("outcome_empty_trade_id")


# ---------------------------------------------------------------------------
# Backward-compatible alias expected by older tools
# ---------------------------------------------------------------------------
def assert_outcome_row_ok(row: Dict[str, Any]) -> None:
    """
    Compatibility shim: verify_outcome_contract.py imports this.
    Canonical validation lives in validate_outcome_v1().
    """
    validate_outcome_v1(row)
