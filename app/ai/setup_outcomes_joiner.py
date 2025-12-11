#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Setup Outcomes Joiner (trade_id spine)

Purpose
-------
Join:
    - state/features_trades.jsonl   (entry-side features, with trade_id)
    - state/journal.jsonl           (journaled outcomes, with trade_id)

Into:
    - state/setup_outcomes.jsonl

Each output row is a merged trade, ready for final normalization by
app.ai.setup_memory:

    {
      "trade_id": "...",
      "account_label": "...",
      "strategy_name": "...",
      "symbol": "...",
      "side": "Buy" | "Sell",

      "opened_at_ms": 1733472000123,
      "closed_at_ms": 1733472600456 | null,

      "risk_pct": 0.25 | null,

      # profit metrics (best-effort, normalized later):
      "rr_realized": 2.31 | null,
      "pnl_usdt": 37.25 | null,
      "realized_rr": 2.31 | null,
      "realized_pnl": 37.25 | null,

      "outcome_label": "UNKNOWN" | (if present in journal),
      "result": "WIN" | "LOSS" | "BREAKEVEN" | "UNKNOWN",

      "features": { ... },   # snapshot at entry
      "journal": { ... }     # raw journal row
    }

Notes
-----
- trade_id is the primary join key.
- Rows without trade_id are currently skipped.
- Final label/bucket logic lives in app.ai.setup_memory.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import orjson

try:
    from app.core.config import settings
    ROOT: Path = settings.ROOT  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

FEATURES_PATH: Path = STATE_DIR / "features_trades.jsonl"
JOURNAL_PATH: Path = STATE_DIR / "journal.jsonl"
OUTPUT_PATH: Path = STATE_DIR / "setup_outcomes.jsonl"


# ----------------- I/O helpers -----------------


def _load_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        print(f"[setup_outcomes_joiner] WARNING: {path} does not exist.")
        return []
    with path.open("rb") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = orjson.loads(line)
            except Exception:
                continue
            if isinstance(row, dict):
                yield row


def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def _to_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        return None


# ----------------- normalization helpers -----------------


def _get_trade_id(row: Dict[str, Any]) -> Optional[str]:
    tid = (
        row.get("trade_id")
        or row.get("id")
        or row.get("order_link_id")
        or row.get("orderLinkId")
    )
    if not tid:
        return None
    return str(tid)


def _norm_side(x: Any) -> str:
    s = str(x or "").strip().lower()
    if s in ("buy", "long"):
        return "Buy"
    if s in ("sell", "short"):
        return "Sell"
    return s.capitalize() if s else "UNKNOWN"


def _norm_account_label(row: Dict[str, Any]) -> str:
    return (
        str(
            row.get("account_label")
            or row.get("label")
            or row.get("account")
            or row.get("account_label_slug")
            or "main"
        )
        .strip()
        or "main"
    )


def _norm_strategy_name(row: Dict[str, Any]) -> str:
    return (
        str(
            row.get("strategy_name")
            or row.get("strategy")
            or row.get("strat_name")
            or row.get("setup_type")
            or "unknown"
        )
        .strip()
        or "unknown"
    )


def _extract_open_ts(features_row: Dict[str, Any]) -> Optional[int]:
    for key in ("ts_open_ms", "ts_ms", "open_ts_ms", "ts_open", "ts"):
        if key in features_row:
            try:
                v = int(features_row[key])
                if v > 0:
                    return v
            except Exception:
                continue
    return None


def _extract_close_ts(journal_row: Dict[str, Any]) -> Optional[int]:
    for key in ("ts_close_ms", "closed_at_ms", "ts_close", "close_ts_ms"):
        if key in journal_row:
            try:
                v = int(journal_row[key])
                if v > 0:
                    return v
            except Exception:
                continue
    return None


def _extract_rr_and_pnl(journal_row: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    rr_raw = journal_row.get("realized_rr")
    if rr_raw is None:
        rr_raw = journal_row.get("rr_realized")

    pnl_raw = journal_row.get("realized_pnl")
    if pnl_raw is None:
        pnl_raw = journal_row.get("pnl_usdt")

    rr = _to_float(rr_raw)
    pnl = _to_float(pnl_raw)
    return rr, pnl


def _extract_result_label(journal_row: Dict[str, Any]) -> str:
    res = str(journal_row.get("result") or journal_row.get("label") or "").upper().strip()
    if res in ("WIN", "LOSS", "BREAKEVEN"):
        return res
    return "UNKNOWN"


# ----------------- index journal by trade_id -----------------


def _index_journal_by_trade_id(journal_rows: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for row in journal_rows:
        tid = _get_trade_id(row)
        if not tid:
            continue
        idx[tid] = row
    return idx


# ----------------- main builder -----------------


def build_setup_outcomes() -> None:
    print(f"[setup_outcomes_joiner] Loading features from {FEATURES_PATH} ...")
    features_rows = list(_load_jsonl(FEATURES_PATH))
    print(f"[setup_outcomes_joiner] Loaded {len(features_rows)} feature rows.")

    print(f"[setup_outcomes_joiner] Loading journal from {JOURNAL_PATH} ...")
    journal_rows = list(_load_jsonl(JOURNAL_PATH))
    print(f"[setup_outcomes_joiner] Loaded {len(journal_rows)} journal rows.")

    journal_idx = _index_journal_by_trade_id(journal_rows)
    print(f"[setup_outcomes_joiner] Indexed {len(journal_idx)} journal rows by trade_id.")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    matched = 0
    unmatched = 0

    with OUTPUT_PATH.open("wb") as f_out:
        for feat in features_rows:
            tid = _get_trade_id(feat)
            if not tid:
                unmatched += 1
                continue

            journal_row = journal_idx.get(tid)

            symbol = str(feat.get("symbol") or feat.get("sym") or "").upper().strip()
            side = _norm_side(feat.get("side") or (journal_row or {}).get("side"))
            account_label = _norm_account_label(
                feat if feat.get("account_label") else (journal_row or {})
            )
            strategy_name = _norm_strategy_name(
                feat if feat.get("strategy_name") else (journal_row or {})
            )

            opened_at_ms = _extract_open_ts(feat)
            closed_at_ms = _extract_close_ts(journal_row) if journal_row else None

            risk_pct = _to_float(
                feat.get("risk_pct")
                or feat.get("riskPercent")
                or (journal_row or {}).get("risk_pct")
            )

            rr_realized, pnl_usdt = (None, None)
            result_label = "UNKNOWN"
            outcome_label = "UNKNOWN"

            if journal_row is not None:
                rr_realized, pnl_usdt = _extract_rr_and_pnl(journal_row)
                result_label = _extract_result_label(journal_row)
                outcome_label = str(journal_row.get("outcome_label") or "UNKNOWN").upper().strip()
                matched += 1
            else:
                unmatched += 1

            out_row: Dict[str, Any] = {
                "trade_id": tid,
                "account_label": account_label,
                "strategy_name": strategy_name,
                "symbol": symbol,
                "side": side,
                "opened_at_ms": opened_at_ms,
                "closed_at_ms": closed_at_ms,
                "risk_pct": risk_pct,
                "rr_realized": rr_realized,
                "pnl_usdt": pnl_usdt,
                "realized_rr": rr_realized,
                "realized_pnl": pnl_usdt,
                "outcome_label": outcome_label,
                "result": result_label,
                "features": feat.get("features") or {},
                "journal": journal_row or {},
            }

            f_out.write(orjson.dumps(out_row) + b"\n")

    print(
        f"[setup_outcomes_joiner] Done. matched={matched}, unmatched={unmatched}, "
        f"output={OUTPUT_PATH}"
    )


if __name__ == "__main__":
    build_setup_outcomes()
