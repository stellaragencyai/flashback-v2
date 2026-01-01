#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Setup Memory Builder (v2.1, trade_id spine + buckets)

Purpose
-------
Consume:
  - state/setup_outcomes.jsonl

and emit:
  - state/setup_memory.jsonl

Each input row (from outcome_joiner) is a merged trade:
  {
    "trade_id": "...",
    "account_label": "...",
    "strategy_name": "...",
    "symbol": "...",
    "side": "Buy" | "Sell",
    "opened_at_ms": 1733472000123,
    "closed_at_ms": 1733472600456 | null,
    "risk_pct": 0.25 | null,

    # profitability metrics (names may vary slightly, we support both):
    "rr_realized": 2.31,        # preferred
    "pnl_usdt": 37.25,          # preferred
    # or legacy:
    "realized_rr": 2.31,
    "realized_pnl": 37.25,

    "outcome_label": "WIN_STRONG",
    "result": "WIN",
    "features": { ... },        # snapshot at entry
    "journal": { ... }          # full journal row as written by trade_journal.py
  }

We normalize that into a training-friendly record with explicit labels:

  {
    "trade_id": "...",
    "account_label": "...",
    "strategy_name": "...",
    "symbol": "...",
    "side": "Buy" | "Sell",
    "mode": "LIVE_CANARY" | "LIVE_FULL" | "PAPER" | "UNKNOWN",

    "ts_open_ms": int,
    "ts_close_ms": int | null,
    "duration_ms": int | null,
    "duration_human": str | null,

    "rr_realized": float | null,
    "pnl_usdt": float | null,
    "result": "WIN" | "LOSS" | "BREAKEVEN" | "UNKNOWN",
    "outcome_bucket": "WIN_STRONG" | "WIN_WEAK" | "SCRATCH" | "LOSS" | "UNKNOWN",

    "rating_score": int | null,
    "rating_reason": str | null,

    "risk_pct": float | null,

    "features": {...},

    # labels for training
    "label_win": bool,
    "label_good": bool,          # rating_score >= 7
    "label_rr_ge_1": bool,       # rr_realized >= 1.0
    "label_win_strong": bool,    # outcome_bucket == WIN_STRONG
    "label_win_weak": bool,      # outcome_bucket == WIN_WEAK
    "label_scratch": bool,       # outcome_bucket == SCRATCH
    "label_loss": bool           # outcome_bucket == LOSS
  }

Notes
-----
- setup_memory no longer performs any joining logic.
- The join responsibility lives in app.ai.outcome_joiner (trade_id-based).
- This script is just the "final polish + labels" step.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import orjson

try:
    from app.core.config import settings
except ImportError:
    from core.config import settings  # type: ignore

ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])
STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

OUTCOMES_PATH: Path = STATE_DIR / "setup_outcomes.jsonl"
OUTPUT_PATH: Path = STATE_DIR / "setup_memory.jsonl"


# ----------------- utils -----------------


def _load_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        print(f"[setup_memory] WARNING: {path} does not exist; nothing to build.")
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


# ----------------- label helpers -----------------


def _normalize_result(raw: Any) -> str:
    res = str(raw or "").upper().strip()
    if res in ("WIN", "LOSS", "BREAKEVEN", "BREAK_EVEN"):
        if res == "BREAK_EVEN":
            return "BREAKEVEN"
        return res
    return "UNKNOWN"


def _compute_bucket(rr: Optional[float]) -> str:
    """
    Simple RR-based bucket:

      - rr is None      -> UNKNOWN
      - rr <= -0.25     -> LOSS
      - |rr| < 0.25     -> SCRATCH
      - 0.25 <= rr < 2  -> WIN_WEAK
      - rr >= 2         -> WIN_STRONG
    """
    if rr is None:
        return "UNKNOWN"

    if rr <= -0.25:
        return "LOSS"

    if abs(rr) < 0.25:
        return "SCRATCH"

    if rr < 2.0:
        return "WIN_WEAK"

    return "WIN_STRONG"


def _extract_result_and_labels(src: Dict[str, Any]) -> Dict[str, Any]:
    """
    src is either the main combined row or the inner journal row.
    We expect keys (some are optional / legacy):
      - result           (WIN/LOSS/BREAKEVEN/UNKNOWN)
      - realized_rr OR rr_realized
      - realized_pnl OR pnl_usdt
      - rating_score
      - rating_reason
      - duration_ms
      - duration_human
    """
    result = _normalize_result(src.get("result"))

    # RR: support both "realized_rr" and "rr_realized"
    rr_raw = src.get("realized_rr")
    if rr_raw is None:
        rr_raw = src.get("rr_realized")

    # PnL: support both "realized_pnl" and "pnl_usdt"
    pnl_raw = src.get("realized_pnl")
    if pnl_raw is None:
        pnl_raw = src.get("pnl_usdt")

    rr = _to_float(rr_raw)
    pnl = _to_float(pnl_raw)

    try:
        rating_score = int(src.get("rating_score")) if src.get("rating_score") is not None else None
    except Exception:
        rating_score = None

    # Labels
    label_win = result == "WIN"
    label_good = rating_score is not None and rating_score >= 7
    label_rr_ge_1 = rr is not None and rr >= 1.0

    bucket = _compute_bucket(rr)

    label_win_strong = bucket == "WIN_STRONG"
    label_win_weak = bucket == "WIN_WEAK"
    label_scratch = bucket == "SCRATCH"
    label_loss = bucket == "LOSS"

    return {
        "result": result,
        "rr_realized": rr,
        "pnl_usdt": pnl,
        "rating_score": rating_score,
        "rating_reason": src.get("rating_reason"),
        "duration_ms": _to_int(src.get("duration_ms")),
        "duration_human": src.get("duration_human"),
        "outcome_bucket": bucket,
        "label_win": label_win,
        "label_good": label_good,
        "label_rr_ge_1": label_rr_ge_1,
        "label_win_strong": label_win_strong,
        "label_win_weak": label_win_weak,
        "label_scratch": label_scratch,
        "label_loss": label_loss,
    }


def _normalize_mode(row: Dict[str, Any], journal: Dict[str, Any]) -> str:
    """
    Try to infer the execution mode.
    We check, in order:
      - row["mode"]
      - journal["mode"]
      - fallback to "UNKNOWN"
    """
    mode = row.get("mode") or journal.get("mode") or ""
    mode = str(mode).upper().strip()
    if mode in ("PAPER", "LIVE_CANARY", "LIVE_FULL"):
        return mode
    return "UNKNOWN"


# ----------------- transform -----------------


def _transform_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Take one setup_outcomes row and turn it into a normalized
    setup_memory training row.
    """
    trade_id = row.get("trade_id")
    symbol = str(row.get("symbol") or "").upper().strip()
    account_label = row.get("account_label") or row.get("account") or "main"
    strategy_name = row.get("strategy_name") or row.get("strategy") or "unknown"
    side = row.get("side") or "UNKNOWN"

    opened_at_ms = _to_int(row.get("opened_at_ms") or row.get("ts_open_ms"))
    closed_at_ms = _to_int(row.get("closed_at_ms") or row.get("ts_close"))

    risk_pct = _to_float(row.get("risk_pct"))

    features = row.get("features") or {}
    journal = row.get("journal") or {}

    # Prefer journal's outcome fields if present
    outcome_src = journal if "result" in journal else row
    labels = _extract_result_and_labels(outcome_src)

    mode = _normalize_mode(row, journal)

    return {
        "trade_id": trade_id,
        "account_label": account_label,
        "strategy_name": strategy_name,
        "symbol": symbol,
        "side": side,
        "mode": mode,

        "ts_open_ms": opened_at_ms,
        "ts_close_ms": closed_at_ms,
        "duration_ms": labels["duration_ms"],
        "duration_human": labels["duration_human"],

        "rr_realized": labels["rr_realized"],
        "pnl_usdt": labels["pnl_usdt"],
        "result": labels["result"],
        "outcome_bucket": labels["outcome_bucket"],
        "rating_score": labels["rating_score"],
        "rating_reason": labels["rating_reason"],

        "risk_pct": risk_pct,

        "features": features,

        "label_win": labels["label_win"],
        "label_good": labels["label_good"],
        "label_rr_ge_1": labels["label_rr_ge_1"],
        "label_win_strong": labels["label_win_strong"],
        "label_win_weak": labels["label_win_weak"],
        "label_scratch": labels["label_scratch"],
        "label_loss": labels["label_loss"],
    }


# ----------------- main builder -----------------


def build_memory() -> None:
    print(f"[setup_memory] Loading outcomes from {OUTCOMES_PATH} ...")
    rows = list(_load_jsonl(OUTCOMES_PATH))
    print(f"[setup_memory] Loaded {len(rows)} merged outcome rows.")

    transformed: List[Dict[str, Any]] = []
    for r in rows:
        try:
            transformed.append(_transform_row(r))
        except Exception as e:
            # Don't let one bad row poison the batch
            print(f"[setup_memory] ERROR transforming row with trade_id={r.get('trade_id')}: {e}")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("wb") as f:
        for row in transformed:
            f.write(orjson.dumps(row) + b"\n")

    print(f"[setup_memory] Wrote {len(transformed)} rows -> {OUTPUT_PATH}")
    print("[setup_memory] Done.")


if __name__ == "__main__":
    build_memory()
