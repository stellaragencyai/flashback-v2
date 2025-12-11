#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Training Set Builder (features + outcomes)

Purpose
-------
Join:

    state/feature_store.jsonl        (per-trade features, from feature_builder)
    state/ai_events/outcomes.jsonl   (per-trade outcomes, from ai_events spine
                                      or outcome logger)

into a single, training-ready JSONL file:

    state/ai_training_set.jsonl

Each output row is:

    {
      "trade_id": "...",
      "symbol": "BTCUSDT",
      "strategy": "Sub1_Trend",
      "account_label": "flashback01" | "main" | null,
      "mode": "PAPER" | "LIVE_CANARY" | "LIVE_FULL" | "UNKNOWN",

      "label_win": 1 | 0,          # binary target (win vs loss)
      "target_pnl_usd": float | null,
      "target_rr": float | null,

      # all feature fields from feature_store kept as-is
      ...
    }

Notes
-----
- We only keep rows that have BOTH:
    • a feature row in feature_store.jsonl
    • a matching outcome in ai_events/outcomes.jsonl

- label_win:
    • 1 if pnl_usd > 0 (or rr > 0 when pnl_usd missing)
    • 0 if pnl_usd < 0 (or rr < 0 when pnl_usd missing)
    • rows with flat/zero or missing signal are skipped (no clear label)

Usage
-----
From project root:

    python -m app.tools.ai_training_builder

Optionally focus on one strategy:

    python -m app.tools.ai_training_builder "Sub1_Trend"

This filters by exact strategy name in feature_store (strategy_name field).
"""

from __future__ import annotations

import math
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# orjson preferred, fallback to stdlib json
try:
    import orjson  # type: ignore

    def _loads(b: bytes) -> Any:
        return orjson.loads(b)

    def _dumps(obj: Any) -> bytes:
        return orjson.dumps(obj)

except Exception:  # pragma: no cover
    import json as _json  # type: ignore

    def _loads(b: bytes) -> Any:  # type: ignore
        if isinstance(b, (bytes, bytearray)):
            b = b.decode("utf-8")
        return _json.loads(b)

    def _dumps(obj: Any) -> bytes:  # type: ignore
        return _json.dumps(obj, separators=(",", ":")).encode("utf-8")


# ---------------------------------------------------------------------------
# ROOT + paths
# ---------------------------------------------------------------------------

try:
    from app.core.config import settings  # type: ignore

    ROOT: Path = settings.ROOT  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

STATE_DIR = ROOT / "state"
AI_EVENTS_DIR = STATE_DIR / "ai_events"

FEATURE_STORE_PATH = STATE_DIR / "feature_store.jsonl"
OUTCOMES_PATH = AI_EVENTS_DIR / "outcomes.jsonl"
TRAINING_SET_PATH = STATE_DIR / "ai_training_set.jsonl"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        return
    with path.open("rb") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = _loads(line)
            except Exception:
                continue
            if isinstance(obj, dict):
                yield obj


def _safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Load feature store
# ---------------------------------------------------------------------------

def load_feature_store(
    strategy_filter: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Load feature_store.jsonl keyed by trade_id.

    Expects rows like:

        {
          "trade_id": "...",
          "symbol": "BTCUSDT",
          "strategy_name": "Sub1_Trend",
          "account_label": "flashback01",
          "mode": "PAPER" | "LIVE_CANARY" | ...
          "f.<...>": ...
        }
    """
    feats: Dict[str, Dict[str, Any]] = {}

    for row in _iter_jsonl(FEATURE_STORE_PATH):
        if not isinstance(row, dict):
            continue

        trade_id = row.get("trade_id") or row.get("id")
        if not trade_id:
            continue

        strat_name = (
            row.get("strategy_name")
            or row.get("strategy")
            or row.get("strat_name")
            or "unknown"
        )

        if strategy_filter and strat_name != strategy_filter:
            continue

        # Normalize some basic fields to keep downstream life easy
        out_row = dict(row)
        out_row.setdefault("strategy_name", strat_name)

        sym = out_row.get("symbol")
        if sym is not None:
            out_row["symbol"] = str(sym).upper()

        feats[str(trade_id)] = out_row

    return feats


# ---------------------------------------------------------------------------
# Load outcomes
# ---------------------------------------------------------------------------

def load_outcomes() -> Dict[str, Dict[str, Any]]:
    """
    Load ai_events/outcomes.jsonl keyed by trade_id.

    Expected schema (soft):

        {
          "trade_id": "...",
          "pnl_usd": float | null,
          "rr": float | null,
          "result": "win" | "loss" | ... (optional)
          ...
        }

    If your outcomes file has extra fields, they are left in place.
    """
    out: Dict[str, Dict[str, Any]] = {}

    for row in _iter_jsonl(OUTCOMES_PATH):
        if not isinstance(row, dict):
            continue

        trade_id = row.get("trade_id") or row.get("id")
        if not trade_id:
            continue

        # Normalize basic metrics
        pnl = (
            row.get("pnl_usd")
            or row.get("pnlUSD")
            or row.get("pnl")
            or row.get("profit_usd")
            or row.get("profit")
        )
        rr = row.get("rr") or row.get("r_multiple") or row.get("r")

        pnl_f = _safe_float(pnl)
        rr_f = _safe_float(rr)

        row = dict(row)
        row["pnl_usd"] = pnl_f
        row["rr"] = rr_f

        out[str(trade_id)] = row

    return out


# ---------------------------------------------------------------------------
# Label builder
# ---------------------------------------------------------------------------

def build_label(
    pnl_usd: Optional[float],
    rr: Optional[float],
) -> Optional[int]:
    """
    Build a simple binary label:

        1 → "win"
        0 → "loss"

    Using pnl_usd when available, else rr.

    Flat/zero / missing → None (skip).
    """
    if pnl_usd is not None:
        if pnl_usd > 0:
            return 1
        if pnl_usd < 0:
            return 0
        return None

    if rr is not None:
        if rr > 0:
            return 1
        if rr < 0:
            return 0
        return None

    return None


# ---------------------------------------------------------------------------
# Training set builder
# ---------------------------------------------------------------------------

def build_training_rows(
    feats: Dict[str, Dict[str, Any]],
    outcomes: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for trade_id, feat_row in feats.items():
        outcome = outcomes.get(trade_id)
        if not outcome:
            continue

        pnl = outcome.get("pnl_usd")
        rr = outcome.get("rr")
        label = build_label(pnl, rr)
        if label is None:
            # No clear win/loss label → skip
            continue

        # Shallow copy to avoid mutating original dicts
        merged = dict(feat_row)

        # Normalize some meta fields
        symbol = merged.get("symbol")
        strategy = (
            merged.get("strategy_name")
            or merged.get("strategy")
            or "unknown"
        )
        acc_label = merged.get("account_label") or merged.get("label") or None
        mode = str(merged.get("mode") or "UNKNOWN").upper().strip()

        merged["trade_id"] = trade_id
        merged["symbol"] = str(symbol).upper() if symbol else None
        merged["strategy"] = strategy
        merged["account_label"] = acc_label
        merged["mode"] = mode

        merged["label_win"] = int(label)
        merged["target_pnl_usd"] = pnl
        merged["target_rr"] = rr

        rows.append(merged)

    return rows


def write_training_set(rows: List[Dict[str, Any]]) -> None:
    TRAINING_SET_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = TRAINING_SET_PATH.with_suffix(".jsonl.tmp")

    with tmp.open("wb") as f:
        for row in rows:
            try:
                f.write(_dumps(row) + b"\n")
            except Exception:
                # Skip bad row serialization
                continue

    tmp.replace(TRAINING_SET_PATH)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> None:
    if argv is None:
        argv = sys.argv[1:]

    strategy_filter: Optional[str] = None
    if argv:
        strategy_filter = " ".join(argv).strip() or None

    print(f"[ai_training_builder] ROOT: {ROOT}")
    print(f"[ai_training_builder] feature_store: {FEATURE_STORE_PATH}")
    print(f"[ai_training_builder] outcomes:      {OUTCOMES_PATH}")

    if not FEATURE_STORE_PATH.exists():
        print(f"[ai_training_builder] feature_store.jsonl not found at {FEATURE_STORE_PATH}")
        return

    if not OUTCOMES_PATH.exists():
        print(f"[ai_training_builder] outcomes.jsonl not found at {OUTCOMES_PATH}")
        return

    feats = load_feature_store(strategy_filter=strategy_filter)
    outcomes = load_outcomes()

    print(f"[ai_training_builder] loaded {len(feats)} feature rows"
          + (f" (strategy={strategy_filter})" if strategy_filter else ""))
    print(f"[ai_training_builder] loaded {len(outcomes)} outcome rows")

    rows = build_training_rows(feats, outcomes)
    print(f"[ai_training_builder] matched + labeled trades: {len(rows)}")

    if not rows:
        print("[ai_training_builder] nothing to write (no labeled intersections).")
        return

    write_training_set(rows)
    print(f"[ai_training_builder] wrote {len(rows)} rows → {TRAINING_SET_PATH}")
    print("[ai_training_builder] Done.")


if __name__ == "__main__":
    main()
