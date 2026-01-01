#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Policy Tuner (v1)

Purpose
-------
Scan state/setup_memory.jsonl and compute per-strategy stats:

    - n_trades
    - winrate (label_win)
    - mean rr_realized
    - distribution by outcome_bucket
    - simple AI-score buckets (if ai_score present in features)

Then emit a "suggestions" file:

    state/setup_policy_suggestions.json

with human-readable guidance plus machine-readable fields that
you (or a later script) can use to update setup_policy.json.

This does NOT modify setup_policy.json directly.
It's a stats / recommendation generator.
"""

from __future__ import annotations

import time
from collections import Counter
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

MEMORY_PATH: Path = STATE_DIR / "setup_memory.jsonl"
OUTPUT_PATH: Path = STATE_DIR / "setup_policy_suggestions.json"


# ----------------- IO helpers -----------------


def _load_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        print(f"[policy_tune] WARNING: {path} does not exist.")
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


def _safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def _bool(v: Any) -> bool:
    return bool(v is True or v == 1)


# ----------------- core analysis -----------------


def _strategy_key(row: Dict[str, Any]) -> str:
    """
    Compact, human-readable strategy key.

    Uses:
      - strategy_name if present
      - otherwise: "<account_label>:<symbol>" as a fallback
    """
    s = str(row.get("strategy_name") or "").strip()
    if s:
        return s
    account = str(row.get("account_label") or "main").strip()
    symbol = str(row.get("symbol") or "UNKNOWN").strip()
    return f"{account}:{symbol}"


def _bucket_result_for_stats(row: Dict[str, Any]) -> str:
    """
    Normalize bucket for stats.
    """
    bucket = str(row.get("outcome_bucket") or "").upper().strip()
    if bucket not in ("WIN_STRONG", "WIN_WEAK", "SCRATCH", "LOSS", "UNKNOWN"):
        bucket = "UNKNOWN"
    return bucket


def _collect_stats() -> Dict[str, Dict[str, Any]]:
    """
    Aggregate per-strategy stats from setup_memory.jsonl.
    """
    per_strat: Dict[str, Dict[str, Any]] = {}

    for row in _load_jsonl(MEMORY_PATH):
        key = _strategy_key(row)
        strat = per_strat.get(key)
        if strat is None:
            strat = {
                "n_trades": 0,
                "wins": 0,
                "rr_sum": 0.0,
                "rr_count": 0,
                "bucket_counts": Counter(),
                "ai_scores_win": [],
                "ai_scores_loss": [],
            }
            per_strat[key] = strat

        strat["n_trades"] += 1

        if _bool(row.get("label_win")):
            strat["wins"] += 1

        rr = _safe_float(row.get("rr_realized"))
        if rr is not None:
            strat["rr_sum"] += rr
            strat["rr_count"] += 1

        bucket = _bucket_result_for_stats(row)
        strat["bucket_counts"][bucket] += 1

        # Extract ai_score if present in features
        features = row.get("features") or {}
        ai_score = _safe_float(features.get("ai_score"))
        if ai_score is not None:
            if bucket in ("WIN_STRONG", "WIN_WEAK", "SCRATCH"):
                strat["ai_scores_win"].append(ai_score)
            elif bucket == "LOSS":
                strat["ai_scores_loss"].append(ai_score)

    return per_strat


def _compute_suggestion_for_strategy(key: str, raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Turn raw counts into a structured suggestion.
    """
    n_trades = raw["n_trades"]
    wins = raw["wins"]
    rr_sum = raw["rr_sum"]
    rr_count = raw["rr_count"]
    buckets: Counter = raw["bucket_counts"]
    ai_scores_win: List[float] = raw["ai_scores_win"]
    ai_scores_loss: List[float] = raw["ai_scores_loss"]

    if n_trades > 0:
        winrate = wins / n_trades
    else:
        winrate = 0.0

    rr_mean = rr_sum / rr_count if rr_count > 0 else 0.0

    # Bucket distribution
    bucket_dist = {
        b: int(c)
        for b, c in buckets.items()
    }

    # Simple AI-score recommendation:
    #
    #   - if we have AI scores, look at median scores for wins vs losses
    #   - if win median > loss median, pick a threshold in between
    #
    # This is intentionally crude; it's just to give you a starting point.
    def _median(xs: List[float]) -> Optional[float]:
        if not xs:
            return None
        xs_sorted = sorted(xs)
        n = len(xs_sorted)
        mid = n // 2
        if n % 2 == 1:
            return xs_sorted[mid]
        return 0.5 * (xs_sorted[mid - 1] + xs_sorted[mid])

    win_med = _median(ai_scores_win)
    loss_med = _median(ai_scores_loss)

    suggested_min_ai_score: Optional[float] = None
    ai_basis = "insufficient_ai_scores"

    if win_med is not None and loss_med is not None:
        if win_med > loss_med:
            # Put the threshold somewhere between loss_med and win_med
            suggested_min_ai_score = float((win_med + loss_med) / 2.0)
            ai_basis = (
                f"median_win_ai_score={win_med:.3f}, "
                f"median_loss_ai_score={loss_med:.3f}"
            )
        else:
            # AI scores don't correlate well (or inverted)
            ai_basis = (
                f"median_win_ai_score={win_med:.3f}, "
                f"median_loss_ai_score={loss_med:.3f}, "
                "no clear separation"
            )

    return {
        "strategy_key": key,
        "n_trades": n_trades,
        "wins": wins,
        "winrate": winrate,
        "rr_mean": rr_mean,
        "bucket_counts": bucket_dist,
        "ai_scores_win_count": len(ai_scores_win),
        "ai_scores_loss_count": len(ai_scores_loss),
        "suggested_min_ai_score": suggested_min_ai_score,
        "ai_basis": ai_basis,
    }


def run_policy_tune() -> None:
    print("[policy_tune] Reading setup_memory from:", MEMORY_PATH)

    per_strat_raw = _collect_stats()
    if not per_strat_raw:
        print("[policy_tune] No strategies found in setup_memory.jsonl (file empty?).")
        suggestions = {
            "version": 1,
            "generated_at": int(time.time() * 1000),
            "total_strategies": 0,
            "per_strategy": {},
        }
        OUTPUT_PATH.write_bytes(orjson.dumps(suggestions, option=orjson.OPT_INDENT_2))
        print("[policy_tune] Wrote empty suggestions to", OUTPUT_PATH)
        return

    per_strategy_suggestions: Dict[str, Any] = {}

    for key, raw in per_strat_raw.items():
        per_strategy_suggestions[key] = _compute_suggestion_for_strategy(key, raw)

    payload = {
        "version": 1,
        "generated_at": int(time.time() * 1000),
        "total_strategies": len(per_strategy_suggestions),
        "per_strategy": per_strategy_suggestions,
    }

    OUTPUT_PATH.write_bytes(orjson.dumps(payload, option=orjson.OPT_INDENT_2))

    print(f"[policy_tune] Processed {len(per_strategy_suggestions)} strategies.")
    print("[policy_tune] Suggestions written to:", OUTPUT_PATH)


if __name__ == "__main__":
    run_policy_tune()
