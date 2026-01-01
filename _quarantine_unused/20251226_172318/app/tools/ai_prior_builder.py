#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Prior Builder (strategy winrate stats)

Purpose
-------
Read:

    state/ai_training_set.jsonl

and compute simple priors:

    - Global winrate and averages
    - Per-strategy winrate and averages

Write them to:

    state/ai_priors.json

This is the first "brain" feeding trade_classifier:
  - classifier will map a strategy_id like "Sub1_Trend (sub 524630315)"
    → base name "Sub1_Trend"
  - look up its stats here
  - use winrate as the AI "score" (0..1).

Schema (ai_priors.json)
-----------------------
{
  "global": {
    "n_trades": 123,
    "n_wins": 70,
    "win_rate": 0.569,
    "avg_rr": 0.84,
    "avg_pnl_usd": 3.21
  },
  "by_strategy": {
    "Sub1_Trend": {
      "n_trades": 40,
      "n_wins": 25,
      "win_rate": 0.625,
      "avg_rr": 1.05,
      "avg_pnl_usd": 4.50
    },
    ...
  }
}
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

# ROOT / paths
try:
    from app.core.config import settings  # type: ignore

    ROOT: Path = settings.ROOT  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

STATE_DIR = ROOT / "state"
TRAINING_SET_PATH = STATE_DIR / "ai_training_set.jsonl"
PRIORS_PATH = STATE_DIR / "ai_priors.json"


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
                obj = json.loads(line.decode("utf-8"))
            except Exception:
                try:
                    obj = json.loads(line)
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
# Core stats builder
# ---------------------------------------------------------------------------

def build_priors() -> Dict[str, Any]:
    """
    Read ai_training_set.jsonl and build global + per-strategy stats.
    """
    if not TRAINING_SET_PATH.exists():
        print(f"[ai_prior_builder] training set not found at {TRAINING_SET_PATH}")
        return {"global": {}, "by_strategy": {}}

    global_n = 0
    global_wins = 0
    global_rr_sum = 0.0
    global_rr_n = 0
    global_pnl_sum = 0.0
    global_pnl_n = 0

    by_strategy: Dict[str, Dict[str, Any]] = {}

    for row in _iter_jsonl(TRAINING_SET_PATH):
        label = row.get("label_win")
        if label not in (0, 1, "0", "1"):
            continue

        try:
            label_int = int(label)
        except Exception:
            continue

        strat = (
            row.get("strategy")
            or row.get("strategy_name")
            or row.get("strategy_id")
            or "unknown"
        )
        strat = str(strat)

        rr = _safe_float(row.get("target_rr") or row.get("rr"))
        pnl = _safe_float(row.get("target_pnl_usd") or row.get("pnl_usd"))

        # Global stats
        global_n += 1
        if label_int == 1:
            global_wins += 1

        if rr is not None:
            global_rr_sum += rr
            global_rr_n += 1

        if pnl is not None:
            global_pnl_sum += pnl
            global_pnl_n += 1

        # Strategy stats
        s = by_strategy.get(strat)
        if s is None:
            s = {
                "n_trades": 0,
                "n_wins": 0,
                "rr_sum": 0.0,
                "rr_n": 0,
                "pnl_sum": 0.0,
                "pnl_n": 0,
            }
            by_strategy[strat] = s

        s["n_trades"] += 1
        if label_int == 1:
            s["n_wins"] += 1
        if rr is not None:
            s["rr_sum"] += rr
            s["rr_n"] += 1
        if pnl is not None:
            s["pnl_sum"] += pnl
            s["pnl_n"] += 1

    # Finalize global
    if global_n > 0:
        global_win_rate = global_wins / float(global_n)
    else:
        global_win_rate = 0.5  # neutral coin-flip if nothing exists

    global_avg_rr = global_rr_sum / global_rr_n if global_rr_n > 0 else None
    global_avg_pnl = global_pnl_sum / global_pnl_n if global_pnl_n > 0 else None

    global_block = {
        "n_trades": global_n,
        "n_wins": global_wins,
        "win_rate": global_win_rate,
        "avg_rr": global_avg_rr,
        "avg_pnl_usd": global_avg_pnl,
    }

    # Finalize per-strategy
    strat_block: Dict[str, Any] = {}
    for strat, s in by_strategy.items():
        n = s["n_trades"] or 0
        w = s["n_wins"] or 0
        if n > 0:
            win_rate = w / float(n)
        else:
            win_rate = None

        avg_rr = s["rr_sum"] / s["rr_n"] if s["rr_n"] > 0 else None
        avg_pnl = s["pnl_sum"] / s["pnl_n"] if s["pnl_n"] > 0 else None

        strat_block[strat] = {
            "n_trades": n,
            "n_wins": w,
            "win_rate": win_rate,
            "avg_rr": avg_rr,
            "avg_pnl_usd": avg_pnl,
        }

    priors = {
        "global": global_block,
        "by_strategy": strat_block,
    }
    return priors


def write_priors(priors: Dict[str, Any]) -> None:
    PRIORS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with PRIORS_PATH.open("w", encoding="utf-8") as f:
        json.dump(priors, f, ensure_ascii=False, indent=2)


def main() -> None:
    print(f"[ai_prior_builder] ROOT: {ROOT}")
    print(f"[ai_prior_builder] training set: {TRAINING_SET_PATH}")
    priors = build_priors()
    write_priors(priors)
    total_strats = len(priors.get("by_strategy", {}))
    print(
        f"[ai_prior_builder] wrote priors for {total_strats} strategies "
        f"→ {PRIORS_PATH}"
    )
    print("[ai_prior_builder] Done.")


if __name__ == "__main__":
    main()
