#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Policy Tuner (v1, suggestion-only)

Purpose
-------
Scan setup_memory.jsonl and, per strategy_name, analyze:

    - ai_score distribution
    - winrate above various score thresholds

Then write *suggested* min_ai_score values into:

    state/setup_policy.json

without overwriting the live min_ai_score used by
executor_ai_gate / setup_memory_policy. The idea:

    - You run this periodically.
    - It updates "suggested_min_ai_score" + stats.
    - You manually decide whether to promote suggestions
      to live min_ai_score via setup_memory_policy tools.

Assumptions
-----------
setup_memory.jsonl rows look like (v2+):

    {
      "trade_id": "...",
      "strategy_name": "Sub2_Breakout",
      "mode": "PAPER" | "LIVE_CANARY" | "LIVE_FULL" | ...,
      "result": "WIN" | "LOSS" | "BREAKEVEN" | "UNKNOWN",
      "features": {
          "ai_score": 0.73,
          ...
      },
      ...
    }

We treat:
    - WIN as 1
    - LOSS as 0
    - BREAKEVEN / UNKNOWN ignored for winrate.

Configuration
-------------
Tune these constants if needed:

    MIN_TRADES_FOR_THRESHOLD   = 50
    TARGET_WINRATE             = 0.55   (55%+)
    MIN_LIFT_OVER_BASE         = 0.05   (5% better than baseline)
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import orjson

try:
    from app.core.config import settings
except ImportError:
    from core.config import settings  # type: ignore

ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])
STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

MEMORY_PATH: Path = STATE_DIR / "setup_memory.jsonl"
POLICY_PATH: Path = STATE_DIR / "setup_policy.json"

# --- Tuning constants --- #
MIN_TRADES_FOR_THRESHOLD = 50
TARGET_WINRATE = 0.55
MIN_LIFT_OVER_BASE = 0.05


@dataclass
class StratSample:
    scores: List[float]
    wins: List[int]  # 1 for WIN, 0 for LOSS


# ----------------- IO helpers ----------------- #


def _load_memory_rows(path: Path) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        print(f"[policy_tune] WARNING: {path} does not exist; nothing to tune.")
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


def _load_policy(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {
            "version": 1,
            "updated_at": None,
            "default": {
                "min_ai_score": 0.0,
                "risk_multiplier": 1.0,
            },
            "strategies": {},
        }
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError("not a dict")
        data.setdefault("strategies", {})
        data.setdefault("default", {"min_ai_score": 0.0, "risk_multiplier": 1.0})
        return data
    except Exception:
        # If it's garbage, start fresh but don't crash.
        return {
            "version": 1,
            "updated_at": None,
            "default": {
                "min_ai_score": 0.0,
                "risk_multiplier": 1.0,
            },
            "strategies": {},
        }


def _save_policy(path: Path, data: Dict[str, Any]) -> None:
    data["updated_at"] = int(time.time() * 1000)
    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, sort_keys=True)
    except Exception as e:
        print(f"[policy_tune] ERROR writing policy file: {e}")


# ----------------- Core logic ----------------- #


def _result_to_int(result: str) -> Optional[int]:
    r = (result or "").upper()
    if r == "WIN":
        return 1
    if r == "LOSS":
        return 0
    # BREAKEVEN / UNKNOWN -> ignore
    return None


def _collect_samples() -> Dict[str, StratSample]:
    """
    Aggregate ai_score + result per strategy_name from setup_memory.
    """
    samples: Dict[str, StratSample] = {}

    for row in _load_memory_rows(MEMORY_PATH):
        strat_name = str(row.get("strategy_name") or "unknown").strip()
        if not strat_name:
            continue

        result = row.get("result")
        res_int = _result_to_int(str(result))
        if res_int is None:
            continue

        features = row.get("features") or {}
        ai_score_raw = features.get("ai_score")
        if ai_score_raw is None:
            continue

        try:
            ai_score = float(ai_score_raw)
        except Exception:
            continue

        if strat_name not in samples:
            samples[strat_name] = StratSample(scores=[], wins=[])
        s = samples[strat_name]
        s.scores.append(ai_score)
        s.wins.append(res_int)

    return samples


def _compute_baseline_winrate(sample: StratSample) -> float:
    if not sample.wins:
        return 0.0
    return sum(sample.wins) / float(len(sample.wins))


def _suggest_threshold_for_sample(sample: StratSample) -> Optional[Tuple[float, Dict[str, Any]]]:
    """
    From a sample of (score, win) pairs, find a threshold t such that:

        winrate(score >= t) >= TARGET_WINRATE
        and count(score >= t) >= MIN_TRADES_FOR_THRESHOLD
        and winrate(score >= t) >= baseline + MIN_LIFT_OVER_BASE

    Return:
        (threshold, stats dict) or None if no good threshold is found.
    """
    n = len(sample.scores)
    if n < MIN_TRADES_FOR_THRESHOLD:
        return None

    # Pair up and sort descending by score
    pairs = sorted(
        zip(sample.scores, sample.wins),
        key=lambda x: x[0],
        reverse=True,
    )

    baseline = _compute_baseline_winrate(sample)

    # cumulative stats scanning from high score -> low
    cum_count = 0
    cum_wins = 0

    best_threshold: Optional[float] = None
    best_stats: Dict[str, Any] = {}

    for score, win in pairs:
        cum_count += 1
        cum_wins += win

        if cum_count < MIN_TRADES_FOR_THRESHOLD:
            continue

        winrate = cum_wins / float(cum_count)

        if winrate >= TARGET_WINRATE and winrate >= baseline + MIN_LIFT_OVER_BASE:
            # candidate threshold is current score
            cand_t = float(score)

            # Keep the *lowest* threshold that meets criteria,
            # so we don't over-restrict everything.
            best_threshold = cand_t
            best_stats = {
                "winrate_above_t": winrate,
                "trades_above_t": cum_count,
                "baseline_winrate": baseline,
            }

    if best_threshold is None:
        return None

    return best_threshold, best_stats


def tune_policies() -> None:
    print("[policy_tune] Loading setup_memory rows...")
    samples = _collect_samples()
    if not samples:
        print("[policy_tune] No usable samples with ai_score + WIN/LOSS found. Nothing to do.")
        return

    print(f"[policy_tune] Collected samples for {len(samples)} strategies.\n")

    policy = _load_policy(POLICY_PATH)
    strategies_cfg: Dict[str, Any] = policy.setdefault("strategies", {})

    summary_rows: List[Tuple[str, Optional[float], Dict[str, Any]]] = []

    for strat_name, sample in samples.items():
        total = len(sample.scores)
        baseline = _compute_baseline_winrate(sample)
        print(
            f"[policy_tune] Strategy={strat_name} | rows={total} | baseline_winrate={baseline:.3f}"
        )

        suggestion = _suggest_threshold_for_sample(sample)
        if suggestion is None:
            print("    -> No threshold meets criteria (or insufficient data).")
            summary_rows.append((strat_name, None, {"baseline": baseline, "rows": total}))
            continue

        threshold, stats = suggestion
        print(
            f"    -> Suggested min_ai_score={threshold:.3f} | "
            f"winrate_above_t={stats['winrate_above_t']:.3f} | "
            f"trades_above_t={stats['trades_above_t']} | "
            f"baseline={stats['baseline_winrate']:.3f}"
        )

        strat_policy = strategies_cfg.get(strat_name, {})
        if not isinstance(strat_policy, dict):
            strat_policy = {}

        # DO NOT overwrite live min_ai_score here.
        # Just store suggestion + stats.
        strat_policy["suggested_min_ai_score"] = threshold
        strat_policy.setdefault("meta", {})
        meta = strat_policy["meta"]
        if not isinstance(meta, dict):
            meta = {}
        meta["tune_stats"] = {
            "baseline_winrate": stats["baseline_winrate"],
            "winrate_above_t": stats["winrate_above_t"],
            "trades_above_t": stats["trades_above_t"],
            "rows_total": total,
            "min_trades_for_threshold": MIN_TRADES_FOR_THRESHOLD,
            "target_winrate": TARGET_WINRATE,
            "min_lift_over_base": MIN_LIFT_OVER_BASE,
            "last_tuned_at_ms": int(time.time() * 1000),
        }
        strat_policy["meta"] = meta

        strategies_cfg[strat_name] = strat_policy
        summary_rows.append((strat_name, threshold, stats))

    _save_policy(POLICY_PATH, policy)

    print("\n[policy_tune] Summary:")
    for strat_name, thr, stats in summary_rows:
        if thr is None:
            print(
                f"  - {strat_name}: no suggestion (rows={stats.get('rows')} "
                f"baseline={stats.get('baseline', 0.0):.3f})"
            )
        else:
            print(
                f"  - {strat_name}: suggested_min_ai_score={thr:.3f} | "
                f"baseline={stats['baseline_winrate']:.3f} | "
                f"winrate_above_t={stats['winrate_above_t']:.3f} | "
                f"trades_above_t={stats['trades_above_t']}"
            )

    print(f"\n[policy_tune] Suggestions written into {POLICY_PATH}")
    print("             (fields: strategies[*].suggested_min_ai_score + meta.tune_stats)")


if __name__ == "__main__":
    tune_policies()
