#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Policy Decision Report Tool

Purpose
-------
Offline inspector for:

    state/ai_policy_decisions.jsonl

This log is written by app.ai.policy_log.record_policy_decision(...)
from executor_v2.run_ai_gate().

It lets you quickly answer:

    - How often is each strategy allowed vs blocked?
    - What are the score ranges per strategy?
    - Which strategies are getting nuked by the min-score gate?
    - What symbols / timeframes are most affected?

Usage
-----
From project root:

    python -m app.tools.ai_policy_report

Optionally filter to a single strategy_id:

    python -m app.tools.ai_policy_report "Sub1_Trend (sub 524630315)"

Output
------
- Overall summary
- Per-strategy stats:
    • total decisions
    • allow / block counts + %
    • score min / max / avg
"""

from __future__ import annotations

import math
import sys
from collections import defaultdict, Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

# orjson preferred for speed / robustness, fallback to stdlib json
try:
    import orjson  # type: ignore

    def _loads(b: bytes) -> Any:
        return orjson.loads(b)

except Exception:  # pragma: no cover
    import json as _json  # type: ignore

    def _loads(b: bytes) -> Any:  # type: ignore
        if isinstance(b, (bytes, bytearray)):
            b = b.decode("utf-8")
        return _json.loads(b)


# ROOT + log path
try:
    from app.core.config import settings  # type: ignore

    ROOT: Path = settings.ROOT  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

STATE_DIR = ROOT / "state"
POLICY_LOG_PATH = STATE_DIR / "ai_policy_decisions.jsonl"


# ---------------------------------------------------------------------------
# Loading helpers
# ---------------------------------------------------------------------------

def iter_decisions(
    path: Path,
    *,
    strategy_filter: Optional[str] = None,
) -> Iterable[Dict[str, Any]]:
    """
    Stream decisions from ai_policy_decisions.jsonl.

    If strategy_filter is provided, only yield rows with that strategy_id.
    """
    if not path.exists():
        print(f"[ai_policy_report] No policy log found at: {path}")
        return

    with path.open("rb") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = _loads(line)
            except Exception:
                # skip bad JSON
                continue
            if not isinstance(obj, dict):
                continue

            strat_id = str(obj.get("strategy_id", "")).strip()
            if strategy_filter and strat_id != strategy_filter:
                continue

            yield obj


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def _safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        val = float(x)
        if math.isnan(val) or math.isinf(val):
            return None
        return val
    except Exception:
        return None


def aggregate_decisions(
    rows: Iterable[Dict[str, Any]]
) -> Tuple[
    int,
    Counter,
    Dict[str, Dict[str, Any]],
]:
    """
    Aggregate per-strategy stats.

    Returns
    -------
    total : int
        Total number of decisions.
    mode_counts : Counter
        Counts by mode (PAPER / LIVE_CANARY / LIVE_FULL / UNKNOWN).
    per_strategy : dict[str, dict]
        Stats per strategy_id:
            {
              "count": int,
              "allow": int,
              "block": int,
              "scores": [float, ...],
              "symbols": Counter(),
              "timeframes": Counter(),
              "modes": Counter(),
            }
    """
    total = 0
    mode_counts: Counter = Counter()
    per_strategy: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        total += 1
        strat_id = str(row.get("strategy_id", "")).strip() or "unknown"
        allow = bool(row.get("allow", False))
        mode = str(row.get("mode") or "UNKNOWN").upper().strip() or "UNKNOWN"
        sym = str(row.get("symbol") or "").upper().strip() or None
        tf = str(row.get("timeframe") or "").strip() or None
        score = _safe_float(row.get("score"))

        mode_counts[mode] += 1

        stats = per_strategy.get(strat_id)
        if stats is None:
            stats = {
                "count": 0,
                "allow": 0,
                "block": 0,
                "scores": [],
                "symbols": Counter(),
                "timeframes": Counter(),
                "modes": Counter(),
            }
            per_strategy[strat_id] = stats

        stats["count"] += 1
        if allow:
            stats["allow"] += 1
        else:
            stats["block"] += 1

        stats["modes"][mode] += 1
        if sym:
            stats["symbols"][sym] += 1
        if tf:
            stats["timeframes"][tf] += 1
        if score is not None:
            stats["scores"].append(score)

    return total, mode_counts, per_strategy


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def _fmt_pct(num: int, denom: int) -> str:
    if denom <= 0:
        return "0.0%"
    return f"{(100.0 * num / denom):4.1f}%"


def _score_stats(scores: List[float]) -> str:
    if not scores:
        return "n/a"
    smin = min(scores)
    smax = max(scores)
    savg = sum(scores) / len(scores)
    return f"min={smin:.3f}, max={smax:.3f}, avg={savg:.3f}"


def print_report(
    total: int,
    mode_counts: Counter,
    per_strategy: Dict[str, Dict[str, Any]],
    *,
    strategy_filter: Optional[str] = None,
) -> None:
    """
    Pretty-print the report to stdout.
    """
    title = "AI Policy Decision Report"
    if strategy_filter:
        title += f" — strategy_id={strategy_filter}"
    print("=" * len(title))
    print(title)
    print("=" * len(title))
    print()

    if total == 0:
        print("No decisions found.")
        return

    print(f"Total decisions: {total}")
    print("By mode:")
    for mode, cnt in mode_counts.items():
        print(f"  - {mode:12s}: {cnt:6d} ({_fmt_pct(cnt, total)})")
    print()

    # Sort strategies by number of decisions desc
    items = sorted(per_strategy.items(), key=lambda kv: kv[1]["count"], reverse=True)

    print("Per-strategy summary:")
    print("----------------------")
    for strat_id, stats in items:
        count = stats["count"]
        allow = stats["allow"]
        block = stats["block"]
        scores = stats["scores"]
        modes = stats["modes"]
        symbols = stats["symbols"]
        timeframes = stats["timeframes"]

        allow_pct = _fmt_pct(allow, count)
        block_pct = _fmt_pct(block, count)

        print(f"\nStrategy: {strat_id}")
        print(f"  total decisions : {count}")
        print(f"  allow           : {allow:6d} ({allow_pct})")
        print(f"  block           : {block:6d} ({block_pct})")
        print(f"  score stats     : {_score_stats(scores)}")

        if modes:
            top_modes = ", ".join(f"{m}={c}" for m, c in modes.most_common())
            print(f"  modes           : {top_modes}")

        if symbols:
            top_syms = ", ".join(f"{s}={c}" for s, c in symbols.most_common(5))
            print(f"  symbols         : {top_syms}")

        if timeframes:
            top_tfs = ", ".join(f"{tf}={c}" for tf, c in timeframes.most_common(5))
            print(f"  timeframes      : {top_tfs}")

    print()
    print("[ai_policy_report] Done.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> None:
    if argv is None:
        argv = sys.argv[1:]

    strategy_filter: Optional[str] = None
    if argv:
        # Everything after the script name is treated as a single strategy_id
        strategy_filter = " ".join(argv).strip() or None

    rows = list(iter_decisions(POLICY_LOG_PATH, strategy_filter=strategy_filter))
    total, mode_counts, per_strategy = aggregate_decisions(rows)
    print_report(total, mode_counts, per_strategy, strategy_filter=strategy_filter)


if __name__ == "__main__":
    main()
