#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Flashback — AI State Health (Phase 3 enforcement)

What it checks (by default):
- features_trades.jsonl exists and is non-empty OR warns if missing
- feature_store.jsonl exists and has enough rows
- per-strategy minimum counts (basic coverage)
- freshness (max age hours) based on file modified time
- JSONL parse quality (bad lines counted)

Exit codes:
- 0 = PASS
- 1 = WARN (usable but weak)
- 2 = FAIL (do not proceed to Phase 4 training/memory builds)

Usage:
  python -m app.tools.ai_state_health
  python -m app.tools.ai_state_health --min-total 50 --min-per-strategy 10 --max-age-hours 72
"""

from __future__ import annotations

import argparse
import json
import os
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple


@dataclass
class FileStats:
    path: Path
    exists: bool
    rows: int
    bad_lines: int
    by_strategy: Counter
    mtime_epoch: Optional[float]


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _now_epoch() -> float:
    return time.time()


def _mtime_epoch(path: Path) -> Optional[float]:
    try:
        return path.stat().st_mtime
    except Exception:
        return None


def read_jsonl_stats(path: Path, strategy_keys: Tuple[str, ...]) -> FileStats:
    exists = path.exists()
    rows = 0
    bad = 0
    by_strategy: Counter = Counter()
    mtime = _mtime_epoch(path)

    if not exists:
        return FileStats(path=path, exists=False, rows=0, bad_lines=0, by_strategy=by_strategy, mtime_epoch=None)

    # Read line-by-line, tolerate junk, count strategies
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                rows += 1
                strat = "UNKNOWN"
                if isinstance(obj, dict):
                    for k in strategy_keys:
                        v = obj.get(k)
                        if isinstance(v, str) and v.strip():
                            strat = v.strip()
                            break
                by_strategy[strat] += 1
            except Exception:
                bad += 1

    return FileStats(path=path, exists=True, rows=rows, bad_lines=bad, by_strategy=by_strategy, mtime_epoch=mtime)


def age_hours(mtime_epoch: Optional[float]) -> Optional[float]:
    if mtime_epoch is None:
        return None
    return round((_now_epoch() - mtime_epoch) / 3600.0, 2)


def classify_health(
    features: FileStats,
    store: FileStats,
    min_total: int,
    min_per_strategy: int,
    max_age_hours: float,
    max_bad_line_ratio: float,
) -> Tuple[str, int, Dict[str, str]]:
    """
    Returns: (status_str, exit_code, reasons)
    status_str ∈ {"PASS", "WARN", "FAIL"}
    """
    reasons: Dict[str, str] = {}

    # Freshness checks
    store_age = age_hours(store.mtime_epoch)

    if store.exists and store_age is not None and store_age > max_age_hours:
        reasons["feature_store_stale"] = f"feature_store.jsonl age {store_age}h > {max_age_hours}h"

    # Parse quality checks
    def bad_ratio(fs: FileStats) -> float:
        total_lines = fs.rows + fs.bad_lines
        if total_lines <= 0:
            return 0.0
        return fs.bad_lines / total_lines

    if store.exists and bad_ratio(store) > max_bad_line_ratio:
        reasons["feature_store_corrupt"] = f"bad line ratio {bad_ratio(store):.2%} > {max_bad_line_ratio:.2%}"

    if features.exists and bad_ratio(features) > max_bad_line_ratio:
        reasons["features_trades_corrupt"] = f"bad line ratio {bad_ratio(features):.2%} > {max_bad_line_ratio:.2%}"

    # Coverage checks (feature_store is the primary dataset)
    if not store.exists:
        reasons["feature_store_missing"] = "state/feature_store.jsonl missing"
    else:
        if store.rows < min_total:
            reasons["feature_store_too_small"] = f"rows {store.rows} < min_total {min_total}"

        # per-strategy coverage
        # (ignore UNKNOWN unless it's dominating)
        for strat, n in store.by_strategy.most_common():
            if strat == "UNKNOWN":
                continue
            if n < min_per_strategy:
                reasons[f"strategy_low_{strat}"] = f"{strat} rows {n} < min_per_strategy {min_per_strategy}"

        # If UNKNOWN dominates, warn (it means your pipeline isn't tagging strategy)
        if store.by_strategy.get("UNKNOWN", 0) > max(10, int(store.rows * 0.25)):
            reasons["strategy_missing_tags"] = f"UNKNOWN strategy rows {store.by_strategy.get('UNKNOWN', 0)} is too high; ensure strategy name is written into JSONL rows"

    # Features file isn't required for PASS, but missing it should warn
    if not features.exists:
        reasons["features_trades_missing"] = "state/features_trades.jsonl missing (WARN unless you are truly fresh)"
    else:
        if features.rows == 0:
            reasons["features_trades_empty"] = "features_trades.jsonl exists but has 0 rows"

    # Decide status
    # FAIL conditions:
    fail_keys_prefixes = (
        "feature_store_missing",
        "feature_store_too_small",
        "feature_store_corrupt",
    )
    is_fail = any(k.startswith(fail_keys_prefixes) for k in reasons.keys())

    # WARN conditions:
    # - stale store
    # - missing features file
    # - strategy tagging issue
    is_warn = (
        "feature_store_stale" in reasons
        or "features_trades_missing" in reasons
        or "strategy_missing_tags" in reasons
        or "features_trades_corrupt" in reasons
    )

    if is_fail:
        return "FAIL", 2, reasons
    if reasons and is_warn:
        return "WARN", 1, reasons
    if reasons:
        # Non-fatal issues (e.g., low counts for some strategies) but store exists and has min_total
        return "WARN", 1, reasons
    return "PASS", 0, reasons


def main() -> int:
    root = repo_root()
    state_dir = root / "state"

    ap = argparse.ArgumentParser()
    ap.add_argument("--state-dir", default=str(state_dir), help="Path to state/ directory")
    ap.add_argument("--min-total", type=int, default=int(os.getenv("AI_MIN_TOTAL_ROWS", "50")))
    ap.add_argument("--min-per-strategy", type=int, default=int(os.getenv("AI_MIN_ROWS_PER_STRATEGY", "10")))
    ap.add_argument("--max-age-hours", type=float, default=float(os.getenv("AI_MAX_AGE_HOURS", "72")))
    ap.add_argument("--max-bad-line-ratio", type=float, default=float(os.getenv("AI_MAX_BAD_LINE_RATIO", "0.02")))
    args = ap.parse_args()

    state = Path(args.state_dir)
    features_path = state / "features_trades.jsonl"
    store_path = state / "feature_store.jsonl"

    # Strategy keys we commonly emit into records (we try a few)
    strategy_keys = ("strategy", "strategy_name", "setup", "setup_name", "strategy_id")

    features = read_jsonl_stats(features_path, strategy_keys)
    store = read_jsonl_stats(store_path, strategy_keys)

    status, code, reasons = classify_health(
        features=features,
        store=store,
        min_total=args.min_total,
        min_per_strategy=args.min_per_strategy,
        max_age_hours=args.max_age_hours,
        max_bad_line_ratio=args.max_bad_line_ratio,
    )

    print("\n🧠 AI STATE HEALTH (Phase 3)\n")
    print(f"Repo root: {root}")
    print(f"State dir: {state}\n")

    def print_file(fs: FileStats, label: str) -> None:
        print(f"{label}: {fs.path}")
        print(f"  exists: {fs.exists}")
        print(f"  rows:   {fs.rows}")
        print(f"  bad:    {fs.bad_lines}")
        a = age_hours(fs.mtime_epoch)
        print(f"  age_h:  {a if a is not None else 'N/A'}")
        if fs.exists and fs.rows > 0:
            top = fs.by_strategy.most_common(5)
            print(f"  top strategies: {top}")
        print("")

    print_file(features, "features_trades.jsonl")
    print_file(store, "feature_store.jsonl")

    print(f"RESULT: {status}")
    if reasons:
        print("\nReasons:")
        for k, v in sorted(reasons.items()):
            print(f"  - {k}: {v}")
    print("")

    return code


if __name__ == "__main__":
    raise SystemExit(main())
