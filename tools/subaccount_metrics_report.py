#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Subaccount Metrics Report (v1)

Goal
----
Aggregate strategy-level metrics into a per-subaccount dashboard:

  - For each subaccount:
      * Number of strategies
      * Total trades
      * Winrate (volume-weighted approx)
      * Expectancy (avg R)
      * PnL (if available)
      * Dominant automation_mode(s)

Input expectations
------------------
We expect a JSON file like:

  state/ai_metrics/strategies_snapshot.json

with a structure similar to:

  {
    "strategies": [
      {
        "strategy_name": "sub1_trend_v1",
        "account_label": "flashback01",
        "trade_count": 120,
        "win_count": 65,
        "avg_r": 0.35,
        "expectancy_r": 0.28,
        "pnl": 123.45,
        "automation_mode": "LEARN_DRY"
      },
      ...
    ]
  }

If the file isn't present or has a different shape, we degrade gracefully.
"""

from __future__ import annotations

import json
from collections import defaultdict, Counter
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "state"
METRICS_PATH = STATE_DIR / "ai_metrics" / "strategies_snapshot.json"


def load_metrics() -> Optional[Dict[str, Any]]:
    if not METRICS_PATH.exists():
        print(f"[WARN] Metrics file not found: {METRICS_PATH}")
        return None
    try:
        with METRICS_PATH.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        print(f"[ERROR] Failed to parse metrics file {METRICS_PATH}: {exc}")
        return None


def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def main() -> int:
    print("=== Flashback Subaccount Metrics Report ===")
    print(f"ROOT:        {ROOT}")
    print(f"STATE_DIR:   {STATE_DIR}")
    print(f"METRICS:     {METRICS_PATH}")
    print("")

    data = load_metrics()
    if not data:
        print("[WARN] No metrics data available. Run ai_paper_report / ai_metrics_aggregator first.")
        return 0

    strategies = data.get("strategies")
    if not isinstance(strategies, list) or not strategies:
        print("[WARN] No strategies array in metrics or it's empty.")
        return 0

    # Aggregate per subaccount
    agg: Dict[str, Dict[str, Any]] = {}
    modes_by_sub: Dict[str, Counter] = defaultdict(Counter)

    for s in strategies:
        if not isinstance(s, dict):
            continue
        acct = s.get("account_label") or s.get("sub_uid") or "UNKNOWN"
        acct = str(acct)

        strat_trade_count = int(s.get("trade_count", 0) or 0)
        strat_win_count = int(s.get("win_count", 0) or 0)
        strat_avg_r = safe_float(s.get("avg_r", 0.0))
        strat_exp_r = safe_float(s.get("expectancy_r", strat_avg_r))
        strat_pnl = safe_float(s.get("pnl", 0.0))
        mode = str(s.get("automation_mode", "UNKNOWN"))

        entry = agg.setdefault(
            acct,
            {
                "strategy_count": 0,
                "total_trades": 0,
                "total_wins": 0,
                "sum_expectancy_r_weighted": 0.0,
                "sum_avg_r_weighted": 0.0,
                "total_pnl": 0.0,
            },
        )

        entry["strategy_count"] += 1
        entry["total_trades"] += strat_trade_count
        entry["total_wins"] += strat_win_count
        entry["total_pnl"] += strat_pnl

        # Weighted by trade count
        weight = max(strat_trade_count, 1)
        entry["sum_expectancy_r_weighted"] += strat_exp_r * weight
        entry["sum_avg_r_weighted"] += strat_avg_r * weight

        modes_by_sub[acct][mode] += strat_trade_count or 1

    # Print table
    print("Subaccount performance:")
    print("-----------------------")
    header = (
        f"{'account':12s} {'strats':>6s} {'trades':>7s} "
        f"{'winrate%':>9s} {'avg_R':>8s} {'exp_R':>8s} {'PnL':>10s} {'top_mode':>12s}"
    )
    print(header)
    print("-" * len(header))

    for acct in sorted(agg.keys()):
        e = agg[acct]
        total_trades = e["total_trades"]
        total_wins = e["total_wins"]
        if total_trades > 0:
            winrate = 100.0 * total_wins / total_trades
            avg_r = e["sum_avg_r_weighted"] / total_trades
            exp_r = e["sum_expectancy_r_weighted"] / total_trades
        else:
            winrate = 0.0
            avg_r = 0.0
            exp_r = 0.0

        total_pnl = e["total_pnl"]
        strategy_count = e["strategy_count"]

        mode_counter = modes_by_sub.get(acct, Counter())
        top_mode = mode_counter.most_common(1)[0][0] if mode_counter else "UNKNOWN"

        print(
            f"{acct:12s} "
            f"{strategy_count:6d} "
            f"{total_trades:7d} "
            f"{winrate:9.2f} "
            f"{avg_r:8.3f} "
            f"{exp_r:8.3f} "
            f"{total_pnl:10.2f} "
            f"{top_mode:>12s}"
        )

    print("")
    print("[OK] Subaccount metrics report complete ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
