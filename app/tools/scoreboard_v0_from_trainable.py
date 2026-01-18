#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scoreboard_v0_from_trainable.py

Builds a deterministic scoreboard.v1.json from:
  state/ai_events/outcomes.v1.trainable.jsonl

Writes:
  state/scoreboard/scoreboard.v1.json
  state/ai_memory/scoreboard.v1.json
"""

from __future__ import annotations

import json
import time
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
IN_PATH = ROOT / "state" / "ai_events" / "outcomes.v1.trainable.jsonl"

OUT_SCOREBOARD = ROOT / "state" / "scoreboard" / "scoreboard.v1.json"
OUT_AI_MEMORY = ROOT / "state" / "ai_memory" / "scoreboard.v1.json"

DEFAULT_MIN_N = 10


def main() -> None:
    if not IN_PATH.exists():
        raise SystemExit(f"FAIL: missing {IN_PATH}")

    buckets = defaultdict(lambda: {"n": 0, "wins": 0, "pnl": 0.0})

    for line in IN_PATH.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not line.strip():
            continue
        try:
            o = json.loads(line)
        except Exception:
            continue

        setup_type = str(o.get("setup_type") or "unknown").lower().strip()
        timeframe = str(o.get("timeframe") or "unknown").lower().strip()
        symbol = str(o.get("symbol") or "unknown").upper().strip()

        key = (setup_type, timeframe, symbol)
        b = buckets[key]
        b["n"] += 1

        pnl = float(o.get("pnl_usd") or 0.0)
        b["pnl"] += pnl
        if pnl > 0:
            b["wins"] += 1

    rows = []
    total_outcomes = 0

    for (setup_type, timeframe, symbol), b in buckets.items():
        n = b["n"]
        if n <= 0:
            continue

        wins = b["wins"]
        pnl_sum = b["pnl"]
        winrate = wins / n
        avg_pnl = pnl_sum / n

        expectancy = avg_pnl
        confidence = min(1.0, n / float(DEFAULT_MIN_N))

        rows.append({
            "bucket_key": {
                "setup_type": setup_type,
                "timeframe": timeframe,
                "symbol": symbol,
            },
            "n": n,
            "wins": wins,
            "winrate": winrate,
            "pnl_usd": pnl_sum,
            "avg_pnl": avg_pnl,
            "expectancy": expectancy,
            "confidence": confidence,
        })

        total_outcomes += n

    rows.sort(key=lambda r: (r["n"], r["expectancy"]), reverse=True)

    scoreboard = {
        "schema_version": "scoreboard.v1",
        "generated_ts_ms": int(time.time() * 1000),
        "generated_from": str(IN_PATH).replace("\\", "/"),
        "min_n": DEFAULT_MIN_N,
        "buckets": rows,
    }

    OUT_SCOREBOARD.parent.mkdir(parents=True, exist_ok=True)
    OUT_AI_MEMORY.parent.mkdir(parents=True, exist_ok=True)

    OUT_SCOREBOARD.write_text(json.dumps(scoreboard, indent=2), encoding="utf-8")
    OUT_AI_MEMORY.write_text(json.dumps(scoreboard, indent=2), encoding="utf-8")

    print("=== SCOREBOARD_V0 BUILT ===")
    print(f"rows={len(rows)} total_outcomes={total_outcomes}")
    print(f"out={OUT_SCOREBOARD}")
    print(f"mirror={OUT_AI_MEMORY}")

    print("")
    print("N    EXPECT   WINRATE   SETUP     TF   SYMBOL")
    for r in rows[:10]:
        print(
            f"{r['n']:<4d} "
            f"{r['expectancy']:>7.2f}   "
            f"{r['winrate']:>6.1%}   "
            f"{r['bucket_key']['setup_type']:<8} "
            f"{r['bucket_key']['timeframe']:<4} "
            f"{r['bucket_key']['symbol']}"
        )


if __name__ == "__main__":
    main()
