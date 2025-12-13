#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Flashback — Seed AI State (Phase 3 bootstrap)

Writes a few synthetic JSONL rows into:
- state/features_trades.jsonl
- state/feature_store.jsonl

This is NOT for performance. It's for pipeline validation:
- health checks
- downstream readers
- Phase 4 sanity tools

Usage:
  python -m app.tools.seed_ai_state --n 5
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def now_ms() -> int:
    return int(time.time() * 1000)


def append_jsonl(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=5)
    args = ap.parse_args()

    root = repo_root()
    state = root / "state"
    f_trades = state / "features_trades.jsonl"
    f_store = state / "feature_store.jsonl"

    # Minimal “strategy” tagging so health checker can count coverage
    strategies = ["Sub1_Trend", "Sub2_Breakout", "Sub7_Canary"]

    base_ts = now_ms()

    for i in range(args.n):
        strat = strategies[i % len(strategies)]
        symbol = ["BTCUSDT", "ETHUSDT", "SOLUSDT"][i % 3]

        row = {
            "ts_ms": base_ts + i * 1000,
            "symbol": symbol,
            "timeframe": "5m",
            "strategy": strat,
            "side": "LONG" if i % 2 == 0 else "SHORT",
            "price": 100.0 + i,
            "features": {
                "atr14": 1.23,
                "adx14": 18.5,
                "vol_z": 0.7,
                "spread_bps": 2.1,
            },
            "meta": {
                "seeded": True,
                "note": "synthetic bootstrap rows for pipeline validation",
            },
        }

        # Write to BOTH files so the pipeline has something to chew on immediately
        append_jsonl(f_trades, row)
        append_jsonl(f_store, row)

    print(f"✅ Seeded {args.n} rows into:")
    print(f" - {f_trades}")
    print(f" - {f_store}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
