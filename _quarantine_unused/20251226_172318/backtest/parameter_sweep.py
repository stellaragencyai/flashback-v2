#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Parameter Sweep Optimization Engine

Sweeps over combinations of strategy thresholds
and evaluates performance using the backtester.
"""

import itertools
import json
from pathlib import Path

from backtest.runner import run_backtest

# ------------------------------------------------------------
# CONFIGURE SWEEP RANGES
# ------------------------------------------------------------

ENTRY_THRESHOLDS = [0.50, 0.55, 0.60, 0.65, 0.70]
EXIT_THRESHOLDS = [0.30, 0.35, 0.40, 0.45]

# initial equity
INITIAL_BALANCE = 100000

# where to put results
REPORT_DIR = Path("reports/parameter_sweep")
REPORT_DIR.mkdir(parents=True, exist_ok=True)
OUTFILE = REPORT_DIR / "sweep_results.jsonl"

# ------------------------------------------------------------
# TARGET SETTINGS (example)
# ------------------------------------------------------------
SYMBOL = "BTCUSDT"
TIMEFRAME = "15m"
CANDLES_PATH = Path("data/BTCUSDT_15m.parquet")  # adapt as needed

def record_summary(summary: dict):
    with OUTFILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(summary) + "\n")

def sweep():
    print("Starting parameter sweep optimization...")

    combinations = list(itertools.product(ENTRY_THRESHOLDS, EXIT_THRESHOLDS))

    for entry, exit_ in combinations:
        cfg = {
            "entry_threshold": entry,
            "exit_threshold": exit_,
        }

        print(f"Testing entry={entry}, exit={exit_} ...")

        try:
            results = run_backtest(
                symbol=SYMBOL,
                timeframe=TIMEFRAME,
                prices_file=str(CANDLES_PATH),
                strategy_config=cfg,
                model=None,  # model still optional; or load one if needed
                initial_balance=INITIAL_BALANCE,
            )
        except Exception as e:
            print(f"  ❌ backtest failed: {e}")
            continue

        summary = {
            "entry_threshold": entry,
            "exit_threshold": exit_,
            "win_rate": results.get("win_rate"),
            "expectancy": results.get("expectancy"),
            "total_pnl": results.get("total_pnl"),
            "num_trades": results.get("num_trades"),
        }
        record_summary(summary)
        print(f"  ✅ done: pnl={summary['total_pnl']}, win_rate={summary['win_rate']:.2%}")

    print("Parameter sweep completed.")

if __name__ == "__main__":
    sweep()
