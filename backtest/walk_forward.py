#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Sliding / Walk-Forward Backtest Runner

Splits market history into multiple rolling windows
and runs backtest on each, accumulating results.
"""

import pandas as pd
import json
from pathlib import Path
from typing import List

from backtest.runner import run_backtest
from backtest.loader import load_candles

# ------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------
SYMBOL = "BTCUSDT"
TIMEFRAME = "15m"
HIST_FILE = Path(f"data/{SYMBOL}_{TIMEFRAME}.parquet")

WINDOW_TRADE_DAYS = 30       # test window in days
WINDOW_TRAIN_DAYS = 90       # train look-back if using an AI model
STEP_DAYS = 30               # slide by 30 days

REPORT_DIR = Path("reports/walk_forward")
REPORT_DIR.mkdir(parents=True, exist_ok=True)
OUTFILE = REPORT_DIR / "walk_forward_summary.jsonl"

INITIAL_BALANCE = 100000

def record_summary(summary: dict):
    with OUTFILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(summary) + "\n")

def generate_windows(df: pd.DataFrame, days_train: int, days_trade: int, step: int) -> List[dict]:
    windows = []
    start = df["ts"].min()
    end = df["ts"].max()

    current_start = start
    while current_start + pd.Timedelta(days=days_train + days_trade) <= end:
        train_start = current_start
        train_end = train_start + pd.Timedelta(days=days_train)
        trade_end = train_end + pd.Timedelta(days=days_trade)

        windows.append({
            "train": (train_start, train_end),
            "trade": (train_end, trade_end),
        })
        current_start += pd.Timedelta(days=step)
    return windows

def run_walk_forward():
    print("Loading history...")
    df = load_candles(str(HIST_FILE))
    if df.empty:
        print("No data!")
        return

    df["ts"] = pd.to_datetime(df["ts"], unit="ms") if df["ts"].dtype == "int64" else df["ts"]

    windows = generate_windows(df, WINDOW_TRAIN_DAYS, WINDOW_TRADE_DAYS, STEP_DAYS)
    print(f"{len(windows)} walk-forward windows generated.")

    for i, w in enumerate(windows):
        train_slice = df[(df["ts"] >= w["train"][0]) & (df["ts"] < w["train"][1])]
        trade_slice = df[(df["ts"] >= w["trade"][0]) & (df["ts"] < w["trade"][1])]

        print(f"\nWindow {i+1}: trade {w['trade']}")
        trade_file = REPORT_DIR / f"temp_trade_{i}.parquet"
        trade_slice.to_parquet(trade_file, index=False)

        # Insert model retraining here if using AI
        # Example: train_model(train_slice)

        # Run backtest on trade window
        results = run_backtest(
            symbol=SYMBOL,
            timeframe=TIMEFRAME,
            prices_file=str(trade_file),
            strategy_config={"entry_threshold": 0.6, "exit_threshold": 0.35},
            model=None,
            initial_balance=INITIAL_BALANCE,
        )

        summary = {
            "window_index": i,
            "train_start": str(w["train"][0]),
            "train_end": str(w["train"][1]),
            "trade_start": str(w["trade"][0]),
            "trade_end": str(w["trade"][1]),
            **results,
        }
        record_summary(summary)
        print(f"  results: pnl={results.get('total_pnl')}")

    print("Walk-forward run complete.")

if __name__ == "__main__":
    run_walk_forward()
