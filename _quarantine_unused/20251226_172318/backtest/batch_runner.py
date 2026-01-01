#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Batch Backtest Runner for Flashback

Loops through all enabled strategies,
runs backtests per symbol/timeframe,
and outputs summary results.
"""

from pathlib import Path
import json
import joblib
import pandas as pd

from app.core.strategies import enabled_strategies
from backtest.runner import run_backtest
from backtest.loader import load_candles
from app.ai_training.metrics_logger import log_model_metrics

# ---------------------------------------------------------------
# CONFIG — adjust these paths to match your data layout
# ---------------------------------------------------------------

HIST_DATA_ROOT = Path("data")         # Where your historical candles are stored
REPORTS_ROOT = Path("reports/backtest")
REPORTS_ROOT.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------
# UTILS
# ---------------------------------------------------------------

def get_candles_path(symbol: str, tf: str) -> str:
    """
    You must adapt this to match your file naming convention.
    Example:
        data/BTCUSDT_15m.parquet
        data/ETHUSDT_1h.parquet
    """
    fn = f"{symbol}_{tf}.parquet"
    path = HIST_DATA_ROOT / fn
    if not path.exists():
        raise FileNotFoundError(f"Missing historical file: {path}")
    return str(path)


def load_model_for_strategy(strategy) -> object | None:
    """
    Load the trained model for this strategy if exists.
    You must adapt to your own model storage convention.
    Example:
        models/trend_v1_v2025-12-18a.pkl
    """
    model_id = strategy.ai_profile
    model_dir = Path("models")
    # You can choose latest by version, timestamp, etc.
    # Simple heuristic: find file starting with model_id
    files = list(model_dir.glob(f"{model_id}*"))
    if not files:
        print(f"[WARN] No model found for {model_id}")
        return None
    # Pick the latest mod time
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return joblib.load(str(files[0]))


def record_summary(summary: dict, out_file: Path):
    with out_file.open("a", encoding="utf-8") as f:
        f.write(json.dumps(summary) + "\n")

# ---------------------------------------------------------------
# MAIN BATCH PROCESS
# ---------------------------------------------------------------

def main():
    strategies = enabled_strategies()
    print(f"Running backtests for {len(strategies)} strategies...")

    for strat in strategies:
        # Strategy metadata
        sid = strat.strategy_name
        sub_uid = strat.sub_uid
        symbols = strat.symbols
        timeframes = strat.timeframes

        print(f"\n--- Backtesting {sid} (sub={sub_uid}) ---")

        # Load model (if available)
        model = load_model_for_strategy(strat)
        if model is None:
            print(f"Skipping model load for {sid} — using rule only")
        
        for sym in symbols:
            for tf in timeframes:
                # Adapt timeframe representation ("15" -> "15m")
                tf_label = f"{tf}m"
                try:
                    prices_file = get_candles_path(sym, tf_label)
                except Exception as e:
                    print(f"  ❌ missing history for {sym} {tf_label}: {e}")
                    continue

                print(f"  Backtesting -> {sym} @ {tf_label}")

                # Build a simple strategy config for backtest
                strat_cfg = {
                    "entry_threshold": getattr(strat, "entry_threshold", 0.5),
                    "exit_threshold": getattr(strat, "exit_threshold", 0.3),
                    # add any other needed backtest params
                }

                try:
                    results = run_backtest(
                        symbol=sym,
                        timeframe=tf_label,
                        prices_file=prices_file,
                        strategy_config=strat_cfg,
                        model=model,
                        initial_balance=100000,
                    )
                except Exception as e:
                    print(f"    ❌ backtest failed: {e}")
                    continue

                # Prepare summary
                summary = {
                    "strategy_name": sid,
                    "sub_uid": sub_uid,
                    "symbol": sym,
                    "timeframe": tf_label,
                    **results,
                }

                # Record to file
                out_file = REPORTS_ROOT / f"batch_backtest_summary.jsonl"
                record_summary(summary, out_file)

                # Optionally log into AI metrics store
                try:
                    # You must build y_true, y_pred, y_score arrays from results if you have them
                    # For now we only log totals + expectancy
                    log_model_metrics(
                        model_id=strat.ai_profile,
                        model_version=getattr(model, "version", "unknown"),
                        strategy_id=sid,
                        sub_uid=str(sub_uid),
                        y_true=[],
                        y_pred=[],
                        y_score=[],
                        extra={
                            "win_rate": results.get("win_rate"),
                            "expectancy": results.get("expectancy"),
                            "total_pnl": results.get("total_pnl"),
                            "num_trades": results.get("num_trades"),
                        },
                    )
                except Exception as e:
                    print(f"    ⚠ metrics log failed: {e}")

                print(f"    ✅ Done: wins={results.get('win_rate'):.2%}, pnl={results.get('total_pnl')}")

    print("\nBatch backtest run complete.")

if __name__ == "__main__":
    main()
