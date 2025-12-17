#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Streamlit Dashboard — Backtest Results Explorer

Run:
    streamlit run streamlit_app.py
"""

import streamlit as st
import pandas as pd
import json
from pathlib import Path

# ---------------------------------------------------
# CONFIG — change if your JSONL file is at a
# different path
# ---------------------------------------------------
BACKTEST_RESULTS_PATH = Path("reports/backtest/batch_backtest_summary.jsonl")

# ---------------------------------------------------
# LOAD DATA
# ---------------------------------------------------

@st.cache_data
def load_backtest_data(jsonl_path: Path) -> pd.DataFrame:
    if not jsonl_path.exists():
        st.error(f"Backtest results not found at: {jsonl_path}")
        return pd.DataFrame()

    rows = []
    with jsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    return df


df = load_backtest_data(BACKTEST_RESULTS_PATH)

if df.empty:
    st.warning("No backtest data to display.")
    st.stop()

# ---------------------------------------------------
# FILTERS
# ---------------------------------------------------
st.sidebar.header("Filters")

strategies = df["strategy_name"].unique().tolist()
selected_strategies = st.sidebar.multiselect(
    "Select strategies", strategies, default=strategies
)

symbols = df["symbol"].unique().tolist()
selected_symbols = st.sidebar.multiselect(
    "Select symbols", symbols, default=symbols
)

timeframes = df["timeframe"].unique().tolist()
selected_timeframes = st.sidebar.multiselect(
    "Select timeframes", timeframes, default=timeframes
)

filtered = df[
    (df["strategy_name"].isin(selected_strategies)) &
    (df["symbol"].isin(selected_symbols)) &
    (df["timeframe"].isin(selected_timeframes))
]

st.title("📊 Flashback Backtest Results Dashboard")

st.markdown(
    """
    Use the filters on the left to explore performance across strategies, symbols, and timeframes.
    """
)

# ---------------------------------------------------
# SUMMARY METRICS
# ---------------------------------------------------
if not filtered.empty:
    st.header("🧠 Aggregate Summary")
    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Total Backtests", len(filtered))
    col2.metric("Average Win Rate", f"{filtered['win_rate'].mean():.2%}")
    col3.metric("Average Expectancy", f"{filtered['expectancy'].mean():.4f}")
    col4.metric("Total PnL", f"{filtered['total_pnl'].sum():,.2f}")

# ---------------------------------------------------
# TABLE VIEW
# ---------------------------------------------------
st.header("📋 Backtest Results Table")
st.dataframe(filtered)

# ---------------------------------------------------
# BAR CHARTS
# ---------------------------------------------------
st.header("📈 Metric Comparisons")

chart_metric = st.selectbox(
    "Choose metric to compare",
    ["win_rate", "expectancy", "total_pnl", "num_trades"],
)

if chart_metric:
    st.bar_chart(
        filtered.set_index("strategy_name")[chart_metric].sort_values(ascending=False)
    )

# ---------------------------------------------------
# TIMEFRAME BREAKDOWN PER STRATEGY
# ---------------------------------------------------
st.header("📊 Strategy vs Timeframe")

grouped = (
    filtered.groupby(["strategy_name", "timeframe"])
    .agg({
        "win_rate": "mean",
        "expectancy": "mean",
        "total_pnl": "sum",
        "num_trades": "sum"
    })
    .reset_index()
)

st.dataframe(grouped)

# ---------------------------------------------------
# OPTIONAL: Download filtered dataset
# ---------------------------------------------------
st.header("⬇ Export Filtered Data")

csv = filtered.to_csv(index=False)
st.download_button(
    label="Download as CSV",
    data=csv,
    file_name="flashback_backtest_filtered.csv",
    mime="text/csv"
)

st.markdown("---")
st.caption("Flashback Backtest Dashboard — powered by Streamlit")
