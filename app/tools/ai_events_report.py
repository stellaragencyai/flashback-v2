#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Events Report (setups vs outcomes)

Purpose
-------
Offline inspector for the AI events spine:

    state/ai_events/setups.jsonl
    state/ai_events/outcomes.jsonl

These are written by app.ai.ai_events.publish_ai_event(...)
when called from executor_v2 (setups) and, later, from
wherever you log trade outcomes.

This tool lets you answer:

    - How many setups have we logged?
    - How many have matching outcomes (by trade_id)?
    - Per-strategy coverage (setups vs outcomes)?
    - Basic PnL stats per strategy when available.

Usage
-----
From project root:

    python -m app.tools.ai_events_report

Optionally filter to a single strategy:

    python -m app.tools.ai_events_report "Sub1_Trend"

or, if your strategy names include sub labels:

    python -m app.tools.ai_events_report "Sub1_Trend (sub 524630315)"

Schema expectations (soft)
--------------------------
We try hard to NOT require a rigid schema. We look for:

Setups (setups.jsonl):
    trade_id        : str
    symbol          : str (optional but recommended)
    strategy        : str (or "strategy_name")
    account_label   : str (optional)
    mode            : in features / extra, best-effort
    ...
    All other fields are ignored for aggregation.

Outcomes (outcomes.jsonl):
    trade_id        : str (join key)
    pnl_usd / pnl   : numeric (optional)
    rr              : numeric (optional)
    result / status : "win"/"loss"/"breakeven"/whatever (optional)
    ...
    All other fields are stored as raw metadata.

The join is purely on trade_id.
"""

from __future__ import annotations

import math
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# orjson preferred
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


# ---------------------------------------------------------------------------
# ROOT + paths
# ---------------------------------------------------------------------------

try:
    from app.core.config import settings  # type: ignore

    ROOT: Path = settings.ROOT  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

STATE_DIR = ROOT / "state" / "ai_events"
SETUPS_PATH = STATE_DIR / "setups.jsonl"
OUTCOMES_PATH = STATE_DIR / "outcomes.jsonl"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None


def _iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        return
    with path.open("rb") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = _loads(line)
            except Exception:
                continue
            if isinstance(obj, dict):
                yield obj


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

def _norm_setup(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalize a setup row to minimal fields we care about.
    """
    trade_id = row.get("trade_id") or row.get("id")
    if not trade_id:
        return None

    sym = row.get("symbol") or row.get("sym") or None
    strat = row.get("strategy") or row.get("strategy_name") or "unknown"
    acc = row.get("account_label") or row.get("label") or None

    # Mode can live in various places (features/extra/raw)
    mode = None
    for container_key in ("mode", "trade_mode", "execution_mode"):
        if container_key in row:
            mode = row.get(container_key)
            break

    # If not top-level, check extra/features
    if mode is None:
        extra = row.get("extra") or {}
        if isinstance(extra, dict):
            mode = extra.get("mode") or extra.get("trade_mode")

    # Fallback
    mode_str = str(mode or "UNKNOWN").upper().strip()

    return {
        "trade_id": str(trade_id),
        "symbol": str(sym).upper() if sym else None,
        "strategy": str(strat),
        "account_label": str(acc) if acc is not None else None,
        "mode": mode_str,
        "raw": row,
    }


def _norm_outcome(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalize an outcome row to minimal fields we care about.
    """
    trade_id = row.get("trade_id") or row.get("id")
    if not trade_id:
        return None

    pnl = (
        row.get("pnl_usd")
        or row.get("pnlUSD")
        or row.get("pnl")
        or row.get("profit_usd")
        or row.get("profit")
    )
    rr = row.get("rr") or row.get("r_multiple") or row.get("r")

    pnl_f = _safe_float(pnl)
    rr_f = _safe_float(rr)

    result = row.get("result") or row.get("status") or row.get("outcome")
    result = str(result or "").lower().strip() or None

    return {
        "trade_id": str(trade_id),
        "pnl_usd": pnl_f,
        "rr": rr_f,
        "result": result,
        "raw": row,
    }


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def load_setups(
    strategy_filter: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Load all setups, keyed by trade_id.
    """
    setups: Dict[str, Dict[str, Any]] = {}
    for row in _iter_jsonl(SETUPS_PATH):
        norm = _norm_setup(row)
        if not norm:
            continue
        if strategy_filter and norm["strategy"] != strategy_filter:
            continue
        tid = norm["trade_id"]
        setups[tid] = norm
    return setups


def load_outcomes() -> Dict[str, Dict[str, Any]]:
    """
    Load all outcomes, keyed by trade_id.
    """
    outcomes: Dict[str, Dict[str, Any]] = {}
    for row in _iter_jsonl(OUTCOMES_PATH):
        norm = _norm_outcome(row)
        if not norm:
            continue
        tid = norm["trade_id"]
        outcomes[tid] = norm
    return outcomes


def aggregate(
    setups: Dict[str, Dict[str, Any]],
    outcomes: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Aggregate per-strategy stats from setups + outcomes.

    Returns a dict with:
        {
          "total_setups": int,
          "total_with_outcome": int,
          "per_strategy": {
              "<strategy>": {
                  "setups": int,
                  "with_outcome": int,
                  "symbols": Counter(),
                  "modes": Counter(),
                  "wins": int,
                  "losses": int,
                  "breakeven": int,
                  "pnl_samples": [float, ...],
                  "rr_samples": [float, ...],
              },
              ...
          }
        }
    """
    per_strategy: Dict[str, Dict[str, Any]] = {}
    total_setups = 0
    total_with_outcome = 0

    for tid, s in setups.items():
        total_setups += 1
        strat = s["strategy"]
        sym = s.get("symbol")
        mode = s.get("mode") or "UNKNOWN"

        stats = per_strategy.get(strat)
        if stats is None:
            stats = {
                "setups": 0,
                "with_outcome": 0,
                "symbols": Counter(),
                "modes": Counter(),
                "wins": 0,
                "losses": 0,
                "breakeven": 0,
                "pnl_samples": [],
                "rr_samples": [],
            }
            per_strategy[strat] = stats

        stats["setups"] += 1
        stats["modes"][mode] += 1
        if sym:
            stats["symbols"][sym] += 1

        oc = outcomes.get(tid)
        if not oc:
            continue

        total_with_outcome += 1
        stats["with_outcome"] += 1

        pnl = oc.get("pnl_usd")
        rr = oc.get("rr")
        result = oc.get("result")

        if pnl is not None:
            stats["pnl_samples"].append(pnl)
            if pnl > 0:
                stats["wins"] += 1
            elif pnl < 0:
                stats["losses"] += 1
            else:
                stats["breakeven"] += 1
        else:
            # fall back to result label, if present
            if isinstance(result, str):
                if "win" in result:
                    stats["wins"] += 1
                elif "loss" in result or "lose" in result or "stopped" in result:
                    stats["losses"] += 1
                elif "flat" in result or "scratch" in result or "breakeven" in result:
                    stats["breakeven"] += 1

        if rr is not None:
            stats["rr_samples"].append(rr)

    return {
        "total_setups": total_setups,
        "total_with_outcome": total_with_outcome,
        "per_strategy": per_strategy,
    }


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def _fmt_pct(num: int, denom: int) -> str:
    if denom <= 0:
        return "0.0%"
    return f"{(100.0 * num / denom):4.1f}%"


def _fmt_samples(samples: List[float]) -> str:
    if not samples:
        return "n/a"
    mn = min(samples)
    mx = max(samples)
    avg = sum(samples) / len(samples)
    return f"min={mn:.3f}, max={mx:.3f}, avg={avg:.3f}"


def print_report(
    agg: Dict[str, Any],
    *,
    strategy_filter: Optional[str] = None,
) -> None:
    total = agg["total_setups"]
    matched = agg["total_with_outcome"]
    per_strategy: Dict[str, Dict[str, Any]] = agg["per_strategy"]

    title = "AI Events Report (setups vs outcomes)"
    if strategy_filter:
        title += f" — strategy={strategy_filter}"
    print("=" * len(title))
    print(title)
    print("=" * len(title))
    print()

    if total == 0:
        print("No setups found in:", SETUPS_PATH)
        return

    cov_pct = _fmt_pct(matched, total)
    print(f"Total setups        : {total}")
    print(f"With outcome        : {matched} ({cov_pct})")
    print()

    # Sort strategies by number of setups
    items = sorted(per_strategy.items(), key=lambda kv: kv[1]["setups"], reverse=True)

    print("Per-strategy summary")
    print("---------------------")

    for strat, stats in items:
        s_count = stats["setups"]
        s_matched = stats["with_outcome"]
        s_cov = _fmt_pct(s_matched, s_count)

        wins = stats["wins"]
        losses = stats["losses"]
        breakeven = stats["breakeven"]
        pnl_samples = stats["pnl_samples"]
        rr_samples = stats["rr_samples"]
        modes = stats["modes"]
        symbols = stats["symbols"]

        print(f"\nStrategy: {strat}")
        print(f"  setups           : {s_count}")
        print(f"  with outcome     : {s_matched} ({s_cov})")
        if s_matched > 0:
            print(f"  wins / losses / BE: {wins} / {losses} / {breakeven}")
            print(f"  pnl_usd          : {_fmt_samples(pnl_samples)}")
            print(f"  rr               : {_fmt_samples(rr_samples)}")

        if modes:
            m_str = ", ".join(f"{m}={c}" for m, c in modes.most_common())
            print(f"  modes            : {m_str}")

        if symbols:
            s_str = ", ".join(f"{sym}={c}" for sym, c in symbols.most_common(5))
            print(f"  symbols          : {s_str}")

    print()
    print("[ai_events_report] Done.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> None:
    if argv is None:
        argv = sys.argv[1:]

    strategy_filter: Optional[str] = None
    if argv:
        strategy_filter = " ".join(argv).strip() or None

    setups = load_setups(strategy_filter=strategy_filter)
    outcomes = load_outcomes()
    agg = aggregate(setups, outcomes)
    print_report(agg, strategy_filter=strategy_filter)


if __name__ == "__main__":
    main()
