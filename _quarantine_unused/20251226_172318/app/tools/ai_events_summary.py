#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Events Summary Tool

Purpose
-------
Read the JSONL AI events from:

    state/ai_events/setups.jsonl
    state/ai_events/outcomes.jsonl

and print a simple summary so you can see:

    - How many setups have been logged
    - How many outcomes
    - Basic linkage by setup_id when available
    - Aggregate stats: wins/losses, avg R, total PnL

This is READ-ONLY and has NO effect on trading or AI behavior.
It's the first lens into your AI "memory."
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple

import orjson

try:
    from app.core.config import settings
    ROOT: Path = settings.ROOT  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

STATE_DIR = ROOT / "state"
AI_EVENTS_DIR = STATE_DIR / "ai_events"
SETUPS_PATH = AI_EVENTS_DIR / "setups.jsonl"
OUTCOMES_PATH = AI_EVENTS_DIR / "outcomes.jsonl"


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        print(f"[ai_events_summary] No file found at {path}")
        return []

    events: List[Dict[str, Any]] = []
    try:
        with path.open("rb") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = orjson.loads(line)
                    if isinstance(obj, dict):
                        events.append(obj)
                except Exception as exc:
                    print(f"[ai_events_summary] WARNING: failed to parse line in {path}: {exc}")
    except Exception as exc:
        print(f"[ai_events_summary] ERROR reading {path}: {exc}")
        return []

    return events


def _index_outcomes_by_setup_id(
    outcomes: List[Dict[str, Any]]
) -> Dict[str, List[Dict[str, Any]]]:
    by_setup: Dict[str, List[Dict[str, Any]]] = {}
    for out in outcomes:
        setup_id = str(out.get("setup_id") or "")
        if not setup_id:
            # Some outcomes may not have setup_id; skip them for linkage.
            continue
        by_setup.setdefault(setup_id, []).append(out)
    return by_setup


def _compute_basic_stats(
    outcomes: List[Dict[str, Any]]
) -> Tuple[int, int, int, float, float]:
    """
    Returns:
        total_outcomes
        num_wins
        num_losses
        avg_r
        total_pnl
    """
    total_outcomes = len(outcomes)
    if total_outcomes == 0:
        return 0, 0, 0, 0.0, 0.0

    wins = 0
    losses = 0
    sum_r = 0.0
    cnt_r = 0
    total_pnl = 0.0

    for out in outcomes:
        outcome = str(out.get("outcome") or "").upper()
        r_val = out.get("realized_R") or out.get("r_multiple")
        pnl_val = out.get("realized_pnl_usd") or out.get("realized_pnl")

        if outcome.startswith("WIN") or outcome in ("TP", "TAKE_PROFIT"):
            wins += 1
        elif outcome.startswith("LOSS") or outcome in ("SL", "STOP_LOSS"):
            losses += 1

        try:
            if r_val is not None:
                r_f = float(r_val)
                sum_r += r_f
                cnt_r += 1
        except Exception:
            pass

        try:
            if pnl_val is not None:
                total_pnl += float(pnl_val)
        except Exception:
            pass

    avg_r = (sum_r / cnt_r) if cnt_r > 0 else 0.0
    return total_outcomes, wins, losses, avg_r, total_pnl


def main() -> None:
    print(f"[ai_events_summary] ROOT:        {ROOT}")
    print(f"[ai_events_summary] EVENTS DIR:  {AI_EVENTS_DIR}")
    print(f"[ai_events_summary] SETUPS PATH: {SETUPS_PATH}")
    print(f"[ai_events_summary] OUTS PATH:   {OUTCOMES_PATH}")
    print("")

    setups = _load_jsonl(SETUPS_PATH)
    outcomes = _load_jsonl(OUTCOMES_PATH)

    print(f"[ai_events_summary] Loaded {len(setups)} setup events")
    print(f"[ai_events_summary] Loaded {len(outcomes)} outcome events")
    print("")

    if not setups and not outcomes:
        print("[ai_events_summary] No events to summarize yet.")
        return

    # Index outcomes by setup_id
    outcomes_by_setup = _index_outcomes_by_setup_id(outcomes)

    linked = 0
    unlinked = 0
    for s in setups:
        sid = str(s.get("signal_id") or s.get("setup_id") or "")
        if not sid:
            continue
        if sid in outcomes_by_setup:
            linked += 1
        else:
            unlinked += 1

    print(f"[ai_events_summary] Linked setups (have outcomes):   {linked}")
    print(f"[ai_events_summary] Unlinked setups (no outcomes):   {unlinked}")
    print("")

    total_outcomes, wins, losses, avg_r, total_pnl = _compute_basic_stats(outcomes)

    print(f"[ai_events_summary] Total outcomes: {total_outcomes}")
    print(f"[ai_events_summary] Wins:           {wins}")
    print(f"[ai_events_summary] Losses:         {losses}")
    print(f"[ai_events_summary] Win rate:       {(wins / total_outcomes * 100.0) if total_outcomes else 0.0:.2f}%")
    print(f"[ai_events_summary] Avg R (if any): {avg_r:.3f}")
    print(f"[ai_events_summary] Total PnL:      {total_pnl:.2f}")
    print("")
    print("[ai_events_summary] Done. This is your first lens on AI setup/outcome history.")


if __name__ == "__main__":
    main()
