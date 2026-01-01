#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Dataset Export (setups + outcomes → training rows, v1)

Purpose
-------
Merge AI setup events and enriched outcome events into a single JSONL dataset:

    state/ai_dataset/trades_enriched.jsonl

Each row is a "training-ready" trade record with:

    - identity: trade_id, account_label, symbol, strategy, timeframe, ai_profile
    - timing: ts_setup, ts_outcome, holding_ms (if available)
    - features: features from setup (if logged)
    - outcome: r_multiple, win, pnl, mfe/mae, exit_reason, regime_at_entry/exit

Input:
    state/ai_events/setups.jsonl
    state/ai_events/outcomes.jsonl

Usage:
    python app/tools/ai_dataset_export.py
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from app.core.config import settings

ROOT: Path = settings.ROOT
STATE_DIR: Path = settings.STATE_DIR
AI_EVENTS_DIR: Path = STATE_DIR / "ai_events"
AI_DATASET_DIR: Path = STATE_DIR / "ai_dataset"

SETUPS_PATH: Path = AI_EVENTS_DIR / "setups.jsonl"
OUTCOMES_PATH: Path = AI_EVENTS_DIR / "outcomes.jsonl"
OUT_PATH: Path = AI_DATASET_DIR / "trades_enriched.jsonl"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                # Skip bad lines, don't nuke the run
                continue
    return rows


def _index_setups_by_trade_id(setups: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Build a map trade_id -> latest setup row.

    If multiple setups share a trade_id, the one with the largest ts/ts_ms wins.
    """
    by_id: Dict[str, Dict[str, Any]] = {}
    for row in setups:
        tid = row.get("trade_id") or row.get("id")
        if not tid:
            continue
        tid = str(tid)

        existing = by_id.get(tid)
        if existing is None:
            by_id[tid] = row
            continue

        ts_new = row.get("ts") or row.get("ts_ms") or 0
        ts_old = existing.get("ts") or existing.get("ts_ms") or 0
        try:
            if float(ts_new) >= float(ts_old):
                by_id[tid] = row
        except Exception:
            by_id[tid] = row

    return by_id


def _pick_from(setup: Optional[Dict[str, Any]], outcome: Dict[str, Any], *keys: str) -> Optional[Any]:
    """
    Prefer outcome[key] over setup[key], first non-None wins.
    """
    for k in keys:
        if k and outcome.get(k) is not None:
            return outcome.get(k)
        if setup and setup.get(k) is not None:
            return setup.get(k)
    return None


def _extract_identity(setup: Optional[Dict[str, Any]], outcome: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build identity fields, preferring outcome values, then setup, then None.
    """
    return {
        "trade_id": _pick_from(setup, outcome, "trade_id", "id"),
        "account_label": _pick_from(setup, outcome, "account_label", "subaccount", "account"),
        "symbol": _pick_from(setup, outcome, "symbol", "sym"),
        "strategy": _pick_from(setup, outcome, "strategy", "strategy_name"),
        "timeframe": _pick_from(setup, outcome, "timeframe", "tf"),
        "ai_profile": _pick_from(setup, outcome, "ai_profile"),
        "setup_type": _pick_from(setup, outcome, "setup_type"),
    }


def _extract_features(setup: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Extract feature dict from setup. If it's not a dict, wrap it.
    """
    if not setup:
        return {}
    feats = setup.get("features") or setup.get("feature_vector") or {}
    if isinstance(feats, dict):
        return feats
    return {"_raw_features": feats}


def _extract_outcome(outcome: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize outcome-related fields into a compact block.
    """
    o = outcome
    extra = o.get("extra") or {}

    def pick(*keys: str) -> Optional[Any]:
        for k in keys:
            if k in o and o[k] is not None:
                return o[k]
            if k in extra and extra[k] is not None:
                return extra[k]
        return None

    return {
        "ts_outcome": pick("ts", "ts_ms"),
        "pnl": pick("pnl", "realized_pnl"),
        "r_multiple": pick("r_multiple", "r"),
        "win": pick("win", "is_win"),
        "mfe_r": pick("max_favorable_excursion_r", "mfe_r"),
        "mae_r": pick("max_adverse_excursion_r", "mae_r"),
        "holding_ms": pick("holding_ms", "hold_ms"),
        "exit_reason": pick("exit_reason", "exit"),
        "regime_at_entry": pick("regime_at_entry"),
        "regime_at_exit": pick("regime_at_exit"),
    }


def _extract_setup_ts(setup: Optional[Dict[str, Any]]) -> Optional[int]:
    if not setup:
        return None
    for k in ("ts", "ts_ms", "setup_ts_ms"):
        v = setup.get(k)
        if v is None:
            continue
        try:
            return int(v)
        except Exception:
            continue
    return None


# ---------------------------------------------------------------------------
# Core builder
# ---------------------------------------------------------------------------

def build_dataset() -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Merge setups + outcomes into a list of enriched trade rows,
    and return both rows and summary counts.
    """
    setups = _load_jsonl(SETUPS_PATH)
    outcomes = _load_jsonl(OUTCOMES_PATH)

    setups_by_id = _index_setups_by_trade_id(setups)

    rows: List[Dict[str, Any]] = []

    counts: Dict[str, Any] = {
        "setups": len(setups),
        "outcomes": len(outcomes),
        "joined": 0,
        "outcomes_without_setup": 0,
        "setups_without_outcome": 0,  # filled later
    }

    seen_trade_ids_with_outcome = set()

    for out in outcomes:
        et = out.get("event_type")
        if et not in ("outcome_enriched", "outcome", "trade_outcome"):
            # ignore irrelevant events
            continue

        tid = out.get("trade_id") or out.get("id")
        if not tid:
            continue
        tid = str(tid)
        seen_trade_ids_with_outcome.add(tid)

        setup = setups_by_id.get(tid)

        identity = _extract_identity(setup, out)
        features = _extract_features(setup)
        outcome_block = _extract_outcome(out)
        ts_setup = _extract_setup_ts(setup)

        row: Dict[str, Any] = {
            "trade_id": identity.get("trade_id"),
            "account_label": identity.get("account_label"),
            "symbol": identity.get("symbol"),
            "strategy": identity.get("strategy"),
            "timeframe": identity.get("timeframe"),
            "ai_profile": identity.get("ai_profile"),
            "setup_type": identity.get("setup_type"),
            "ts_setup": ts_setup,
            "features": features,
            "outcome": outcome_block,
            "has_setup": setup is not None,
        }

        rows.append(row)

        if setup is not None:
            counts["joined"] += 1
        else:
            counts["outcomes_without_setup"] += 1

    # setups that never saw an outcome
    for tid in setups_by_id.keys():
        if tid not in seen_trade_ids_with_outcome:
            counts["setups_without_outcome"] += 1

    return rows, counts


def write_dataset(rows: List[Dict[str, Any]]) -> None:
    AI_DATASET_DIR.mkdir(parents=True, exist_ok=True)
    with OUT_PATH.open("w", encoding="utf-8") as f:
        for row in rows:
            json.dump(row, f, separators=(",", ":"), ensure_ascii=False)
            f.write("\n")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main() -> int:
    print("=== Flashback AI Dataset Export ===")
    print(f"ROOT:          {ROOT}")
    print(f"STATE_DIR:     {STATE_DIR}")
    print(f"AI_EVENTS_DIR: {AI_EVENTS_DIR}")
    print(f"AI_DATASET_DIR:{AI_DATASET_DIR}")
    print(f"SETUPS_PATH:   {SETUPS_PATH}")
    print(f"OUTCOMES_PATH: {OUTCOMES_PATH}")
    print(f"OUT_PATH:      {OUT_PATH}")
    print("")

    rows, counts = build_dataset()

    write_dataset(rows)

    print("Export summary:")
    print("---------------")
    print(f"Setups:                     {counts['setups']}")
    print(f"Outcomes:                   {counts['outcomes']}")
    print(f"Joined (setup+outcome):     {counts['joined']}")
    print(f"Outcomes without setup:     {counts['outcomes_without_setup']}")
    print(f"Setups without outcome:     {counts['setups_without_outcome']}")
    print(f"Dataset rows written:       {len(rows)}")
    print("")
    print(f"[OK] Wrote dataset to {OUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
