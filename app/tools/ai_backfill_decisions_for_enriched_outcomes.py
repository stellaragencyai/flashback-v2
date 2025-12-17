#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Decision Backfill for outcome_enriched (Phase 5 unblock) v2

What it does
------------
- Scans state/ai_events/outcomes.jsonl for event_type="outcome_enriched"
- For each trade_id found:
    - If ai_decisions.jsonl has no decision for that trade_id:
        append a minimal BLOCKED_BY_GATES decision WITH context (account_label/symbol/etc)

Why
---
Your join pipeline needs decision(trade_id) for each outcome_enriched(trade_id).
Backfill should complete the contract without corrupting learning.

Usage
-----
python app/tools/ai_backfill_decisions_for_enriched_outcomes.py
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Set, Tuple

import orjson

ROOT = Path(__file__).resolve().parents[2]
STATE_DIR = ROOT / "state"
OUTCOMES_PATH = STATE_DIR / "ai_events" / "outcomes.jsonl"
DECISIONS_PATH = STATE_DIR / "ai_decisions.jsonl"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _iter_jsonl(path: Path) -> Iterator[Dict[str, Any]]:
    if not path.exists():
        return
    with path.open("rb") as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue
            try:
                obj = orjson.loads(raw)
            except Exception:
                continue
            if isinstance(obj, dict):
                yield obj


def _safe_str(x: Any) -> str:
    try:
        return ("" if x is None else str(x)).strip()
    except Exception:
        return ""


def _extract_outcome_context(ev: Dict[str, Any]) -> Tuple[str, str, str, Optional[str]]:
    """
    Returns: (account_label, symbol, timeframe, policy_hash)
    Best-effort across outcome_enriched shapes.
    """
    account_label = _safe_str(ev.get("account_label"))
    symbol = _safe_str(ev.get("symbol")).upper()
    timeframe = _safe_str(ev.get("timeframe"))

    # outcome_enriched often carries policy in ev["policy"]["policy_hash"]
    policy_hash = None
    pol = ev.get("policy")
    if isinstance(pol, dict):
        ph = _safe_str(pol.get("policy_hash"))
        if ph:
            policy_hash = ph

    # sometimes nested under setup/setup_context
    setup = ev.get("setup")
    if isinstance(setup, dict):
        if not account_label:
            account_label = _safe_str(setup.get("account_label"))
        if not symbol:
            symbol = _safe_str(setup.get("symbol")).upper()
        if not timeframe:
            timeframe = _safe_str(setup.get("timeframe"))

        pol2 = setup.get("policy")
        if policy_hash is None and isinstance(pol2, dict):
            ph2 = _safe_str(pol2.get("policy_hash"))
            if ph2:
                policy_hash = ph2

    setup_ctx = ev.get("setup_context")
    if isinstance(setup_ctx, dict):
        if not account_label:
            account_label = _safe_str(setup_ctx.get("account_label"))
        if not symbol:
            symbol = _safe_str(setup_ctx.get("symbol")).upper()
        if not timeframe:
            timeframe = _safe_str(setup_ctx.get("timeframe"))

        pol3 = setup_ctx.get("policy")
        if policy_hash is None and isinstance(pol3, dict):
            ph3 = _safe_str(pol3.get("policy_hash"))
            if ph3:
                policy_hash = ph3

    # Common: outcome_enriched has setup.payload.extra.timeframe
    payload = setup.get("payload") if isinstance(setup, dict) else None
    if not timeframe and isinstance(payload, dict):
        extra = payload.get("extra")
        if isinstance(extra, dict):
            tf = _safe_str(extra.get("timeframe"))
            if tf:
                timeframe = tf

    return (account_label, symbol, timeframe, policy_hash)


def _decisions_trade_ids() -> Set[str]:
    s: Set[str] = set()
    for ev in _iter_jsonl(DECISIONS_PATH):
        tid = _safe_str(ev.get("trade_id"))
        if tid:
            s.add(tid)
    return s


def _append_decision(payload: Dict[str, Any]) -> None:
    DECISIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with DECISIONS_PATH.open("ab") as f:
        f.write(orjson.dumps(payload))
        f.write(b"\n")


def main() -> None:
    print("=== Decision Backfill v2 (outcome_enriched -> ai_decisions) ===")
    print("outcomes :", OUTCOMES_PATH)
    print("decisions:", DECISIONS_PATH)

    if not OUTCOMES_PATH.exists():
        print("FAIL: outcomes.jsonl missing")
        return

    existing = _decisions_trade_ids()
    print("existing_decisions_trade_ids:", len(existing))

    # collect needed trade_ids + context
    needed: Dict[str, Tuple[str, str, str, Optional[str]]] = {}
    sample = []

    for ev in _iter_jsonl(OUTCOMES_PATH):
        if ev.get("event_type") != "outcome_enriched":
            continue
        tid = _safe_str(ev.get("trade_id"))
        if not tid:
            continue
        if tid in existing:
            continue

        ctx = _extract_outcome_context(ev)
        needed[tid] = ctx

        if len(sample) < 10:
            sample.append(tid)

    print("missing_decisions_for_enriched:", len(needed))
    if sample:
        print("sample_missing:", sample)

    wrote = 0
    now = _now_ms()
    for tid in sorted(needed.keys()):
        account_label, symbol, timeframe, policy_hash = needed[tid]

        # Minimal contract-ish decision. We intentionally BLOCK to be safe.
        payload: Dict[str, Any] = {
            "schema_version": 1,
            "ts": now,
            "ts_ms": now,
            "trade_id": tid,
            "client_trade_id": tid,
            "account_label": account_label,
            "symbol": symbol,
            "timeframe": timeframe,
            "policy_hash": policy_hash,
            "decision": "BLOCKED_BY_GATES",
            "tier_used": "NONE",
            "memory": None,
            "gates": {"reason": "backfill_for_memory_builder"},
            "proposed_action": None,
            "size_multiplier": 1.0,
            "allow": False,
            "reason": "backfill_for_memory_builder",
            "mode": "BACKFILL",
        }

        _append_decision(payload)
        wrote += 1
        # bump ts for deterministic ordering if many lines append in same ms
        now += 1

    print("backfill_written:", wrote)
    print("DONE")


if __name__ == "__main__":
    main()
