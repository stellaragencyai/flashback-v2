#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Decision Backfill for outcome_enriched (Phase 5 unblock) v3.1 ✅

ENFORCEMENT:
- Single-writer law: MUST NOT write directly to state/ai_decisions.jsonl.
- All writes go through app.core.ai_decision_logger.append_decision.
- If logger import fails, tool fails closed (no raw append fallback).

What it does
------------
- Scans state/ai_events/outcomes.jsonl for event_type="outcome_enriched"
- For each trade_id found:
    - If ai_decisions.jsonl has no decision for that trade_id:
        append a minimal BLOCK decision with context.

Usage
-----
python .\app\tools\ai_backfill_decisions_for_enriched_outcomes.py
"""

from __future__ import annotations

import sys
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
            if raw[:1] != b"{":
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
    account_label = _safe_str(ev.get("account_label"))
    symbol = _safe_str(ev.get("symbol")).upper()
    timeframe = _safe_str(ev.get("timeframe"))

    policy_hash: Optional[str] = None
    pol = ev.get("policy")
    if isinstance(pol, dict):
        ph = _safe_str(pol.get("policy_hash"))
        if ph:
            policy_hash = ph

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

        payload = setup.get("payload")
        if not timeframe and isinstance(payload, dict):
            extra = payload.get("extra")
            if isinstance(extra, dict):
                tf = _safe_str(extra.get("timeframe"))
                if tf:
                    timeframe = tf

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

    return (account_label, symbol, timeframe, policy_hash)


def _decisions_trade_ids() -> Set[str]:
    s: Set[str] = set()
    if not DECISIONS_PATH.exists():
        return s
    for ev in _iter_jsonl(DECISIONS_PATH):
        tid = _safe_str(ev.get("trade_id"))
        if tid:
            s.add(tid)
    return s


try:
    from app.core.ai_decision_logger import append_decision as append_decision
except Exception as e:
    append_decision = None  # type: ignore
    _IMPORT_ERR = e
else:
    _IMPORT_ERR = None


def _require_logger() -> None:
    if append_decision is not None:
        return
    raise RuntimeError(f"FATAL: ai_decision_logger.append_decision unavailable (single-writer law). err={_IMPORT_ERR!r}")


def main() -> None:
    print("=== Decision Backfill v3.1 (outcome_enriched -> ai_decisions) ===")
    print("outcomes :", OUTCOMES_PATH)
    print("decisions:", DECISIONS_PATH)

    _require_logger()

    if not OUTCOMES_PATH.exists():
        print("FAIL: outcomes.jsonl missing")
        return

    existing = _decisions_trade_ids()
    print("existing_decisions_trade_ids:", len(existing))

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

        payload: Dict[str, Any] = {
            "ts_ms": now,
            "event_type": "ai_decision",
            "trade_id": tid,
            "client_trade_id": tid,
            "source_trade_id": None,
            "account_label": account_label,
            "symbol": symbol,
            "timeframe": timeframe,
            "policy_hash": policy_hash,
            "allow": False,
            "decision_code": "BLOCKED_BY_GATES",
            "reason": "backfill_for_memory_builder",
            "tier_used": "NONE",
            "gates": {"reason": "backfill_for_memory_builder"},
            "memory": None,
            "size_multiplier": 1.0,
            "mode": "BACKFILL",
            "extra": {"stage": "backfill", "backfill_source": "outcome_enriched"},
        }

        append_decision(payload)  # type: ignore[misc]
        wrote += 1
        now += 1

    print("backfill_written:", wrote)
    print("DONE")


if __name__ == "__main__":
    main()
