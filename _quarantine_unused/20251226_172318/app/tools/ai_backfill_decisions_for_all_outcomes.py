#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Decision Backfill for ALL outcomes (Phase 5 unblock) v5

What it does
------------
- Scans state/ai_events/outcomes.jsonl for:
    event_type in {"outcome_enriched", "outcome_record"}
- For each trade_id found:
    - If ai_decisions.jsonl has no decision indexed by:
        trade_id OR client_trade_id OR source_trade_id
      then emit a minimal BLOCKED_BY_GATES decision WITH context.

HARD RULE (single-writer law)
-----------------------------
- This tool MUST NOT write directly to state/ai_decisions.jsonl.
- All decision writes go through app.core.ai_decision_logger.append_decision.

Usage
-----
python app/tools/ai_backfill_decisions_for_all_outcomes.py
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Set, Tuple

import orjson

# ✅ Canonical decision writer (single-writer law)
try:
    from app.core.ai_decision_logger import append_decision as log_decision
except Exception:  # pragma: no cover
    def log_decision(_: Dict[str, Any]) -> None:  # type: ignore
        return


ROOT = Path(__file__).resolve().parents[2]
STATE_DIR = ROOT / "state"
AI_DIR = STATE_DIR / "ai_events"

OUTCOMES_PATH = AI_DIR / "outcomes.jsonl"
DECISIONS_PATH = STATE_DIR / "ai_decisions.jsonl"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _iter_jsonl(path: Path) -> Iterator[Dict[str, Any]]:
    if not path.exists():
        return
    with path.open("rb") as f:
        for line in f:
            raw = line.strip()
            if not raw or raw[:1] != b"{":
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


def _extract_trade_id(ev: Dict[str, Any]) -> str:
    tid = ev.get("trade_id")
    if isinstance(tid, str) and tid.strip():
        return tid.strip()
    if tid is not None:
        return str(tid).strip()
    return ""


def _extract_context(ev: Dict[str, Any]) -> Tuple[str, str, str, Optional[str]]:
    """
    Returns: (account_label, symbol, timeframe, policy_hash)
    Best-effort across outcome shapes:
      - outcome_enriched: top-level + setup/setup_context + policy
      - outcome_record: top-level + payload.extra hints (sometimes)
    """
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

        payload = setup.get("payload") if isinstance(setup.get("payload"), dict) else None
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

    payload_o = ev.get("payload") if isinstance(ev.get("payload"), dict) else {}
    extra_o = payload_o.get("extra") if isinstance(payload_o.get("extra"), dict) else {}
    if not timeframe:
        tfh = _safe_str(extra_o.get("timeframe_hint") or extra_o.get("timeframe"))
        if tfh:
            timeframe = tfh

    return (account_label, symbol, timeframe, policy_hash)


def _decision_ids_index() -> Set[str]:
    """
    Build an index of all known decision IDs:
      trade_id, client_trade_id, source_trade_id
    """
    ids: Set[str] = set()
    if not DECISIONS_PATH.exists():
        return ids

    for d in _iter_jsonl(DECISIONS_PATH):
        for k in ("trade_id", "client_trade_id", "source_trade_id"):
            v = d.get(k)
            if isinstance(v, str) and v.strip():
                ids.add(v.strip())
    return ids


def main() -> None:
    print("=== Decision Backfill v5 (ALL outcomes -> ai_decisions via logger) ===")
    print("outcomes :", OUTCOMES_PATH)
    print("decisions:", DECISIONS_PATH)

    if not OUTCOMES_PATH.exists():
        print("FAIL: outcomes.jsonl missing")
        return

    existing = _decision_ids_index()
    print("existing_decision_ids:", len(existing))

    needed: Dict[str, Tuple[str, str, str, Optional[str], str]] = {}
    sample = []

    for ev in _iter_jsonl(OUTCOMES_PATH):
        et = _safe_str(ev.get("event_type"))
        if et not in ("outcome_enriched", "outcome_record"):
            continue

        tid = _extract_trade_id(ev)
        if not tid:
            continue

        if tid in existing:
            continue

        account_label, symbol, timeframe, policy_hash = _extract_context(ev)
        needed[tid] = (account_label, symbol, timeframe, policy_hash, et)

        if len(sample) < 10:
            sample.append(tid)

    print("missing_decisions_for_outcomes:", len(needed))
    if sample:
        print("sample_missing:", sample)

    wrote = 0
    now = _now_ms()

    for tid in sorted(needed.keys()):
        account_label, symbol, timeframe, policy_hash, etype = needed[tid]

        payload: Dict[str, Any] = {
            "event_type": "ai_decision",
            "schema_version": 1,
            "ts_ms": now,
            "trade_id": tid,
            "client_trade_id": tid,
            "account_label": account_label,
            "symbol": symbol,
            "timeframe": timeframe,
            "policy_hash": policy_hash,
            "decision": "BLOCKED_BY_GATES",
            "decision_code": "BLOCKED_BY_GATES",
            "tier_used": "NONE",
            "memory": None,
            "gates": {"reason": f"backfill_for_outcome_join::{etype}"},
            "proposed_action": None,
            "size_multiplier": 1.0,
            "allow": False,
            "reason": f"backfill_for_outcome_join::{etype}",
            "mode": "BACKFILL",
            "extra": {"stage": "backfill", "source_outcome_event_type": etype},
            "meta": {"source": "ai_backfill_decisions_for_all_outcomes", "stage": "backfill"},
        }

        # ✅ single-writer law
        log_decision(payload)

        wrote += 1
        now += 1

    print("backfill_written:", wrote)
    print("DONE")


if __name__ == "__main__":
    main()
