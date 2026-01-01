#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Patch Backfilled Decisions (Phase 5.4) v2 (context-preserving)

Purpose
-------
Appends override decisions (last-write-wins) so memory entries reflect
desired allow/block behavior for outcome_enriched trade_ids.

Critical fix
------------
Old overrides were missing account_label/symbol/timeframe/policy_hash.
That reintroduced ambiguous joins. This version preserves context.

Usage
-----
python app/tools/ai_patch_backfilled_decisions_allow.py
python app/tools/ai_patch_backfilled_decisions_allow.py --dry-run
"""

from __future__ import annotations

import argparse
import time
from typing import Any, Dict, List, Tuple

from app.ai.ai_memory_contract import (
    ContractPaths,
    iter_jsonl,
    validate_outcome_enriched,
    validate_decision_record,
)
from app.core.ai_decision_logger import append_decision  # append-only, rotation-safe


PATHS = ContractPaths.default()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_str(x: Any) -> str:
    try:
        return ("" if x is None else str(x)).strip()
    except Exception:
        return ""


def _tid_is_block_test(tid: str) -> bool:
    t = (tid or "").strip().upper()
    if not t:
        return True
    if t.startswith("TP_BLOCK"):
        return True
    if t.startswith("DEC_ENFORCE"):
        return True
    if "BLOCK" in t:
        return True
    if "COVERAGE" in t:
        return True
    return False


def _extract_outcome_ctx(ev: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pull best-effort join keys + metadata from an outcome_enriched record.
    """
    account_label = _safe_str(ev.get("account_label"))
    symbol = _safe_str(ev.get("symbol")).upper()
    timeframe = _safe_str(ev.get("timeframe"))
    policy_hash = None

    pol = ev.get("policy")
    if isinstance(pol, dict):
        ph = _safe_str(pol.get("policy_hash"))
        if ph:
            policy_hash = ph

    # outcome_enriched often nests a setup_context or setup
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

    return {
        "account_label": account_label,
        "symbol": symbol,
        "timeframe": timeframe,
        "policy_hash": policy_hash,
    }


def _load_outcome_enriched_ctx_by_tid() -> Dict[str, Dict[str, Any]]:
    """
    Map trade_id -> {account_label, symbol, timeframe, policy_hash}
    Prefer last occurrence (should be consistent).
    """
    m: Dict[str, Dict[str, Any]] = {}
    for ev in iter_jsonl(PATHS.outcomes_path):
        ok, _ = validate_outcome_enriched(ev)
        if not ok:
            continue
        tid = _safe_str(ev.get("trade_id"))
        if not tid:
            continue
        m[tid] = _extract_outcome_ctx(ev)
    return m


def _load_latest_decisions_map() -> Dict[str, Dict[str, Any]]:
    """
    last-write-wins map keyed by trade_id
    """
    m: Dict[str, Dict[str, Any]] = {}
    for ev in iter_jsonl(PATHS.decisions_path):
        ok, _ = validate_decision_record(ev)
        if not ok:
            continue
        tid = _safe_str(ev.get("trade_id"))
        if tid:
            m[tid] = ev
    return m


def _make_override_decision(*, trade_id: str, allow: bool, ctx: Dict[str, Any]) -> Dict[str, Any]:
    now = _now_ms()
    account_label = _safe_str(ctx.get("account_label"))
    symbol = _safe_str(ctx.get("symbol")).upper()
    timeframe = _safe_str(ctx.get("timeframe"))
    policy_hash = ctx.get("policy_hash")

    d: Dict[str, Any] = {
        "schema_version": 1,
        "ts": now,
        "ts_ms": now,
        "trade_id": trade_id,
        "client_trade_id": trade_id,
        "account_label": account_label,
        "symbol": symbol,
        "timeframe": timeframe,
        "policy_hash": policy_hash,
        "allow": bool(allow),
        "size_multiplier": 1.0,
        "decision": "ALLOW_BACKFILL" if allow else "BLOCK_BACKFILL",
        "tier_used": "BACKFILL",
        "reason": "phase5_patch_backfill",
        "mode": "patch_backfill",
        "gates": {"reason": "patched_for_memory_consistency"},
    }
    return d


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Print what would be appended, do not write.")
    args = ap.parse_args()

    if not PATHS.outcomes_path.exists():
        print(f"FAIL ❌ outcomes missing: {PATHS.outcomes_path}")
        return
    if not PATHS.decisions_path.exists():
        print(f"FAIL ❌ decisions missing: {PATHS.decisions_path}")
        return

    ctx_by_tid = _load_outcome_enriched_ctx_by_tid()
    latest_decisions = _load_latest_decisions_map()

    to_patch: List[Tuple[str, bool, str]] = []
    for tid, ctx in ctx_by_tid.items():
        is_block_test = _tid_is_block_test(tid)
        desired_allow = (not is_block_test)

        cur = latest_decisions.get(tid)
        cur_allow = None
        if isinstance(cur, dict) and isinstance(cur.get("allow"), bool):
            cur_allow = bool(cur.get("allow"))

        if cur is None:
            to_patch.append((tid, desired_allow, "missing_decision"))
        else:
            if cur_allow is None:
                to_patch.append((tid, desired_allow, "bad_decision_allow"))
            elif cur_allow != desired_allow:
                to_patch.append((tid, desired_allow, f"allow_mismatch current={cur_allow} desired={desired_allow}"))

    print("=== Patch Backfilled Decisions v2 (Phase 5.4) ===")
    print(f"outcome_enriched_trade_ids: {len(ctx_by_tid)}")
    print(f"latest_decisions_trade_ids: {len(latest_decisions)}")
    print(f"patch_needed             : {len(to_patch)}")
    if to_patch:
        print("sample_patch:")
        for tid, allow, why in to_patch[:10]:
            print(f" - {tid} -> allow={int(allow)} ({why})")

    if args.dry_run:
        print("\nDRY RUN. No writes.")
        return

    written = 0
    for tid, allow, _why in to_patch:
        ctx = ctx_by_tid.get(tid) or {}
        append_decision(_make_override_decision(trade_id=tid, allow=allow, ctx=ctx))
        written += 1

    print(f"\nwritten_overrides: {written}")
    print("DONE")


if __name__ == "__main__":
    main()
