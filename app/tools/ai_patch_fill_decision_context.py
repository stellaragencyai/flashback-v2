#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Patch ai_decisions.jsonl to fill missing account_label + symbol (deterministic)

Goal
----
Fill historical decision rows that have trade_id but missing account_label/symbol.

Determinism sources (in order):
1) Other decision rows with same trade_id that DO have account_label/symbol.
2) outcomes.jsonl (canonical): trade_id -> account_label/symbol from:
   - top-level fields
   - nested setup / setup_context
   - outcome_enriched payloads

Output
------
Writes:
- state/ai_decisions.patched.jsonl
Leaves original untouched.

Usage
-----
python app/tools/ai_patch_fill_decision_context.py
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import orjson

ROOT = Path(__file__).resolve().parents[2]
STATE = ROOT / "state"
AI_EVENTS = STATE / "ai_events"

DECISIONS_IN = STATE / "ai_decisions.jsonl"
OUTCOMES = AI_EVENTS / "outcomes.jsonl"

DECISIONS_OUT = STATE / "ai_decisions.patched.jsonl"


def _safe_str(x: Any) -> str:
    try:
        return ("" if x is None else str(x)).strip()
    except Exception:
        return ""


def _upper(x: str) -> str:
    return x.strip().upper()


def _extract_trade_id(evt: Dict[str, Any]) -> str:
    tid = evt.get("trade_id")
    if isinstance(tid, str) and tid.strip():
        return tid.strip()

    setup = evt.get("setup")
    if isinstance(setup, dict):
        tid2 = setup.get("trade_id")
        if isinstance(tid2, str) and tid2.strip():
            return tid2.strip()

    ctx = evt.get("setup_context")
    if isinstance(ctx, dict):
        tid3 = ctx.get("trade_id")
        if isinstance(tid3, str) and tid3.strip():
            return tid3.strip()

    return ""


def _extract_symbol(evt: Dict[str, Any]) -> str:
    sym = evt.get("symbol")
    if isinstance(sym, str) and sym.strip():
        return sym.strip().upper()

    setup = evt.get("setup")
    if isinstance(setup, dict):
        sym2 = setup.get("symbol")
        if isinstance(sym2, str) and sym2.strip():
            return sym2.strip().upper()

    ctx = evt.get("setup_context")
    if isinstance(ctx, dict):
        sym3 = ctx.get("symbol")
        if isinstance(sym3, str) and sym3.strip():
            return sym3.strip().upper()

    # outcome_enriched sometimes nests setup in raw outcome
    raw = evt.get("raw")
    if isinstance(raw, dict):
        return _extract_symbol(raw)

    return ""


def _extract_account_label(evt: Dict[str, Any]) -> str:
    v = evt.get("account_label")
    if isinstance(v, str) and v.strip():
        return v.strip()

    setup = evt.get("setup")
    if isinstance(setup, dict):
        v2 = setup.get("account_label")
        if isinstance(v2, str) and v2.strip():
            return v2.strip()

    ctx = evt.get("setup_context")
    if isinstance(ctx, dict):
        v3 = ctx.get("account_label")
        if isinstance(v3, str) and v3.strip():
            return v3.strip()

    raw = evt.get("raw")
    if isinstance(raw, dict):
        return _extract_account_label(raw)

    return ""


def _build_tid_map_from_outcomes(path: Path) -> Dict[str, Tuple[str, str]]:
    """
    Build trade_id -> (account_label, symbol) from outcomes file.
    Only stores entries with BOTH fields non-empty.
    """
    m: Dict[str, Tuple[str, str]] = {}
    if not path.exists():
        return m

    with path.open("rb") as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue
            try:
                evt = orjson.loads(raw)
            except Exception:
                continue
            if not isinstance(evt, dict):
                continue

            tid = _extract_trade_id(evt)
            if not tid:
                continue

            acct = _extract_account_label(evt)
            sym = _extract_symbol(evt)
            if acct and sym:
                # keep first found; outcomes are canonical enough
                if tid not in m:
                    m[tid] = (acct, sym)
    return m


def _build_tid_map_from_decisions(lines: list[Dict[str, Any]]) -> Dict[str, Tuple[str, str]]:
    """
    Build trade_id -> (account_label, symbol) from decisions themselves,
    using rows that already have both fields.
    """
    m: Dict[str, Tuple[str, str]] = {}
    for d in lines:
        tid = _safe_str(d.get("trade_id"))
        if not tid:
            continue
        acct = _safe_str(d.get("account_label"))
        sym = _upper(_safe_str(d.get("symbol")))
        if acct and sym and tid not in m:
            m[tid] = (acct, sym)
    return m


def main() -> int:
    if not DECISIONS_IN.exists():
        print(f"missing decisions file: {DECISIONS_IN}")
        return 1

    # load decision rows (line-by-line)
    decisions: list[Dict[str, Any]] = []
    with DECISIONS_IN.open("rb") as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue
            try:
                d = orjson.loads(raw)
            except Exception:
                continue
            if isinstance(d, dict) and "trade_id" in d:
                decisions.append(d)

    tid_from_decisions = _build_tid_map_from_decisions(decisions)
    tid_from_outcomes = _build_tid_map_from_outcomes(OUTCOMES)

    patched = 0
    still_missing = 0

    with DECISIONS_OUT.open("wb") as out:
        for d in decisions:
            tid = _safe_str(d.get("trade_id"))
            acct = _safe_str(d.get("account_label"))
            sym = _upper(_safe_str(d.get("symbol")))

            if (not acct or not sym) and tid:
                fill: Optional[Tuple[str, str]] = None
                fill = tid_from_decisions.get(tid) or tid_from_outcomes.get(tid)
                if fill:
                    acct_f, sym_f = fill
                    if not acct:
                        d["account_label"] = acct_f
                    if not sym:
                        d["symbol"] = sym_f
                    patched += 1
                else:
                    still_missing += 1

            out.write(orjson.dumps(d))
            out.write(b"\n")

    print("ok=1")
    print(f"total_in={len(decisions)}")
    print(f"patched={patched}")
    print(f"still_missing={still_missing}")
    print(f"out={DECISIONS_OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
