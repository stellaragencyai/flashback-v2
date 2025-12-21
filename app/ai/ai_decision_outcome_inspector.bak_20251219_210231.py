#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Flashback — AI Decision↔Outcome Inspector (Phase 4) v1.0

Purpose
-------
Read state/ai_decision_outcomes.jsonl and produce:
- counts by status
- coverage rate (how many outcomes have a matching decision)
- breakdown by decision code (ALLOW_TRADE vs BLOCKED...)
- quick tail samples with extracted fields (not raw blob spam)
- optional hard FAIL when coverage too low

Why
---
You don't "have Phase 4" until you can measure:
- decision coverage
- gate effectiveness
- outcome distribution by decision
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple

import orjson


ROOT = Path(__file__).resolve().parents[2]
JOINED_PATH = ROOT / "state" / "ai_decision_outcomes.jsonl"


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        return default


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _read_jsonl(path: Path, limit: int = 0) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not path.exists():
        return out

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
                out.append(obj)
                if limit > 0 and len(out) >= limit:
                    break
    return out


def _tail_jsonl(path: Path, n: int) -> List[Dict[str, Any]]:
    # Simple tail: read all if file is small, else last ~2MB window
    if not path.exists():
        return []
    size = path.stat().st_size
    window = min(size, 2 * 1024 * 1024)
    data = b""
    with path.open("rb") as f:
        if size > window:
            f.seek(size - window)
        data = f.read()

    lines = [ln for ln in data.splitlines() if ln.strip()]
    tail = lines[-max(1, n):]
    out: List[Dict[str, Any]] = []
    for ln in tail:
        try:
            obj = orjson.loads(ln)
        except Exception:
            continue
        if isinstance(obj, dict):
            out.append(obj)
    return out


def _extract_summary(row: Dict[str, Any]) -> Dict[str, Any]:
    dec = row.get("decision") if isinstance(row.get("decision"), dict) else {}
    outc = row.get("outcome") if isinstance(row.get("outcome"), dict) else {}

    return {
        "trade_id": row.get("trade_id"),
        "symbol": row.get("symbol"),
        "account_label": row.get("account_label"),
        "status": row.get("status"),
        "decision_code": dec.get("decision"),
        "tier_used": dec.get("tier_used"),
        "gates_reason": dec.get("gates_reason"),
        "memory_id": dec.get("memory_id"),
        "memory_score": dec.get("memory_score"),
        "pnl_usd": outc.get("pnl_usd"),
        "r_multiple": outc.get("r_multiple"),
        "win": outc.get("win"),
        "exit_reason": outc.get("exit_reason"),
        "final_status": outc.get("final_status"),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--min_coverage", type=float, default=0.50, help="Fail if decision coverage < this")
    ap.add_argument("--tail", type=int, default=5, help="Show last N summarized rows")
    args = ap.parse_args()

    if not JOINED_PATH.exists():
        print(f"FAIL: missing {JOINED_PATH}")
        return 2

    # Read full file (Phase 4 files are small now; later we can optimize)
    rows = _read_jsonl(JOINED_PATH, limit=0)
    total = len(rows)
    if total <= 0:
        print("FAIL: file exists but no valid JSONL rows parsed.")
        return 3

    status_ctr = Counter()
    decision_ctr = Counter()
    gates_ctr = Counter()
    by_decision_pnl: Dict[str, List[float]] = defaultdict(list)

    ok_with_decision = 0
    ok_total = 0

    for r in rows:
        status = str(r.get("status") or "UNKNOWN")
        status_ctr[status] += 1

        dec = r.get("decision")
        if status == "OK":
            ok_total += 1
        if isinstance(dec, dict) and dec:
            if status == "OK":
                ok_with_decision += 1

            code = str(dec.get("decision") or "UNKNOWN")
            decision_ctr[code] += 1
            gates_ctr[str(dec.get("gates_reason") or "none")] += 1

            outc = r.get("outcome") if isinstance(r.get("outcome"), dict) else {}
            pnl = _safe_float(outc.get("pnl_usd"), 0.0)
            by_decision_pnl[code].append(pnl)

    coverage = (ok_with_decision / ok_total) if ok_total > 0 else 0.0

    print("\n=== AI DECISION↔OUTCOME INSPECTOR ===")
    print(f"file: {JOINED_PATH}")
    print(f"rows_total: {total}")
    print(f"status_counts: {dict(status_ctr)}")
    print(f"ok_total: {ok_total}  ok_with_decision: {ok_with_decision}  coverage: {coverage:.3f}")
    print(f"decision_counts: {dict(decision_ctr)}")
    print(f"gates_reason_counts: {dict(gates_ctr)}")

    # PnL means per decision code
    print("\n--- PnL by decision (mean over pnl_usd) ---")
    for code, arr in sorted(by_decision_pnl.items(), key=lambda kv: kv[0]):
        if not arr:
            continue
        mean = sum(arr) / max(1, len(arr))
        print(f"{code:18s} n={len(arr):4d} mean_pnl_usd={mean:.4f}")

    # Tail summarized
    print(f"\n--- Tail (last {args.tail}) summarized ---")
    tail = _tail_jsonl(JOINED_PATH, args.tail)
    for row in tail:
        print(orjson.dumps(_extract_summary(row)).decode("utf-8"))

    # Hard fail if coverage too low
    if ok_total > 0 and coverage < float(args.min_coverage):
        print(f"\nFAIL: decision coverage {coverage:.3f} < min_coverage {args.min_coverage:.3f}")
        return 5

    print("\nPASS ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
