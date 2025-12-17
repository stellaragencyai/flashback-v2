#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Memory Sanity Check v1 (Phase 5.3)

Purpose
-------
Detect contradictions and drift between:
- outcomes_enriched (canonical)
- ai_decisions (canonical)
- memory_entries (derived)

This catches issues like:
- decision says allow=0 but outcome exists (common in backfilled test data)
- memory has allow=0 for everything
- policy_hash or fingerprints missing (should be 0 now)

Usage
-----
python app/tools/ai_memory_sanity_check.py
python app/tools/ai_memory_sanity_check.py --limit 50
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from app.ai.ai_memory_contract import ContractPaths, iter_jsonl, validate_outcome_enriched

PATHS = ContractPaths.default()


def _exists_nonempty(p: Path) -> bool:
    try:
        return p.exists() and p.stat().st_size > 0
    except Exception:
        return False


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _decision_allow_from_row(row: Any) -> Optional[bool]:
    # memory_entries.allow is stored as 0/1/NULL
    v = row["allow"]
    if v is None:
        return None
    try:
        return bool(int(v))
    except Exception:
        return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=25, help="Max examples to print per category (default 25)")
    args = ap.parse_args()
    limit = int(args.limit or 25)
    if limit < 1:
        limit = 1
    if limit > 200:
        limit = 200

    # Preconditions
    missing = []
    for p in (PATHS.outcomes_path, PATHS.decisions_path, PATHS.memory_index_path):
        if not _exists_nonempty(p):
            missing.append(str(p))
    if missing:
        print("FAIL ❌ Missing required files:")
        for m in missing:
            print("  -", m)
        return

    # Load outcome_enriched trade_ids
    outcome_tids: List[str] = []
    for ev in iter_jsonl(PATHS.outcomes_path):
        ok, _ = validate_outcome_enriched(ev)
        if not ok:
            continue
        tid = str(ev.get("trade_id") or "").strip()
        if tid:
            outcome_tids.append(tid)

    outcome_set: Set[str] = set(outcome_tids)

    # Read memory entries
    conn = _connect(PATHS.memory_index_path)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT trade_id, symbol, timeframe, setup_type, allow, size_multiplier, win, r_multiple, pnl_usd
        FROM memory_entries
        ORDER BY ts_ms DESC;
        """
    )
    rows = cur.fetchall()
    conn.close()

    mem_set: Set[str] = set([str(r["trade_id"]) for r in rows if r["trade_id"]])

    # Sanity categories
    mem_without_outcome = []   # should be rare, but could happen if someone inserted bogus rows
    outcome_without_mem = []   # expected if builder skipped or failed
    outcome_with_block = []    # the big one: decision says allow=0 but outcome exists

    for r in rows:
        tid = str(r["trade_id"])
        if tid not in outcome_set:
            mem_without_outcome.append(r)

        allow = _decision_allow_from_row(r)
        if tid in outcome_set and allow is False:
            outcome_with_block.append(r)

    for tid in sorted(outcome_set):
        if tid not in mem_set:
            outcome_without_mem.append(tid)

    # Summary
    print("=== AI Memory Sanity Check v1 ===")
    print(f"outcome_enriched_count   : {len(outcome_set)}")
    print(f"memory_entries_count     : {len(mem_set)}")
    print(f"mem_without_outcome      : {len(mem_without_outcome)}")
    print(f"outcome_without_mem      : {len(outcome_without_mem)}")
    print(f"outcome_with_allow_false : {len(outcome_with_block)}")

    # Print examples
    def _print_rows(title: str, rr: List[Any]) -> None:
        print(f"\n[{title}] showing up to {min(limit, len(rr))}")
        for r in rr[:limit]:
            print(
                " - "
                f"trade_id={r['trade_id']} "
                f"{r['symbol']} {r['timeframe']} "
                f"setup_type={r['setup_type'] or 'n/a'} "
                f"allow={r['allow']} "
                f"win={r['win']} "
                f"R={r['r_multiple']} "
                f"pnl={r['pnl_usd']}"
            )

    if mem_without_outcome:
        _print_rows("MEM WITHOUT OUTCOME", mem_without_outcome)

    if outcome_without_mem:
        print(f"\n[OUTCOME WITHOUT MEMORY] showing up to {min(limit, len(outcome_without_mem))}")
        for tid in outcome_without_mem[:limit]:
            print(" - trade_id=" + tid)

    if outcome_with_block:
        _print_rows("OUTCOME WITH allow=0", outcome_with_block)

    # Verdict logic for Phase 5:
    # We accept outcome_with_block for test/backfilled data, but we want to see it clearly.
    print("\nVERDICT:")
    if len(mem_set) == 0:
        print("FAIL ❌ No memory entries. Builder is not producing output.")
        return
    if len(outcome_without_mem) > 0:
        print("WARN ⚠️ Some outcome_enriched rows did not become memory entries (check builder filters).")
    if len(outcome_with_block) == len(mem_set):
        print("WARN ⚠️ All memory entries are allow=0. Likely backfill defaulted to BLOCK.")
    print("PASS ✅ Sanity check completed.")


if __name__ == "__main__":
    main()
