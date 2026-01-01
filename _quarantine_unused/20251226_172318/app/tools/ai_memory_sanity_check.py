#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Memory Sanity Check v1.1 (Phase 5.3)

Purpose
-------
Detect contradictions and drift between:
- outcomes_enriched (canonical)
- ai_decisions (canonical)
- memory_entries (derived, history-safe)

History-safe semantics
---------------------
- memory_entries can contain MULTIPLE rows per trade_id
- Sanity checks should reason primarily about the LATEST row per trade_id
  (ORDER BY ts_ms DESC, first row encountered per trade_id)

Usage
-----
python -m app.tools.ai_memory_sanity_check
python -m app.tools.ai_memory_sanity_check --limit 50
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Any, List, Optional, Set, Dict

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

    # Read memory entries (row-level)
    conn = _connect(PATHS.memory_index_path)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT trade_id, symbol, timeframe, setup_type, allow, size_multiplier, win, r_multiple, pnl_usd, ts_ms
        FROM memory_entries
        ORDER BY ts_ms DESC;
        """
    )
    rows = cur.fetchall()
    conn.close()

    memory_rows = len(rows)
    trade_ids_all = [str(r["trade_id"]) for r in rows if r["trade_id"]]
    mem_trade_id_set: Set[str] = set(trade_ids_all)

    # Build latest row per trade_id (history-safe semantics)
    latest_by_tid: Dict[str, Any] = {}
    for r in rows:
        tid = str(r["trade_id"] or "").strip()
        if not tid:
            continue
        if tid not in latest_by_tid:
            latest_by_tid[tid] = r  # rows are DESC ts_ms, so first seen is latest

    # Sanity categories (use latest row per trade_id)
    mem_without_outcome = []   # latest memory exists but no outcome_enriched
    outcome_without_mem = []   # outcome_enriched exists but no memory row
    outcome_with_block = []    # outcome exists but latest memory says allow=0

    for tid, r in latest_by_tid.items():
        if tid not in outcome_set:
            mem_without_outcome.append(r)

        allow = _decision_allow_from_row(r)
        if tid in outcome_set and allow is False:
            outcome_with_block.append(r)

    for tid in sorted(outcome_set):
        if tid not in mem_trade_id_set:
            outcome_without_mem.append(tid)

    # Summary
    print("=== AI Memory Sanity Check v1.1 ===")
    print(f"outcome_enriched_count      : {len(outcome_set)}")
    print(f"memory_entries_rows         : {memory_rows}")
    print(f"memory_entries_trade_ids    : {len(mem_trade_id_set)}")
    print(f"latest_trade_ids_evaluated  : {len(latest_by_tid)}")
    print(f"mem_without_outcome         : {len(mem_without_outcome)}")
    print(f"outcome_without_mem         : {len(outcome_without_mem)}")
    print(f"outcome_with_allow_false    : {len(outcome_with_block)}")

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
                f"pnl={r['pnl_usd']} "
                f"ts={r['ts_ms']}"
            )

    if mem_without_outcome:
        _print_rows("MEM (LATEST) WITHOUT OUTCOME", mem_without_outcome)

    if outcome_without_mem:
        print(f"\n[OUTCOME WITHOUT MEMORY] showing up to {min(limit, len(outcome_without_mem))}")
        for tid in outcome_without_mem[:limit]:
            print(" - trade_id=" + tid)

    if outcome_with_block:
        _print_rows("OUTCOME WITH allow=0 (LATEST)", outcome_with_block)

    # Verdict
    print("\nVERDICT:")
    if len(mem_trade_id_set) == 0 or memory_rows == 0:
        print("FAIL ❌ No memory entries. Builder is not producing output.")
        return
    if len(outcome_without_mem) > 0:
        print("WARN ⚠️ Some outcome_enriched rows did not become memory entries (check builder filters).")
    if len(outcome_with_block) == len(latest_by_tid) and len(latest_by_tid) > 0:
        print("WARN ⚠️ All latest memory entries are allow=0. Likely backfill defaulted to BLOCK.")
    print("PASS ✅ Sanity check completed.")


if __name__ == "__main__":
    main()
