#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — MemoryEntry Build Tool (Phase 5) v1.0

Usage
-----
python app/tools/ai_memory_entry_build.py

What it does
------------
- Reads canonical streams (setups / outcomes_enriched / decisions)
- Builds:
    state/ai_memory/memory_entries.jsonl
    state/ai_memory/memory_index.sqlite
- Prints a blunt summary and exits 0 even on partial skips.

This is a Phase 5 foundations tool only.
"""

from __future__ import annotations

from pathlib import Path

from app.ai.ai_memory_contract import ContractPaths
from app.ai.ai_memory_entry_builder import build_memory_entries


def main() -> None:
    paths = ContractPaths.default()

    out_jsonl = Path("state/ai_memory/memory_entries.jsonl")
    db_path = Path("state/ai_memory/memory_index.sqlite")

    print("=== MemoryEntry Builder v1 ===")
    print(f"setups   : {paths.setups_path}")
    print(f"outcomes : {paths.outcomes_path}")
    print(f"decisions: {paths.decisions_path}")
    print(f"out_jsonl: {out_jsonl.resolve()}")
    print(f"db_path  : {db_path.resolve()}")
    print()

    stats = build_memory_entries(
        paths=paths,
        out_jsonl=out_jsonl,
        db_path=db_path,
    )

    print("[STATS]")
    for k in [
        "processed_outcome_rows",
        "inserted",
        "skipped_existing",
        "bad_rows",
        "setup_index_ok",
        "setup_index_bad",
        "decision_index_ok",
        "decision_index_bad",
        "elapsed_sec",
    ]:
        print(f"{k:24s}: {stats.get(k)}")

    print("\nPASS ✅ MemoryEntry artifacts built (or already up-to-date).")


if __name__ == "__main__":
    main()
