#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Memory Contract Check (Phase 5)

Purpose
-------
Single-command PASS/FAIL check that:
- Canonical streams exist
- Their shapes are contract-compliant enough for Phase 5
- MemoryEntry build will not be garbage

Usage
-----
python app/tools/ai_memory_contract_check.py
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from app.ai.ai_memory_contract import (
    ContractPaths,
    iter_jsonl,
    validate_setup_record,
    validate_outcome_enriched,
    validate_decision_record,
)

PATHS = ContractPaths.default()


def _exists(p: Path) -> bool:
    return p.exists() and p.stat().st_size > 0


def _peek_keys(path: Path, n: int = 2) -> None:
    print(f"\n[PEEK] {path}")
    c = 0
    for ev in iter_jsonl(path, max_lines=5000):
        print("  - keys=" + ",".join(sorted(ev.keys())))
        c += 1
        if c >= n:
            return
    if c == 0:
        print("  - (no readable rows)")


def main() -> None:
    print("=== AI Memory Contract Check v1 ===")
    print(f"setups   : {PATHS.setups_path}")
    print(f"outcomes : {PATHS.outcomes_path}")
    print(f"decisions: {PATHS.decisions_path}")

    missing = []
    for p in (PATHS.setups_path, PATHS.outcomes_path, PATHS.decisions_path):
        if not _exists(p):
            missing.append(str(p))
    if missing:
        print("\nFAIL ❌ Missing required canonical files:")
        for m in missing:
            print("  -", m)
        return

    # Validate samples
    sample_n = 200
    setup_ok = 0
    setup_bad = 0
    for ev in iter_jsonl(PATHS.setups_path, max_lines=sample_n):
        ok, _ = validate_setup_record(ev)
        setup_ok += 1 if ok else 0
        setup_bad += 0 if ok else 1

    out_ok = 0
    out_bad = 0
    out_not_enriched = 0
    for ev in iter_jsonl(PATHS.outcomes_path, max_lines=sample_n):
        ok, reason = validate_outcome_enriched(ev)
        if ok:
            out_ok += 1
        else:
            if reason == "not_outcome_enriched":
                out_not_enriched += 1
            out_bad += 1

    dec_ok = 0
    dec_bad = 0
    for ev in iter_jsonl(PATHS.decisions_path, max_lines=sample_n):
        ok, _ = validate_decision_record(ev)
        dec_ok += 1 if ok else 0
        dec_bad += 0 if ok else 1

    print("\n[STATS]")
    print(f"setup_sample_ok     : {setup_ok}")
    print(f"setup_sample_bad    : {setup_bad}")
    print(f"outcome_enriched_ok : {out_ok}")
    print(f"outcome_bad_total   : {out_bad}")
    print(f"outcome_not_enriched: {out_not_enriched}")
    print(f"decision_sample_ok  : {dec_ok}")
    print(f"decision_sample_bad : {dec_bad}")

    # Keys peek for sanity (helps catch contamination quickly)
    _peek_keys(PATHS.setups_path, n=2)
    _peek_keys(PATHS.outcomes_path, n=2)

    # Verdict rules (Phase 5 foundations):
    # - Setups sample should mostly validate
    # - Outcomes should be outcome_enriched for learning substrate
    # - Decisions sample should mostly validate
    if setup_ok < 10:
        print("\nFAIL ❌ Not enough valid setup_context rows found.")
        return
    if out_ok < 10:
        print("\nFAIL ❌ Not enough valid outcome_enriched rows found (Phase 5 needs enriched outcomes).")
        return
    if dec_ok < 10:
        print("\nFAIL ❌ Not enough valid decision rows found.")
        return

    print("\nPASS ✅ Contract is usable for Phase 5 foundations.")
    print("Next: run MemoryEntry builder to create memory_entries.jsonl + memory_index.sqlite")


if __name__ == "__main__":
    main()
