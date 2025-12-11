#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Memory Sanity Checker

Purpose
-------
Give you a blunt, numeric verdict on whether Phase 4's data pipeline
is actually sane:

    - state/setup_outcomes.jsonl
    - state/setup_memory.jsonl

Checks:
    - total rows in each file
    - unique trade_ids in each
    - duplicates per file
    - orphan outcomes (in outcomes, missing in memory)
    - orphan memory rows (in memory, missing in outcomes)
    - label distribution (WIN/LOSS/BREAKEVEN/UNKNOWN)
    - basic flags on label_win / label_good / label_rr_ge_1

Usage:
    python -m app.ai.ai_memory_sanity
"""

from __future__ import annotations

from collections import Counter, defaultdict  # noqa: F401
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import orjson

try:
    from app.core.config import settings
except ImportError:
    from core.config import settings  # type: ignore

ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])
STATE_DIR: Path = ROOT / "state"

OUTCOMES_PATH: Path = STATE_DIR / "setup_outcomes.jsonl"
MEMORY_PATH: Path = STATE_DIR / "setup_memory.jsonl"


# ----------------- IO utils -----------------


def _load_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        print(f"[ai_memory_sanity] WARNING: {path} does not exist.")
        return []
    with path.open("rb") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = orjson.loads(line)
            except Exception:
                continue
            if isinstance(row, dict):
                yield row


# ----------------- core analysis -----------------


def _collect_by_trade_id(
    rows: Iterable[Dict[str, Any]],
    src_name: str,
) -> Tuple[Dict[str, Dict[str, Any]], Counter]:
    """
    Build a mapping: trade_id -> canonical row (last one wins),
    and a Counter for duplicate detection.
    """
    by_id: Dict[str, Dict[str, Any]] = {}
    counts: Counter = Counter()

    for r in rows:
        tid = str(r.get("trade_id") or "").strip()
        if not tid:
            # no trade_id: log as anonymous
            counts["__no_trade_id__"] += 1
            continue
        counts[tid] += 1
        by_id[tid] = r

    if counts["__no_trade_id__"] > 0:
        print(
            f"[ai_memory_sanity] {src_name}: {counts['__no_trade_id__']} "
            f"rows had no trade_id."
        )

    return by_id, counts


def _result_from_memory_row(r: Dict[str, Any]) -> str:
    res = str(r.get("result") or "").upper().strip()
    if res not in ("WIN", "LOSS", "BREAKEVEN", "UNKNOWN"):
        return "UNKNOWN"
    return res


def _bool_flag(v: Any) -> bool:
    return bool(v is True or v == 1)


def _analyze_memory_labels(memory_rows: Dict[str, Dict[str, Any]]) -> None:
    by_result: Counter = Counter()
    win_flag: Counter = Counter()
    good_flag: Counter = Counter()
    rr_ge_1_flag: Counter = Counter()

    for _tid, r in memory_rows.items():
        res = _result_from_memory_row(r)
        by_result[res] += 1

        win_flag[_bool_flag(r.get("label_win"))] += 1
        good_flag[_bool_flag(r.get("label_good"))] += 1
        rr_ge_1_flag[_bool_flag(r.get("label_rr_ge_1"))] += 1

    total = sum(by_result.values()) or 1

    print("\n[ai_memory_sanity] Label distribution in setup_memory:")
    for k in ("WIN", "LOSS", "BREAKEVEN", "UNKNOWN"):
        c = by_result.get(k, 0)
        pct = (c / total) * 100
        print(f"  - {k:10s}: {c:6d} ({pct:5.1f}%)")

    print("\n[ai_memory_sanity] Flags:")
    total_flags = sum(by_result.values()) or 1

    def _pct(cnt: int) -> float:
        return (cnt / total_flags) * 100

    print(
        f"  - label_win=True:       {win_flag[True]:6d} ({_pct(win_flag[True]):5.1f}%)"
    )
    print(
        f"  - label_good=True:      {good_flag[True]:6d} ({_pct(good_flag[True]):5.1f}%)"
    )
    print(
        f"  - label_rr_ge_1=True:   {rr_ge_1_flag[True]:6d} ({_pct(rr_ge_1_flag[True]):5.1f}%)"
    )


def _print_duplicates(counts: Counter, src_name: str) -> None:
    dupes = [tid for tid, c in counts.items() if tid not in ("__no_trade_id__") and c > 1]
    if not dupes:
        print(f"[ai_memory_sanity] {src_name}: no duplicate trade_ids detected.")
        return
    print(
        f"[ai_memory_sanity] {src_name}: {len(dupes)} duplicate trade_ids "
        f"({sum(counts[tid] for tid in dupes)} rows total)."
    )


def run_sanity_check() -> None:
    print("[ai_memory_sanity] Root:", ROOT)
    print("[ai_memory_sanity] Outcomes file:", OUTCOMES_PATH)
    print("[ai_memory_sanity] Memory file:", MEMORY_PATH)

    # Load both datasets
    outcomes_iter = list(_load_jsonl(OUTCOMES_PATH))
    memory_iter = list(_load_jsonl(MEMORY_PATH))

    print("\n[ai_memory_sanity] Raw row counts:")
    print(f"  - setup_outcomes: {len(outcomes_iter)}")
    print(f"  - setup_memory:   {len(memory_iter)}")

    outcomes_by_id, outcomes_counts = _collect_by_trade_id(outcomes_iter, "setup_outcomes")
    memory_by_id, memory_counts = _collect_by_trade_id(memory_iter, "setup_memory")

    print("\n[ai_memory_sanity] Unique trade_ids:")
    print(f"  - setup_outcomes: {len(outcomes_by_id)}")
    print(f"  - setup_memory:   {len(memory_by_id)}")

    _print_duplicates(outcomes_counts, "setup_outcomes")
    _print_duplicates(memory_counts, "setup_memory")

    # Orphans
    set_out = set(outcomes_by_id.keys())
    set_mem = set(memory_by_id.keys())

    only_in_outcomes = sorted(set_out - set_mem)
    only_in_memory = sorted(set_mem - set_out)

    print("\n[ai_memory_sanity] Trade_id overlap:")
    print(f"  - in BOTH:             {len(set_out & set_mem)}")
    print(f"  - only in outcomes:    {len(only_in_outcomes)}")
    print(f"  - only in memory:      {len(only_in_memory)}")

    if only_in_outcomes:
        sample = only_in_outcomes[:10]
        print("    * sample only-in-outcomes trade_ids:", ", ".join(sample))
    if only_in_memory:
        sample = only_in_memory[:10]
        print("    * sample only-in-memory trade_ids:", ", ".join(sample))

    # Label analysis on memory
    _analyze_memory_labels(memory_by_id)

    print("\n[ai_memory_sanity] Sanity check complete.\n")


if __name__ == "__main__":
    run_sanity_check()
