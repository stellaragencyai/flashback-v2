#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Memory Sanity Checker (canonical, Phase 4)

Checks:
  - state/setup_outcomes.jsonl     (output of outcome_joiner)
  - state/setup_memory.jsonl       (output of setup_memory_builder)
  - OPTIONAL upstream events (if present):
      • state/ai_events/setups.jsonl
      • state/ai_events/outcomes.jsonl

Validates:
  - Counts: outcomes vs memory rows
  - Distribution: result + outcome_bucket
  - JSONL corruption (bad line count)
  - Join-key coverage (best-effort orphan detection)
  - Exits non-zero if mismatch ratio or corruption is too high
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import orjson

from app.core.config import settings

ROOT: Path = settings.ROOT
STATE_DIR: Path = ROOT / "state"

# Current canonical outputs
OUTCOMES_PATH: Path = STATE_DIR / "setup_outcomes.jsonl"
MEMORY_PATH: Path = STATE_DIR / "setup_memory.jsonl"

# Optional upstream event logs (if you use ai_events_spine disk logging)
AI_EVENTS_DIR: Path = STATE_DIR / "ai_events"
SETUPS_EVENTS_PATH: Path = AI_EVENTS_DIR / "setups.jsonl"
OUTCOMES_EVENTS_PATH: Path = AI_EVENTS_DIR / "outcomes.jsonl"


@dataclass
class JsonlLoadResult:
    rows: List[Dict[str, Any]]
    bad_lines: int
    total_lines: int


def _load_jsonl(path: Path) -> JsonlLoadResult:
    if not path.exists():
        print(f"[ai_memory_sanity] WARNING: {path} missing.")
        return JsonlLoadResult(rows=[], bad_lines=0, total_lines=0)

    rows: List[Dict[str, Any]] = []
    bad = 0
    total = 0

    with path.open("rb") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            total += 1
            try:
                obj = orjson.loads(line)
            except Exception:
                bad += 1
                continue
            if isinstance(obj, dict):
                rows.append(obj)

    return JsonlLoadResult(rows=rows, bad_lines=bad, total_lines=total)


def _extract_keys(rows: List[Dict[str, Any]], key_fields: Tuple[str, ...]) -> Set[str]:
    keys: Set[str] = set()
    for r in rows:
        for k in key_fields:
            v = r.get(k)
            if isinstance(v, str) and v.strip():
                keys.add(v.strip())
                break
    return keys


def main() -> None:
    # Load files
    out_lr = _load_jsonl(OUTCOMES_PATH)
    mem_lr = _load_jsonl(MEMORY_PATH)

    setups_ev = _load_jsonl(SETUPS_EVENTS_PATH) if SETUPS_EVENTS_PATH.exists() else JsonlLoadResult([], 0, 0)
    outcomes_ev = _load_jsonl(OUTCOMES_EVENTS_PATH) if OUTCOMES_EVENTS_PATH.exists() else JsonlLoadResult([], 0, 0)

    outcomes = out_lr.rows
    memory = mem_lr.rows

    n_out = len(outcomes)
    n_mem = len(memory)

    print("\n📚 [ai_memory_sanity] Phase 4 — Coverage + Integrity\n")
    print(f"[ai_memory_sanity] outcomes:      {n_out} (bad_lines={out_lr.bad_lines}, total_lines={out_lr.total_lines})")
    print(f"[ai_memory_sanity] setup_memory:  {n_mem} (bad_lines={mem_lr.bad_lines}, total_lines={mem_lr.total_lines})")

    if setups_ev.total_lines > 0 or outcomes_ev.total_lines > 0:
        print(f"[ai_memory_sanity] ai_events/setups:   {len(setups_ev.rows)} (bad_lines={setups_ev.bad_lines}, total_lines={setups_ev.total_lines})")
        print(f"[ai_memory_sanity] ai_events/outcomes: {len(outcomes_ev.rows)} (bad_lines={outcomes_ev.bad_lines}, total_lines={outcomes_ev.total_lines})")

    # Fresh system: nothing to check
    if n_out == 0 and n_mem == 0 and out_lr.total_lines == 0 and mem_lr.total_lines == 0:
        print("\n[ai_memory_sanity] ✅ Nothing to check yet (fresh system, no data).")
        raise SystemExit(0)

    # Corruption guardrails (don’t silently accept garbage)
    def bad_ratio(lr: JsonlLoadResult) -> float:
        return (lr.bad_lines / lr.total_lines) if lr.total_lines > 0 else 0.0

    max_bad_ratio = 0.02  # 2% default tolerance

    corrupt_reasons = []
    if bad_ratio(out_lr) > max_bad_ratio:
        corrupt_reasons.append(f"setup_outcomes.jsonl bad_ratio={bad_ratio(out_lr):.2%} > {max_bad_ratio:.2%}")
    if bad_ratio(mem_lr) > max_bad_ratio:
        corrupt_reasons.append(f"setup_memory.jsonl bad_ratio={bad_ratio(mem_lr):.2%} > {max_bad_ratio:.2%}")
    if outcomes_ev.total_lines > 0 and bad_ratio(outcomes_ev) > max_bad_ratio:
        corrupt_reasons.append(f"ai_events/outcomes.jsonl bad_ratio={bad_ratio(outcomes_ev):.2%} > {max_bad_ratio:.2%}")
    if setups_ev.total_lines > 0 and bad_ratio(setups_ev) > max_bad_ratio:
        corrupt_reasons.append(f"ai_events/setups.jsonl bad_ratio={bad_ratio(setups_ev):.2%} > {max_bad_ratio:.2%}")

    # Mismatch ratio (your original logic, kept)
    mismatch_ratio = 0.0
    if n_out > 0:
        mismatch_ratio = abs(n_out - n_mem) / float(max(n_out, 1))

    print(f"\n[ai_memory_sanity] size mismatch ratio (outcomes vs memory): {mismatch_ratio:.3f}")

    # Buckets / results in memory (kept from your version)
    bucket_counter = Counter()
    result_counter = Counter()

    for row in memory:
        bucket = str(row.get("outcome_bucket") or "UNKNOWN").upper()
        result = str(row.get("result") or "UNKNOWN").upper()
        bucket_counter[bucket] += 1
        result_counter[result] += 1

    print("\n[ai_memory_sanity] outcome_bucket distribution:")
    total_buckets = sum(bucket_counter.values()) or 1
    for bucket, cnt in bucket_counter.most_common():
        pct = 100.0 * cnt / total_buckets
        print(f"  {bucket:12s}: {cnt:6d}  ({pct:5.1f}%)")

    print("\n[ai_memory_sanity] result distribution:")
    total_results = sum(result_counter.values()) or 1
    for res, cnt in result_counter.most_common():
        pct = 100.0 * cnt / total_results
        print(f"  {res:12s}: {cnt:6d}  ({pct:5.1f}%)")

    # Join-key coverage (best-effort)
    # These keys exist in different parts of the pipeline; we try several.
    join_fields = ("setup_id", "trade_id", "orderLinkId", "order_link_id", "client_id")

    out_keys = _extract_keys(outcomes, join_fields)
    mem_keys = _extract_keys(memory, join_fields)

    if out_keys or mem_keys:
        print("\n[ai_memory_sanity] join-key coverage (best-effort):")
        print(f"  unique outcome keys: {len(out_keys)}")
        print(f"  unique memory keys:  {len(mem_keys)}")
        if out_keys and mem_keys:
            print(f"  memory missing outcomes: {len(out_keys - mem_keys)}")
            print(f"  outcomes missing memory: {len(mem_keys - out_keys)}")

    # Decide exit codes
    # FAIL if corruption high or mismatch ridiculous
    if corrupt_reasons:
        print("\n[ai_memory_sanity] ❌ Corruption detected:")
        for r in corrupt_reasons:
            print(f"  - {r}")
        raise SystemExit(2)

    if n_out > 0 and mismatch_ratio > 0.25:
        print("\n[ai_memory_sanity] ❌ Mismatch ratio > 0.25 — investigate outcome_joiner / setup_memory_builder.")
        raise SystemExit(1)

    print("\n[ai_memory_sanity] ✅ Sanity check passed (within tolerated limits).")
    raise SystemExit(0)


if __name__ == "__main__":
    main()
