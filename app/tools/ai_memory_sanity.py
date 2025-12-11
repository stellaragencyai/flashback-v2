#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Memory Sanity Checker

Checks:
  - state/setup_outcomes.jsonl  (output of outcome_joiner)
  - state/setup_memory.jsonl    (output of setup_memory_builder v2.1)

Validates:
  - Counts: outcomes vs memory rows
  - Distribution: result + outcome_bucket
  - Simple mismatch ratio, exits non-zero if too bad
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

import orjson

from app.core.config import settings

ROOT: Path = settings.ROOT
STATE_DIR: Path = ROOT / "state"
OUTCOMES_PATH: Path = STATE_DIR / "setup_outcomes.jsonl"
MEMORY_PATH: Path = STATE_DIR / "setup_memory.jsonl"


def _load_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        print(f"[ai_memory_sanity] WARNING: {path} missing.")
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


def main() -> None:
    outcomes = list(_load_jsonl(OUTCOMES_PATH))
    memory = list(_load_jsonl(MEMORY_PATH))

    n_out = len(outcomes)
    n_mem = len(memory)

    print(f"[ai_memory_sanity] outcomes: {n_out}  |  setup_memory: {n_mem}")

    if n_out == 0 and n_mem == 0:
        print("[ai_memory_sanity] Nothing to check. Both files empty / missing.")
        raise SystemExit(0)

    mismatch_ratio = 0.0
    if n_out > 0:
        mismatch_ratio = abs(n_out - n_mem) / float(max(n_out, 1))

    print(f"[ai_memory_sanity] size mismatch ratio: {mismatch_ratio:.3f}")

    # Buckets / results in memory
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

    # Exit code: fail if mismatch is ridiculous
    if n_out > 0 and mismatch_ratio > 0.25:
        print("\n[ai_memory_sanity] ❌ Mismatch ratio > 0.25 — investigate outcome_joiner / builder.")
        raise SystemExit(1)

    print("\n[ai_memory_sanity] ✅ Sanity check passed (within tolerated mismatch).")
    raise SystemExit(0)


if __name__ == "__main__":
    main()
