#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Learning Determinism Test v1 (Phase 6)

Runs Phase 6 learning build twice and asserts byte-identical JSONL output by sha256.
This is the lock gate for Phase 6.

Usage:
  python -m app.tools.ai_learning_determinism_test
"""

from __future__ import annotations

import time
from pathlib import Path

from app.ai.ai_learning_contract import LearningPaths
from app.ai.ai_learning_builder import build_and_write


def _sha(path: Path) -> str:
    import hashlib
    b = path.read_bytes()
    return hashlib.sha256(b).hexdigest()


def main() -> None:
    lp = LearningPaths.default()

    print("=== AI Learning Determinism Test v1 (Phase 6) ===")

    # Run 1
    s1 = build_and_write()
    if not s1.get("ok"):
        print("FAIL ❌ build #1:", s1.get("reason"))
        return
    h1 = s1.get("jsonl_sha256") or _sha(lp.memory_stats_jsonl_path)
    b1 = int(s1.get("jsonl_bytes") or lp.memory_stats_jsonl_path.stat().st_size)

    # Small pause to avoid identical timestamps accidentally masking issues
    time.sleep(0.25)

    # Run 2
    s2 = build_and_write()
    if not s2.get("ok"):
        print("FAIL ❌ build #2:", s2.get("reason"))
        return
    h2 = s2.get("jsonl_sha256") or _sha(lp.memory_stats_jsonl_path)
    b2 = int(s2.get("jsonl_bytes") or lp.memory_stats_jsonl_path.stat().st_size)

    print("run1_sha256 :", h1)
    print("run1_bytes  :", b1)
    print("run2_sha256 :", h2)
    print("run2_bytes  :", b2)

    if h1 != h2 or b1 != b2:
        print("FAIL ❌ NON-DETERMINISTIC OUTPUT")
        return

    print("PASS ✅ deterministic (byte-identical JSONL)")
    print("DONE ✅")


if __name__ == "__main__":
    main()
