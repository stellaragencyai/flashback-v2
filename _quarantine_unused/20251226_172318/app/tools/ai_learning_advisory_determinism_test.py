#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Learning Advisory Determinism Test v1 (Phase 6)

Runs advisory build twice and asserts byte-identical advisory_v1.jsonl.

No wall-clock. No randomness. If it drifts, Phase 6 is NOT lockable.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from app.ai.ai_learning_contract import LearningPaths
from app.tools.ai_learning_advisory_build import main as advisory_build_main


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    lp = LearningPaths.default()
    p = lp.advisory_jsonl_path

    # Run 1
    advisory_build_main()
    sha1 = _sha256(p) if p.exists() else ""
    b1 = p.stat().st_size if p.exists() else 0

    # Run 2
    advisory_build_main()
    sha2 = _sha256(p) if p.exists() else ""
    b2 = p.stat().st_size if p.exists() else 0

    print("=== AI Learning Advisory Determinism Test v1 (Phase 6) ===")
    print("run1_sha256 :", sha1)
    print("run1_bytes  :", b1)
    print("run2_sha256 :", sha2)
    print("run2_bytes  :", b2)

    if sha1 != sha2 or b1 != b2:
        print("FAIL ❌ NON-DETERMINISTIC OUTPUT")
        return

    print("PASS ✅ deterministic (byte-identical advisory JSONL)")
    print("DONE ✅")


if __name__ == "__main__":
    main()
