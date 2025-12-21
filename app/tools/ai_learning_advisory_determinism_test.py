#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Learning Advisory Determinism Test v1 (Phase 6)

Goal:
- Ensure advisory_v1.jsonl is byte-identical across repeated builds
- Fail hard if advisory output drifts

This test assumes:
- ai_learning_advisory_build --rebuild is deterministic
- built_ts_ms is derived from memory, NOT wall clock
"""

from __future__ import annotations

import hashlib
from pathlib import Path
import subprocess
import sys

from app.ai.ai_learning_contract import LearningPaths


def sha256_bytes(p: Path) -> tuple[str, int]:
    b = p.read_bytes()
    return (hashlib.sha256(b).hexdigest(), len(b))


def run_build() -> None:
    cmd = [
        sys.executable,
        "-m",
        "app.tools.ai_learning_advisory_build",
        "--rebuild",
    ]
    subprocess.check_call(cmd)


def main() -> None:
    paths = LearningPaths.default()
    advisory_path = paths.advisory_jsonl_path

    # Run twice
    run_build()
    h1, n1 = sha256_bytes(advisory_path)

    run_build()
    h2, n2 = sha256_bytes(advisory_path)

    print("=== AI Learning Advisory Determinism Test v1 (Phase 6) ===")
    print("run1_sha256 :", h1)
    print("run1_bytes  :", n1)
    print("run2_sha256 :", h2)
    print("run2_bytes  :", n2)

    if h1 != h2 or n1 != n2:
        print("FAIL ❌ NON-DETERMINISTIC ADVISORY OUTPUT")
        sys.exit(1)

    print("PASS ✅ deterministic (byte-identical advisory JSONL)")
    print("DONE ✅")


if __name__ == "__main__":
    main()
