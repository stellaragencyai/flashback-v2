#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Patch: Add backward-compatible assert_outcome_row_ok() to outcome_contract.py
So existing tools (verify_outcome_contract.py) keep working.
"""

from __future__ import annotations

from pathlib import Path
import re

p = Path(r"app\ai\outcome_contract.py")
if not p.exists():
    raise SystemExit(f"Missing: {p}")

s = p.read_text(encoding="utf-8", errors="ignore")

# If it's already there, exit cleanly
if "def assert_outcome_row_ok" in s:
    print("OK: assert_outcome_row_ok already present")
    raise SystemExit(0)

# Ensure validate_outcome_v1 exists
if "def validate_outcome_v1" not in s:
    raise SystemExit("Expected validate_outcome_v1(...) in outcome_contract.py but not found")

alias = r'''

# ---------------------------------------------------------------------------
# Backward-compatible alias expected by older tools
# ---------------------------------------------------------------------------
def assert_outcome_row_ok(row: Dict[str, Any]) -> None:
    """
    Compatibility shim: verify_outcome_contract.py imports this.
    Canonical validation lives in validate_outcome_v1().
    """
    validate_outcome_v1(row)
'''.rstrip() + "\n"

# Append at end of file
s2 = s.rstrip() + "\n" + alias
p.write_text(s2, encoding="utf-8")

print("OK: patched outcome_contract.py (added assert_outcome_row_ok alias)")
