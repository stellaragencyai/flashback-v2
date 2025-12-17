#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — manual allow decision writer

Writes a manual override ALLOW decision row to state/ai_decisions.jsonl
Optionally includes size_multiplier for deterministic Step-2 sizing tests.

Usage:
  python app/tools/manual_allow_decision.py TRADE_ID "reason" [size_multiplier]

Examples:
  python app/tools/manual_allow_decision.py ALLOW_SM1 "phase4_matrix_manual_allow_sm1" 1.0
  python app/tools/manual_allow_decision.py ALLOW_SM025 "phase4_matrix_manual_allow_sm025" 0.25
"""

from __future__ import annotations

import sys
import json
import time
from pathlib import Path

try:
    from app.core.config import settings
    ROOT = settings.ROOT
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

DECISIONS_PATH = ROOT / "state" / "ai_decisions.jsonl"
DECISIONS_PATH.parent.mkdir(parents=True, exist_ok=True)


def main() -> int:
    if len(sys.argv) < 3:
        print("usage: manual_allow_decision.py TRADE_ID \"reason\" [size_multiplier]")
        return 2

    trade_id = str(sys.argv[1]).strip()
    reason = str(sys.argv[2]).strip() or "manual_allow"

    size_multiplier = None
    if len(sys.argv) >= 4:
        raw = str(sys.argv[3]).strip()
        if raw != "":
            try:
                size_multiplier = float(raw)
            except Exception:
                size_multiplier = None

    row = {
        "ts_ms": int(time.time() * 1000),
        "event_type": "ai_decision",
        "trade_id": trade_id,
        "allow": True,
        "decision_code": "ALLOW_TRADE",
        "reason": reason,
    }
    if size_multiplier is not None:
        row["size_multiplier"] = float(size_multiplier)

    with DECISIONS_PATH.open("ab") as f:
        f.write(json.dumps(row, ensure_ascii=False).encode("utf-8") + b"\n")

    print("allow_written", trade_id, reason, ("size_multiplier=" + str(size_multiplier) if size_multiplier is not None else "size_multiplier=None"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
