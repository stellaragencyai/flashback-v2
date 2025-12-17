#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Phase 4 Smoke Test

Proves end-to-end:
setup_context -> pilot_decide (decision logged) -> outcome -> decision/outcome join -> inspector visibility

Run:
  python app/tools/phase4_smoke_test.py
"""

from __future__ import annotations

import time
from pathlib import Path

from app.ai.ai_events_spine import build_setup_context, build_outcome_record, publish_ai_event
from app.bots.ai_pilot import pilot_decide

LINKER = Path("app/ai/ai_decision_outcome_linker.py")
INSPECTOR = Path("app/ai/ai_decision_outcome_inspector.py")

def main() -> int:
    ts = int(time.time())
    tid = f"PHASE4_SMOKE_{ts}"

    # 1) setup + decision
    s = build_setup_context(
        trade_id=tid,
        symbol="BTCUSDT",
        account_label="main",
        strategy="test",
        features={"risk_usd": 10.0, "foo": 1},
        setup_type="test_probe",
        timeframe="5",
    )
    d = pilot_decide(s)

    print("\n=== DECISION ===")
    print("trade_id:", d.get("trade_id"))
    print("decision:", d.get("decision"))

    # 2) outcome
    publish_ai_event(build_outcome_record(
        trade_id=tid,
        symbol="BTCUSDT",
        account_label="main",
        strategy="test",
        pnl_usd=1.0,
        exit_reason="phase4_smoke",
    ))

    print("\n=== OUTCOME PUBLISHED ===")
    print("trade_id:", tid)

    # 3) reset linker cursor + output so we guarantee it reprocesses
    Path("state/ai_decision_outcome_cursor.json").unlink(missing_ok=True)
    Path("state/ai_decision_outcomes.jsonl").unlink(missing_ok=True)

    # 4) run linker
    import subprocess, sys
    print("\n=== LINKER RUN ===")
    r = subprocess.run([sys.executable, str(LINKER), "--once"], capture_output=True, text=True)
    print(r.stdout.strip() or r.stderr.strip())

    # 5) run inspector
    print("\n=== INSPECTOR RUN ===")
    r2 = subprocess.run([sys.executable, str(INSPECTOR), "--tail", "20"], capture_output=True, text=True)
    print(r2.stdout.strip() or r2.stderr.strip())

    # 6) quick assertion: joined file should contain tid
    out = Path("state/ai_decision_outcomes.jsonl")
    if not out.exists():
        print("\nFAIL: ai_decision_outcomes.jsonl missing")
        return 2

    b = out.read_bytes()
    if f"\"trade_id\":\"{tid}\"".encode("utf-8") not in b:
        print("\nFAIL: trade_id not found in joined output")
        return 3

    print("\nPASS ✅ Phase 4 join pipeline works end-to-end.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
