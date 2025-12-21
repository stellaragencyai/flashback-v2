#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Flashback — AI Events Integrity Sentinel

Asserts invariants for Phase 4/5 event streams:
- outcomes.jsonl must be ENRICHED ONLY
- outcomes_raw.jsonl must be outcome_record only (if present)
- outcomes_orphans.jsonl must be outcome_record only (if present)
- pending_setups.json must be bounded and mostly empty during idle periods
- enriched events should contain setup/outcome blocks for joinability

Exit code:
- 0 = PASS
- 2 = FAIL
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parents[2]
AI_DIR = ROOT / "state" / "ai_events"

OUTCOMES = AI_DIR / "outcomes.jsonl"
RAW = AI_DIR / "outcomes_raw.jsonl"
ORPH = AI_DIR / "outcomes_orphans.jsonl"
PENDING = AI_DIR / "pending_setups.json"

MAX_PENDING = 5000


def _read_jsonl(path: Path):
    if not path.exists():
        return []
    out = []
    for ln in path.read_text("utf-8", errors="ignore").splitlines():
        if not ln.strip():
            continue
        try:
            out.append(json.loads(ln))
        except Exception:
            out.append({"__malformed__": True})
    return out


def fail(msg: str):
    print("FAIL:", msg)
    sys.exit(2)


def main() -> int:
    print("=== AI Events Integrity Sentinel ===")
    print("ai_dir=", AI_DIR)

    # outcomes.jsonl: enriched-only
    outcomes = _read_jsonl(OUTCOMES)
    c = Counter()
    malformed = 0
    for d in outcomes:
        if isinstance(d, dict) and d.get("__malformed__"):
            malformed += 1
            continue
        if not isinstance(d, dict):
            c["<non_dict>"] += 1
            continue
        c[d.get("event_type", "<missing>")] += 1

    print("outcomes_total=", len(outcomes), "malformed=", malformed, "types=", dict(c))
    if malformed:
        fail("outcomes.jsonl has malformed lines")
    if any(k != "outcome_enriched" for k in c.keys()):
        fail("outcomes.jsonl contains non-enriched event_type(s)")

    # enriched structure spot-check (sample up to 20 from tail)
    tail = outcomes[-20:] if len(outcomes) > 20 else outcomes
    bad_struct = 0
    for d in tail:
        if not isinstance(d, dict):
            bad_struct += 1
            continue
        if d.get("event_type") != "outcome_enriched":
            bad_struct += 1
            continue
        if not isinstance(d.get("setup"), dict) or not isinstance(d.get("outcome"), dict):
            bad_struct += 1
    print("enriched_tail_struct_bad=", bad_struct, "(checking last", len(tail), ")")
    if bad_struct:
        fail("outcome_enriched missing setup/outcome blocks in tail sample")

    # outcomes_raw.jsonl: record-only
    if RAW.exists():
        raw = _read_jsonl(RAW)
        c2 = Counter()
        malformed2 = 0
        for d in raw:
            if isinstance(d, dict) and d.get("__malformed__"):
                malformed2 += 1
                continue
            if not isinstance(d, dict):
                c2["<non_dict>"] += 1
                continue
            c2[d.get("event_type", "<missing>")] += 1
        print("raw_total=", len(raw), "malformed=", malformed2, "types=", dict(c2))
        if malformed2:
            fail("outcomes_raw.jsonl has malformed lines")
        if any(k != "outcome_record" for k in c2.keys()):
            fail("outcomes_raw.jsonl contains non-outcome_record event_type(s)")

    # outcomes_orphans.jsonl: record-only
    if ORPH.exists():
        orph = _read_jsonl(ORPH)
        c3 = Counter()
        malformed3 = 0
        for d in orph:
            if isinstance(d, dict) and d.get("__malformed__"):
                malformed3 += 1
                continue
            if not isinstance(d, dict):
                c3["<non_dict>"] += 1
                continue
            c3[d.get("event_type", "<missing>")] += 1
        print("orphans_total=", len(orph), "malformed=", malformed3, "types=", dict(c3))
        if malformed3:
            fail("outcomes_orphans.jsonl has malformed lines")
        if any(k != "outcome_record" for k in c3.keys()):
            fail("outcomes_orphans.jsonl contains non-outcome_record event_type(s)")

    # pending_setups.json: bounded
    if PENDING.exists():
        try:
            pend = json.loads(PENDING.read_text("utf-8"))
        except Exception:
            fail("pending_setups.json is not valid JSON")
        if not isinstance(pend, dict):
            fail("pending_setups.json is not a dict")
        print("pending_count=", len(pend), "max_allowed=", MAX_PENDING)
        if len(pend) > MAX_PENDING:
            fail("pending_setups.json exceeds max pending cap")
    else:
        print("pending_count= 0 (file missing)")

    print("PASS: invariants hold")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
