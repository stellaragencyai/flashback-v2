#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from pathlib import Path
from typing import Any, Dict

ROOT = Path(__file__).resolve().parents[2]
AI = ROOT / "state" / "ai_events"

SETUPS = AI / "setups.jsonl"
OUTCOMES = AI / "outcomes.jsonl"
SETUPS_RAW = AI / "setups_raw.jsonl"
OUTCOMES_RAW = AI / "outcomes_raw.jsonl"
UNMATCHED = AI / "outcomes_unmatched.jsonl"

def read_lines(p: Path):
    if not p.exists():
        return []
    return [ln for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]

def write_lines(p: Path, objs):
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        for o in objs:
            f.write(json.dumps(o, separators=(",", ":"), ensure_ascii=False))
            f.write("\n")

def main():
    AI.mkdir(parents=True, exist_ok=True)

    # Preserve existing canonicals as legacy (no deletion)
    if SETUPS.exists():
        SETUPS.rename(AI / "setups.legacy.jsonl")
    if OUTCOMES.exists():
        OUTCOMES.rename(AI / "outcomes.legacy.jsonl")

    # Rebuild: only keep schema-valid setups + enriched outcomes
    setups_out = []
    setups_raw_out = []

    # Pull from legacy setups + setups_raw if present
    legacy_setups = read_lines(AI / "setups.legacy.jsonl")
    existing_setups_raw = read_lines(SETUPS_RAW)

    for ln in legacy_setups + existing_setups_raw:
        try:
            o = json.loads(ln)
        except Exception:
            continue
        if isinstance(o, dict) and o.get("event_type") == "setup_context":
            # minimal canonical requirement
            ok = all(k in o and o.get(k) not in (None, "", "UNKNOWN") for k in
                     ("schema_version","account_label","strategy_name","symbol","timeframe"))
            if ok:
                setups_out.append(o)
            else:
                setups_raw_out.append(o)

    outcomes_out = []

    # Outcomes: keep only outcome_enriched from legacy outcomes
    legacy_outcomes = read_lines(AI / "outcomes.legacy.jsonl")
    for ln in legacy_outcomes:
        try:
            o = json.loads(ln)
        except Exception:
            continue
        if isinstance(o, dict) and o.get("event_type") == "outcome_enriched":
            outcomes_out.append(o)

    # Write rebuilt canonicals
    write_lines(SETUPS, setups_out)
    write_lines(OUTCOMES, outcomes_out)

    # Keep raw forensics (append-only behavior preserved by your spine, but rebuild consolidates)
    if setups_raw_out:
        write_lines(SETUPS_RAW, setups_raw_out)

    print("Rebuild complete:")
    print(" - setups.jsonl:", len(setups_out))
    print(" - outcomes.jsonl:", len(outcomes_out))
    print(" - setups_raw.jsonl:", len(setups_raw_out))
    print("Legacy preserved:")
    print(" - setups.legacy.jsonl exists:", (AI / "setups.legacy.jsonl").exists())
    print(" - outcomes.legacy.jsonl exists:", (AI / "outcomes.legacy.jsonl").exists())

if __name__ == "__main__":
    main()
