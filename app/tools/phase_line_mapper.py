#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
from pathlib import Path
import re
import json

FILES = [
    r"app\bots\executor_v2.py",
    r"app\core\flashback_common.py",
    r"app\core\ai_state_bus.py",
    r"app\core\position_bus.py",
    r"app\core\ws_switchboard.py",
    r"app\ws\ws_switchboard.py",
    r"app\bots\ws_switchboard.py",
    r"app\ai\ai_decision_enforcer.py",
    r"app\core\ai_decision_logger.py",
    r"app\ai\ai_memory_contract.py",
    r"app\ai\ai_decision_outcome_linker.py",
]

PHASE_PATTERNS = [
    ("P1", "Core Infra / Exchange", [
        r"\bbybit_(get|post)\b", r"\bHMAC\b|\bhashlib\b", r"\bdotenv\b", r"\brequests\b",
        r"\bget_logger\b|\blogger\b", r"\bENV\b|\bos\.getenv\b", r"state[\\/]",
    ]),
    ("P2", "Multi-Account / Capital", [
        r"\bACCOUNT_LABEL\b", r"\bsubaccount\b|\bsubs\b", r"\btransfer\b|\bsweep\b|\bfunding\b|\ballocation\b",
    ]),
    ("P3", "Market Data / Execution Plumbing", [
        r"\bws\b.*\bswitchboard\b", r"\borderbook_bus\b|\btrades_bus\b|\bexecutions\b", r"\bplace_order\b",
        r"\breduceOnly\b", r"\bidempot", r"\bslippage\b",
    ]),
    ("P4", "Decision Gate / Enforcement", [
        r"\bai_decision\b", r"\b(enforce|gate|block)\b", r"\bALLOW\b|\bBLOCK\b", r"\bsize_multiplier\b",
        r"\bemit_ai_decision\b", r"\bai_decisions\.jsonl\b",
    ]),
    ("P5", "Memory Contract / Journal", [
        r"\bai_memory_contract\b", r"\bsetup_fingerprint\b|\bmemory_fingerprint\b",
        r"\bjournal\b", r"\bmemory_records\.jsonl\b|\bmemory_snapshot\.json\b",
    ]),
    ("P6", "Replay / Integrity / Analytics", [
        r"\breplay\b|\brebuild\b|\bintegrity\b", r"\blearning\.sqlite\b", r"\brollup\b|\bprune\b|\bstats\b",
    ]),
    ("P7", "State Bus / Safety Snapshot", [
        r"\bai_state_bus\b", r"\bbuild_ai_snapshot\b|\bbuild_symbol_state\b",
        r"\bfreshness\b", r"\bis_safe\b", r"\binclude_orderbook\b|\binclude_trades\b",
        r"\bpositions_bus\b",
    ]),
    ("P8", "Outcome Linking / Learning Signals", [
        r"\boutcome\b", r"\br_multiple\b|\bR-multiple\b", r"\bexpectancy\b",
        r"\boutcome_link\b|\bdecision.*outcome\b",
    ]),
    ("P9", "Adaptive / Auto-Execution", [
        r"\badaptive\b|\bregime\b", r"\bauto_execute\b|\bstrategy_select\b", r"\bpromotion\b|\bdemotion\b",
    ]),
    ("P10", "Portfolio Intelligence / Fleet", [
        r"\bportfolio\b|\brebalance\b", r"\bfleet\b|\bsupervisor\b|\bglobal policy\b", r"\bcross-account\b",
    ]),
]

def compile_patterns():
    compiled = []
    for code, title, pats in PHASE_PATTERNS:
        compiled.append((code, title, [re.compile(p, re.I) for p in pats]))
    return compiled

COMPILED = compile_patterns()

def tag_line(line: str):
    tags = []
    for code, title, rxs in COMPILED:
        if any(rx.search(line) for rx in rxs):
            tags.append((code, title))
    return tags

def coalesce(tagged):
    # tagged: list of (lineno, text, tags[code])
    segments = []
    cur = None
    for lineno, line, codes in tagged:
        codes_sorted = sorted(set(codes))
        if not codes_sorted:
            if cur:
                segments.append(cur)
                cur = None
            continue
        if not cur or cur["codes"] != codes_sorted or lineno != cur["end"] + 1:
            if cur:
                segments.append(cur)
            cur = {"start": lineno, "end": lineno, "codes": codes_sorted}
        else:
            cur["end"] = lineno
    if cur:
        segments.append(cur)
    return segments

def main():
    out = {"files": {}, "missing": []}
    for fp in FILES:
        p = Path(fp)
        if not p.exists():
            out["missing"].append(fp.replace("\\", "/"))
            continue
        lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()
        tagged = []
        for i, line in enumerate(lines, start=1):
            tags = tag_line(line)
            codes = [t[0] for t in tags]
            tagged.append((i, line, codes))
        segs = coalesce(tagged)
        out["files"][fp.replace("\\", "/")] = {
            "total_lines": len(lines),
            "segments": segs[:500],  # cap to avoid insane output
        }

    Path("state/reports").mkdir(parents=True, exist_ok=True)
    Path("state/reports/phase_line_map.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
    print("OK: wrote state/reports/phase_line_map.json")
    if out["missing"]:
        print("MISSING_FILES:", ", ".join(out["missing"]))

if __name__ == "__main__":
    main()
