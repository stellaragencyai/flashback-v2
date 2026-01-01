#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback Phase Mapper (local static analysis)
- Scans .py files under app/
- Extracts file-level and symbol-level anchors (def/class)
- Tags each file to one or more phases using deterministic heuristics
- Emits a report you can paste or upload

This is NOT perfect "semantic understanding"; it's a structured evidence map.
"""

from __future__ import annotations
from pathlib import Path
import re
import json
from dataclasses import dataclass, asdict
from typing import List, Dict, Tuple

ROOT = Path(".")
APP = ROOT / "app"

PHASES = [
    ("P1", "Core Infrastructure & Exchange Abstraction"),
    ("P2", "Multi-Account & Capital Topology"),
    ("P3", "Market Data & Execution Plumbing"),
    ("P4", "AI Decision Gate & Enforcement"),
    ("P5", "AI Memory Contract & Journal"),
    ("P6", "Memory Integrity, Replay & Analytics"),
    ("P7", "AI State Bus & Safety Snapshot"),
    ("P8", "Decision Outcome Linking & Learning Signals"),
    ("P9", "Adaptive Strategy & Auto-Execution"),
    ("P10", "Autonomous Portfolio Intelligence"),
]

# Heuristic rules: (phase_code, weight, regex pattern, reason)
RULES: List[Tuple[str, int, re.Pattern, str]] = []

def add(phase: str, weight: int, pattern: str, reason: str) -> None:
    RULES.append((phase, weight, re.compile(pattern, re.I), reason))

# ---- P1 ----
add("P1", 4, r"\bflashback_common\.py\b|\bbybit_(get|post)\b|HMAC|hashlib|requests|dotenv|ENV|secrets", "infra/exchange abstraction")
add("P1", 3, r"\blogger\b|get_logger|rotat(e|ion)|state[/\\]", "infra/logging/state roots")

# ---- P2 ----
add("P2", 5, r"\bsubaccount\b|\bsubs\.py\b|ACCOUNT_LABEL|transfer|sweep|funding|allocation", "multi-account/capital topology")

# ---- P3 ----
add("P3", 5, r"\bws_switchboard\b|orderbook_bus|trades_bus|executions|place_order|reduceOnly|slippage|idempot", "market data + execution plumbing")

# ---- P4 ----
add("P4", 6, r"\bai_decision_enforcer\b|\benforce\b.*\bdecision\b|ALLOW|BLOCK|size_multiplier|ai_decisions\.jsonl|emit_ai_decision", "decision gate/enforcement")

# ---- P5 ----
add("P5", 6, r"\bai_memory_contract\b|memory_fingerprint|setup_fingerprint|journal|memory_records\.jsonl|memory_snapshot\.json", "memory contract/journal")

# ---- P6 ----
add("P6", 6, r"learning\.sqlite|rebuild|replay|integrity|backfill|analytics|memory_stats|prune|rollup", "memory integrity/replay/analytics")

# ---- P7 ----
add("P7", 7, r"\bai_state_bus\b|build_ai_snapshot|build_symbol_state|freshness|is_safe|positions_bus|orderbook optional|include_orderbook|include_trades", "state bus + safety snapshot")

# ---- P8 ----
add("P8", 7, r"outcome|R-multiple|r_multiple|expectancy|decision.*outcome|outcome_link|join.*decision|label.*win|loss", "decision-outcome linking/learning signals")

# ---- P9 ----
add("P9", 6, r"adaptive|regime|auto_execute|strategy_select|promotion|demotion|canary learning|weights", "adaptive strategy/auto-execution")

# ---- P10 ----
add("P10", 6, r"portfolio|rebalance|capital allocator|cross-account|autonomous|supervisor|global policy|fleet", "portfolio intelligence/coordination")

DEF_RE = re.compile(r"^(def|class)\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(", re.M)

@dataclass
class FileHit:
    path: str
    size: int
    phases: Dict[str, int]
    reasons: Dict[str, List[str]]
    symbols: List[str]

def score_text(txt: str) -> Tuple[Dict[str, int], Dict[str, List[str]]]:
    scores: Dict[str, int] = {p[0]: 0 for p in PHASES}
    reasons: Dict[str, List[str]] = {p[0]: [] for p in PHASES}
    for phase, w, rx, why in RULES:
        if rx.search(txt):
            scores[phase] += w
            reasons[phase].append(why)
    # Remove zeros
    scores = {k: v for k, v in scores.items() if v > 0}
    reasons = {k: v for k, v in reasons.items() if k in scores}
    return scores, reasons

def main() -> None:
    if not APP.exists():
        raise SystemExit("ERROR: app/ folder not found. Run from repo root (C:\\flashback).")

    hits: List[FileHit] = []

    for f in APP.rglob("*.py"):
        try:
            txt = f.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        scores, reasons = score_text(txt + "\n" + str(f))
        symbols = [m.group(2) for m in DEF_RE.finditer(txt)][:60]
        hits.append(FileHit(str(f).replace("\\", "/"), f.stat().st_size, scores, reasons, symbols))

    # Rank phases per file
    out = []
    for h in hits:
        ranked = sorted(h.phases.items(), key=lambda kv: kv[1], reverse=True)
        primary = ranked[0][0] if ranked else None
        confidence = ranked[0][1] if ranked else 0
        out.append({
            "path": h.path,
            "size": h.size,
            "primary_phase": primary,
            "score": confidence,
            "all_phase_scores": dict(ranked),
            "reasons": h.reasons,
            "symbols_sample": h.symbols,
        })

    # Summaries
    phase_totals: Dict[str, int] = {p[0]: 0 for p in PHASES}
    phase_files: Dict[str, int] = {p[0]: 0 for p in PHASES}
    for row in out:
        ph = row["primary_phase"]
        if ph:
            phase_files[ph] += 1
            phase_totals[ph] += int(row["score"] or 0)

    summary = {
        "repo_root": str(ROOT.resolve()),
        "files_scanned": len(out),
        "phase_summary": {
            ph: {
                "title": dict(PHASES)[ph],
                "primary_files": phase_files[ph],
                "total_primary_score": phase_totals[ph],
            } for ph, _ in PHASES
        },
        "top_files_by_phase": {}
    }

    for ph, _ in PHASES:
        rows = [r for r in out if r["primary_phase"] == ph]
        rows.sort(key=lambda r: r["score"], reverse=True)
        summary["top_files_by_phase"][ph] = rows[:12]

    # Write outputs
    Path("state").mkdir(exist_ok=True)
    Path("state/reports").mkdir(parents=True, exist_ok=True)
    Path("state/reports/phase_map_report.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    Path("state/reports/phase_map_files.json").write_text(json.dumps(out, indent=2), encoding="utf-8")

    print("OK: wrote")
    print(" - state/reports/phase_map_report.json")
    print(" - state/reports/phase_map_files.json")
    print("files_scanned:", len(out))

if __name__ == "__main__":
    main()
