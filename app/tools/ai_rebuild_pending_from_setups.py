#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Rebuild pending_setups.json from setups.jsonl (Phase 3 reconciler) v1.0

- Reads state/ai_events/setups.jsonl and outcomes.jsonl
- For every setup_context trade_id missing an outcome trade_id:
    writes it into state/ai_events/pending_setups.json

This is a deterministic rebuild to allow expire/reconcile tools to work again.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterator

import orjson

ROOT = Path(__file__).resolve().parents[2]
AI_DIR = ROOT / "state" / "ai_events"
SETUPS = AI_DIR / "setups.jsonl"
OUTCOMES = AI_DIR / "outcomes.jsonl"
PENDING = AI_DIR / "pending_setups.json"

def _iter_jsonl(path: Path) -> Iterator[Dict[str, Any]]:
    if not path.exists():
        return
    for b in path.read_bytes().splitlines():
        s = b.strip()
        if not s or s[:1] != b"{":
            continue
        try:
            obj = orjson.loads(s)
        except Exception:
            continue
        if isinstance(obj, dict):
            yield obj

def _safe_tid(x: Any) -> str:
    try:
        if x is None:
            return ""
        return str(x).strip()
    except Exception:
        return ""

def main() -> int:
    print("=== rebuild_pending_from_setups ===")
    print("root        :", ROOT)
    print("setups      :", SETUPS)
    print("outcomes    :", OUTCOMES)
    print("pending_out :", PENDING)

    if not SETUPS.exists():
        print("FAIL: setups.jsonl missing")
        return 2

    # Load outcomes trade_ids (unique)
    outcome_tids = set()
    if OUTCOMES.exists():
        for o in _iter_jsonl(OUTCOMES):
            et = o.get("event_type")
            if et not in ("outcome_record", "outcome_enriched"):
                continue
            tid = _safe_tid(o.get("trade_id"))
            if tid:
                outcome_tids.add(tid)

    # Load setups and keep only missing outcomes
    pending: Dict[str, Any] = {}
    setups_seen = 0
    missing = 0

    for s in _iter_jsonl(SETUPS):
        if s.get("event_type") != "setup_context":
            continue
        setups_seen += 1
        tid = _safe_tid(s.get("trade_id"))
        if not tid:
            continue
        if tid in outcome_tids:
            continue
        pending[tid] = s
        missing += 1

    AI_DIR.mkdir(parents=True, exist_ok=True)
    PENDING.write_text(json.dumps(pending, indent=2, sort_keys=True), encoding="utf-8")

    print("setups_seen             :", setups_seen)
    print("outcomes_trade_ids      :", len(outcome_tids))
    print("rebuilt_pending_entries :", len(pending))
    print("DONE")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
