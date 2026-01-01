#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Phase 3 Backfill: Enrich Outcomes with Setup Fingerprints (v1.0)

Why this exists
---------------
Your integrity check shows outcomes_missing_fp, which means outcomes.jsonl
contains outcome_record entries that never got merged with setup_context
(via pending_setups). That breaks setup_perf scoring.

This tool:
- Loads setups.jsonl (setup_context events) keyed by trade_id
- Streams outcomes.jsonl (outcome_record or outcome_enriched)
- For each outcome_record, merges with matching setup_context -> outcome_enriched
- Writes a NEW file: outcomes.enriched.backfill.jsonl
- Optionally swaps it into outcomes.jsonl safely (backups first)

It does NOT invent data. If a setup is missing, it keeps the original outcome_record.

Usage
-----
python app/tools/phase3_backfill_enrich_outcomes.py --swap
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Any, Dict, Optional

import orjson


def _now_ms() -> int:
    return int(time.time() * 1000)


def _load_jsonl_by_trade_id(path: Path, want_event_type: str) -> Dict[str, Dict[str, Any]]:
    """
    Loads JSONL and returns dict[trade_id] = event for the specified event_type.
    Keeps the LAST event per trade_id (latest wins).
    """
    out: Dict[str, Dict[str, Any]] = {}
    if not path.exists():
        return out

    with path.open("rb") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                evt = orjson.loads(line)
            except Exception:
                continue
            if not isinstance(evt, dict):
                continue
            if evt.get("event_type") != want_event_type:
                continue
            tid = evt.get("trade_id")
            if not isinstance(tid, str) or not tid.strip():
                continue
            out[tid.strip()] = evt
    return out


def _extract_pnl_usd(outcome_evt: Dict[str, Any]) -> float:
    payload = outcome_evt.get("payload")
    if isinstance(payload, dict) and payload.get("pnl_usd") is not None:
        try:
            return float(payload["pnl_usd"])
        except Exception:
            pass
    # legacy fallback
    try:
        return float(outcome_evt.get("pnl_usd") or 0.0)
    except Exception:
        return 0.0


def _extract_r_multiple(outcome_evt: Dict[str, Any]) -> Optional[float]:
    payload = outcome_evt.get("payload")
    if isinstance(payload, dict) and payload.get("r_multiple") is not None:
        try:
            return float(payload["r_multiple"])
        except Exception:
            return None
    return None


def _extract_setup_features(setup_evt: Dict[str, Any]) -> Dict[str, Any]:
    payload = setup_evt.get("payload")
    if not isinstance(payload, dict):
        return {}
    feats = payload.get("features")
    if isinstance(feats, dict):
        return feats
    return {}


def _merge_setup_and_outcome(setup_evt: Dict[str, Any], outcome_evt: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create outcome_enriched record using setup + outcome.
    Keeps canonical identity from setup when available.
    """
    setup_feats = _extract_setup_features(setup_evt)
    pnl_usd = _extract_pnl_usd(outcome_evt)

    risk_usd = setup_feats.get("risk_usd")
    r_multiple = None
    win = None

    # Prefer existing r_multiple if already provided (rare)
    existing_r = _extract_r_multiple(outcome_evt)
    if existing_r is not None:
        r_multiple = existing_r
        win = r_multiple > 0
    else:
        # Only compute if risk_usd is valid and non-zero
        if risk_usd not in (None, "", "0", 0):
            try:
                denom = float(risk_usd)
                if denom != 0:
                    r_multiple = float(pnl_usd) / denom
                    win = r_multiple > 0
            except Exception:
                r_multiple = None
                win = None

    enriched: Dict[str, Any] = {
        "event_type": "outcome_enriched",
        "ts": _now_ms(),
        "schema_version": setup_evt.get("schema_version") or outcome_evt.get("schema_version") or 3,
        "trade_id": setup_evt.get("trade_id") or outcome_evt.get("trade_id"),
        "symbol": setup_evt.get("symbol") or outcome_evt.get("symbol"),
        "timeframe": setup_evt.get("timeframe") or outcome_evt.get("timeframe"),
        "account_label": setup_evt.get("account_label") or outcome_evt.get("account_label"),
        "strategy_name": setup_evt.get("strategy_name") or outcome_evt.get("strategy_name") or outcome_evt.get("strategy"),
        "strategy": setup_evt.get("strategy") or outcome_evt.get("strategy") or setup_evt.get("strategy_name"),
        "setup_type": setup_evt.get("setup_type"),
        "ai_profile": setup_evt.get("ai_profile") or (setup_evt.get("payload", {}).get("ai_profile") if isinstance(setup_evt.get("payload"), dict) else None),
        "setup": setup_evt,
        "outcome": outcome_evt,
        "stats": {
            "pnl_usd": float(pnl_usd),
            "risk_usd": float(risk_usd) if risk_usd not in (None, "", "null") else None,
            "r_multiple": float(r_multiple) if r_multiple is not None else None,
            "win": win,
        },
    }

    # Minimal data-quality warnings (don’t lie, just flag)
    dq = []
    if not enriched.get("timeframe") or enriched.get("timeframe") in ("UNKNOWN", ""):
        dq.append("timeframe_missing_or_unknown")
    if not setup_feats.get("setup_fingerprint"):
        dq.append("setup_missing_fingerprint")
    if dq:
        enriched.setdefault("payload", {})
        if isinstance(enriched["payload"], dict):
            enriched["payload"].setdefault("data_quality", {})
            if isinstance(enriched["payload"]["data_quality"], dict):
                enriched["payload"]["data_quality"]["warnings"] = dq

    return enriched


def backfill(
    root: Path,
    *,
    swap: bool,
) -> int:
    ai_dir = root / "state" / "ai_events"
    setups_path = ai_dir / "setups.jsonl"
    outcomes_path = ai_dir / "outcomes.jsonl"
    out_path = ai_dir / "outcomes.enriched.backfill.jsonl"

    if not setups_path.exists():
        raise SystemExit(f"Missing {setups_path}")

    if not outcomes_path.exists():
        raise SystemExit(f"Missing {outcomes_path}")

    setups = _load_jsonl_by_trade_id(setups_path, "setup_context")

    total = 0
    enriched_count = 0
    kept_count = 0
    missing_setup = 0

    with outcomes_path.open("rb") as fin, out_path.open("wb") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            total += 1
            try:
                evt = orjson.loads(line)
            except Exception:
                continue
            if not isinstance(evt, dict):
                continue

            et = evt.get("event_type")

            # Keep already-enriched outcomes as-is
            if et == "outcome_enriched":
                fout.write(orjson.dumps(evt))
                fout.write(b"\n")
                kept_count += 1
                continue

            # Only attempt merge for outcome_record
            if et != "outcome_record":
                fout.write(orjson.dumps(evt))
                fout.write(b"\n")
                kept_count += 1
                continue

            tid = evt.get("trade_id")
            if not isinstance(tid, str) or not tid.strip():
                fout.write(orjson.dumps(evt))
                fout.write(b"\n")
                kept_count += 1
                continue

            setup_evt = setups.get(tid.strip())
            if not setup_evt:
                # No matching setup, keep raw outcome
                fout.write(orjson.dumps(evt))
                fout.write(b"\n")
                missing_setup += 1
                continue

            enriched = _merge_setup_and_outcome(setup_evt, evt)
            fout.write(orjson.dumps(enriched))
            fout.write(b"\n")
            enriched_count += 1

    print("\nBackfill complete")
    print(f"• total_outcomes_read      : {total}")
    print(f"• already_enriched_kept    : {kept_count}")
    print(f"• enriched_from_records    : {enriched_count}")
    print(f"• missing_setup_kept_raw   : {missing_setup}")
    print(f"• output                  : {out_path}")

    if swap:
        ts = time.strftime("%Y%m%d_%H%M%S")
        backup = ai_dir / f"outcomes.backup_{ts}.jsonl"
        outcomes_path.replace(backup)
        out_path.replace(outcomes_path)
        print(f"\nSWAP DONE")
        print(f"• backup old outcomes.jsonl -> {backup}")
        print(f"• new outcomes.jsonl        -> {outcomes_path}")

    return enriched_count


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="", help="Project root (auto if empty)")
    ap.add_argument("--swap", action="store_true", help="Replace outcomes.jsonl after backfill (backs up first)")
    args = ap.parse_args()

    if args.root:
        root = Path(args.root).resolve()
    else:
        # script is app/tools/..., so root is 2 parents up from app/
        root = Path(__file__).resolve().parents[2]

    backfill(root, swap=args.swap)


if __name__ == "__main__":
    main()
