#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Backfill Outcome Fingerprints v1.0

What it does
------------
Backfills missing setup_fingerprint into state/ai_events/outcomes.jsonl
using state/ai_events/setups.jsonl as the source of truth.

Targets
-------
- outcome_enriched:
    Ensures:
      setup.payload.features.setup_fingerprint
      setup.payload.features.setup_fingerprint_features (if known)
    Also sets:
      top-level setup_fingerprint (optional helper)

- outcome_record (raw, non-enriched):
    Sets:
      top-level setup_fingerprint
      payload.extra.setup_fingerprint
      payload.extra.setup_fingerprint_features (if known)

Safety
------
- Creates a timestamped backup:
    outcomes.jsonl.bak.YYYYMMDD_HHMMSS
- Writes to a temp file, then atomically replaces the original.

Usage
-----
python app/tools/backfill_outcome_fingerprints.py
python app/tools/backfill_outcome_fingerprints.py --dry-run
python app/tools/backfill_outcome_fingerprints.py --root "C:\\Flashback"

Exit codes
----------
0 success
2 missing files / unrecoverable
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import orjson


def _now_ts_str() -> str:
    return time.strftime("%Y%m%d_%H%M%S", time.localtime())


def _safe_get(d: Any, *keys: str) -> Any:
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _ensure_dict(parent: Dict[str, Any], key: str) -> Dict[str, Any]:
    v = parent.get(key)
    if isinstance(v, dict):
        return v
    v = {}
    parent[key] = v
    return v


def _load_tradeid_to_fp(setups_path: Path) -> Tuple[Dict[str, str], Dict[str, Dict[str, Any]]]:
    """
    Returns:
      trade_id -> fingerprint
      trade_id -> fp_features (optional dict)
    """
    trade_to_fp: Dict[str, str] = {}
    trade_to_fp_features: Dict[str, Dict[str, Any]] = {}

    if not setups_path.exists():
        return trade_to_fp, trade_to_fp_features

    with setups_path.open("rb") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            try:
                evt = orjson.loads(line)
            except Exception:
                continue
            if not isinstance(evt, dict):
                continue
            if evt.get("event_type") != "setup_context":
                continue

            trade_id = evt.get("trade_id")
            if not isinstance(trade_id, str) or not trade_id.strip():
                continue
            trade_id = trade_id.strip()

            feats = _safe_get(evt, "payload", "features")
            if not isinstance(feats, dict):
                continue

            fp = feats.get("setup_fingerprint")
            if isinstance(fp, str) and fp.strip():
                trade_to_fp[trade_id] = fp.strip()

            fpf = feats.get("setup_fingerprint_features")
            if isinstance(fpf, dict) and fpf:
                trade_to_fp_features[trade_id] = fpf

    return trade_to_fp, trade_to_fp_features


def _backfill_one_outcome(
    evt: Dict[str, Any],
    trade_to_fp: Dict[str, str],
    trade_to_fp_features: Dict[str, Dict[str, Any]],
) -> Tuple[bool, bool]:
    """
    Returns:
      (changed, fp_found)
    """
    changed = False

    trade_id = evt.get("trade_id")
    if not isinstance(trade_id, str) or not trade_id.strip():
        return False, False
    trade_id = trade_id.strip()

    fp = trade_to_fp.get(trade_id)
    fp_features = trade_to_fp_features.get(trade_id)

    if not fp:
        return False, False

    et = evt.get("event_type")

    # Helper: attach top-level fingerprint (works for both event types)
    if evt.get("setup_fingerprint") != fp:
        evt["setup_fingerprint"] = fp
        changed = True

    if isinstance(fp_features, dict) and fp_features:
        # Optional: attach top-level fp features too
        if not isinstance(evt.get("setup_fingerprint_features"), dict):
            evt["setup_fingerprint_features"] = fp_features
            changed = True

    if et == "outcome_enriched":
        setup = evt.get("setup")
        if isinstance(setup, dict):
            payload = _ensure_dict(setup, "payload")
            feats = _ensure_dict(payload, "features")

            if feats.get("setup_fingerprint") != fp:
                feats["setup_fingerprint"] = fp
                changed = True

            if isinstance(fp_features, dict) and fp_features:
                if not isinstance(feats.get("setup_fingerprint_features"), dict):
                    feats["setup_fingerprint_features"] = fp_features
                    changed = True

    elif et == "outcome_record":
        payload = evt.get("payload")
        if not isinstance(payload, dict):
            payload = {}
            evt["payload"] = payload
            changed = True

        extra = payload.get("extra")
        if not isinstance(extra, dict):
            extra = {}
            payload["extra"] = extra
            changed = True

        if extra.get("setup_fingerprint") != fp:
            extra["setup_fingerprint"] = fp
            changed = True

        if isinstance(fp_features, dict) and fp_features:
            if not isinstance(extra.get("setup_fingerprint_features"), dict):
                extra["setup_fingerprint_features"] = fp_features
                changed = True

    return changed, True


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="", help="Project root (defaults to cwd).")
    ap.add_argument("--dry-run", action="store_true", help="Do not write anything; just report.")
    ap.add_argument("--outcomes", default="", help="Override outcomes.jsonl path.")
    ap.add_argument("--setups", default="", help="Override setups.jsonl path.")
    args = ap.parse_args()

    root = Path(args.root).resolve() if args.root else Path.cwd().resolve()

    outcomes_path = Path(args.outcomes).resolve() if args.outcomes else (root / "state" / "ai_events" / "outcomes.jsonl")
    setups_path = Path(args.setups).resolve() if args.setups else (root / "state" / "ai_events" / "setups.jsonl")

    if not outcomes_path.exists():
        print(f"❌ Missing outcomes file: {outcomes_path}")
        return 2
    if not setups_path.exists():
        print(f"❌ Missing setups file: {setups_path}")
        return 2

    print("📌 Backfill Outcome Fingerprints")
    print(f"• root     : {root}")
    print(f"• setups   : {setups_path}")
    print(f"• outcomes : {outcomes_path}")
    print(f"• dry_run  : {args.dry_run}")
    print("")

    trade_to_fp, trade_to_fp_features = _load_tradeid_to_fp(setups_path)
    print(f"• setups indexed (trade_id→fp): {len(trade_to_fp)}")
    print("")

    tmp_path = outcomes_path.with_suffix(outcomes_path.suffix + ".tmp")
    bak_path = outcomes_path.with_suffix(outcomes_path.suffix + f".bak.{_now_ts_str()}")

    total = 0
    parsed_ok = 0
    changed_lines = 0
    fp_found = 0
    fp_missing = 0
    enriched_seen = 0
    outcome_record_seen = 0

    # Stream read/write
    out_f = None
    try:
        if not args.dry_run:
            tmp_path.parent.mkdir(parents=True, exist_ok=True)
            out_f = tmp_path.open("wb")

        with outcomes_path.open("rb") as f:
            for raw in f:
                total += 1
                line = raw.strip()
                if not line:
                    if not args.dry_run and out_f:
                        out_f.write(raw)
                    continue

                try:
                    evt = orjson.loads(line)
                except Exception:
                    # preserve original line
                    if not args.dry_run and out_f:
                        out_f.write(raw)
                    continue

                if not isinstance(evt, dict):
                    if not args.dry_run and out_f:
                        out_f.write(raw)
                    continue

                parsed_ok += 1

                et = evt.get("event_type")
                if et == "outcome_enriched":
                    enriched_seen += 1
                elif et == "outcome_record":
                    outcome_record_seen += 1

                # Decide if it needs backfill (fast check)
                already_has_fp = False
                if et == "outcome_enriched":
                    fp0 = _safe_get(evt, "setup", "payload", "features", "setup_fingerprint")
                    already_has_fp = isinstance(fp0, str) and bool(fp0.strip())
                else:
                    fp0 = evt.get("setup_fingerprint")
                    already_has_fp = isinstance(fp0, str) and bool(fp0.strip())

                did_change = False
                did_find = False

                if not already_has_fp:
                    did_change, did_find = _backfill_one_outcome(evt, trade_to_fp, trade_to_fp_features)
                    if did_find:
                        fp_found += 1
                    else:
                        fp_missing += 1

                if did_change:
                    changed_lines += 1

                if not args.dry_run and out_f:
                    out_f.write(orjson.dumps(evt))
                    out_f.write(b"\n")

        if out_f:
            out_f.flush()
            os.fsync(out_f.fileno())
            out_f.close()
            out_f = None

        print("📊 Results")
        print(f"• lines_total        : {total}")
        print(f"• parsed_ok          : {parsed_ok}")
        print(f"• outcome_enriched    : {enriched_seen}")
        print(f"• outcome_record      : {outcome_record_seen}")
        print(f"• missing_fp_candidates: {fp_found + fp_missing}")
        print(f"• fp_found_from_setups: {fp_found}")
        print(f"• fp_missing_no_setup : {fp_missing}")
        print(f"• lines_changed       : {changed_lines}")
        print("")

        if args.dry_run:
            print("✅ DRY-RUN complete (no files modified).")
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except Exception:
                    pass
            return 0

        # Backup original, then replace atomically
        print("🧷 Writing changes safely")
        shutil.copy2(outcomes_path, bak_path)
        tmp_path.replace(outcomes_path)

        print(f"✅ Backfill applied.")
        print(f"• backup : {bak_path}")
        print(f"• updated: {outcomes_path}")
        return 0

    except Exception as e:
        if out_f:
            try:
                out_f.close()
            except Exception:
                pass
        print(f"❌ Backfill failed: {e}")
        print("Your original outcomes.jsonl was NOT replaced unless the script printed 'Backfill applied'.")
        return 2
    finally:
        # Cleanup temp if still present and we didn't replace
        if tmp_path.exists() and args.dry_run:
            try:
                tmp_path.unlink()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
