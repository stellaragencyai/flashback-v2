#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 3 Backfill: add payload.features.setup_fingerprint to setup_context records.

- Reads:  state/ai_events/setups.jsonl
- Writes: state/ai_events/setups.backfill.jsonl
- --swap will backup and replace setups.jsonl
"""

from __future__ import annotations

import argparse
import hashlib
import time
from pathlib import Path
from typing import Any, Dict

import orjson


def _root() -> Path:
    return Path(__file__).resolve().parents[2]


def _ensure_features(evt: Dict[str, Any]) -> Dict[str, Any]:
    payload = evt.get("payload")
    if not isinstance(payload, dict):
        payload = {}
        evt["payload"] = payload
    feats = payload.get("features")
    if not isinstance(feats, dict):
        feats = {}
        payload["features"] = feats
    return feats


def _fallback_fp(evt: Dict[str, Any]) -> str:
    # Deterministic + stable. Not fancy. It works.
    strategy = (evt.get("strategy_name") or evt.get("strategy") or "unknown_strategy").strip()
    symbol = (evt.get("symbol") or "UNKNOWN").strip()
    tf = (evt.get("timeframe") or "UNKNOWN").strip()

    feats = _ensure_features(evt)
    # If these exist, include them. If not, fine.
    atr_pct = feats.get("atr_pct")
    volz = feats.get("volume_z") or feats.get("volume_zscore") or feats.get("volume_zscore")

    raw = f"{strategy}|{symbol}|{tf}|ATR{atr_pct}|VOLZ{volz}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--swap", action="store_true")
    args = ap.parse_args()

    root = _root()
    ai_dir = root / "state" / "ai_events"
    setups_path = ai_dir / "setups.jsonl"
    out_path = ai_dir / "setups.backfill.jsonl"

    if not setups_path.exists():
        raise SystemExit(f"Missing {setups_path}")

    total = added = kept = 0

    with setups_path.open("rb") as fin, out_path.open("wb") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                evt = orjson.loads(line)
            except Exception:
                continue
            if not isinstance(evt, dict):
                continue

            total += 1

            if evt.get("event_type") == "setup_context":
                feats = _ensure_features(evt)
                fp = feats.get("setup_fingerprint")
                if not isinstance(fp, str) or not fp.strip():
                    feats["setup_fingerprint"] = _fallback_fp(evt)
                    added += 1
                else:
                    kept += 1

            fout.write(orjson.dumps(evt))
            fout.write(b"\n")

    print(f"total={total} added_fp={added} already_had_fp={kept}")
    print(f"output={out_path}")

    if args.swap:
        ts = time.strftime("%Y%m%d_%H%M%S")
        backup = ai_dir / f"setups.backup_{ts}.jsonl"
        setups_path.replace(backup)
        out_path.replace(setups_path)
        print(f"SWAP OK: backup={backup} new_setups={setups_path}")


if __name__ == "__main__":
    main()
