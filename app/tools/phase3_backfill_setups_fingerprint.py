#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Phase 3 — Backfill setup_fingerprint in setups.jsonl

Why this exists
---------------
Older setup_context events may lack payload.features.setup_fingerprint and/or have
timeframe stored inconsistently.

This tool:
- Reads state/ai_events/setups.jsonl
- Detects "setup_context" records across a few shapes:
    A) root: {"event_type":"setup_context", "payload":{"features":{...}}}
    B) root: {"type":"setup_context", ...}
    C) nested legacy: {"payload":{"event_type":"setup_context", ...}}
- Ensures payload.features.setup_fingerprint exists (deterministic hash)
- Normalizes timeframe to "<Nm>" if it can (best effort)
- Writes to state/ai_events/setups.backfill.jsonl

It does NOT overwrite your original file.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Dict, Tuple

import orjson


ROOT = Path(__file__).resolve().parents[2]
IN_PATH = ROOT / "state" / "ai_events" / "setups.jsonl"
OUT_PATH = ROOT / "state" / "ai_events" / "setups.backfill.jsonl"


def _is_setup_event(d: Dict[str, Any]) -> bool:
    et = d.get("event_type") or d.get("type")
    if et == "setup_context":
        return True
    # legacy nested
    p = d.get("payload")
    if isinstance(p, dict) and (p.get("event_type") or p.get("type")) == "setup_context":
        return True
    return False


def _get_payload(d: Dict[str, Any]) -> Dict[str, Any]:
    p = d.get("payload")
    return p if isinstance(p, dict) else {}


def _get_features(payload: Dict[str, Any]) -> Dict[str, Any]:
    f = payload.get("features")
    return f if isinstance(f, dict) else {}


def _normalize_timeframe(tf: Any) -> str | None:
    if tf is None:
        return None
    s = str(tf).strip().lower()
    # common inputs: "5", "5m", "15", "15m", "1h"
    if s.isdigit():
        return f"{s}m"
    if s.endswith("m") and s[:-1].isdigit():
        return f"{int(s[:-1])}m"
    if s.endswith("h") and s[:-1].isdigit():
        return f"{int(s[:-1])}h"
    return None


def _stable_fingerprint(trade_id: str | None, symbol: str | None, tf: str | None, features: Dict[str, Any]) -> str:
    """
    Deterministic hash. We include:
    - symbol
    - timeframe
    - a stable, sorted JSON of features (minus obviously volatile keys)
    """
    feats = dict(features or {})
    # remove noisy keys if present
    for k in ("positions_snapshot", "ts_ms", "timestamp", "now_ms"):
        feats.pop(k, None)

    base = {
        "symbol": symbol or "",
        "timeframe": tf or "",
        "trade_id_hint": trade_id or "",
        "features": feats,
    }
    b = orjson.dumps(base, option=orjson.OPT_SORT_KEYS)
    return hashlib.sha256(b).hexdigest()


def _backfill_one(d: Dict[str, Any]) -> Tuple[Dict[str, Any], bool, bool]:
    """
    Returns: (updated_dict, is_setup_event, changed)
    """
    if not _is_setup_event(d):
        return d, False, False

    changed = False
    payload = _get_payload(d)
    features = _get_features(payload)

    # timeframe can be root timeframe or payload.extra.timeframe
    tf_root = d.get("timeframe")
    extra = payload.get("extra") if isinstance(payload.get("extra"), dict) else {}
    tf_extra = extra.get("timeframe") if isinstance(extra, dict) else None

    tf_norm = _normalize_timeframe(tf_root) or _normalize_timeframe(tf_extra)
    if tf_norm:
        # prefer storing normalized string in root timeframe
        if d.get("timeframe") != tf_norm:
            d["timeframe"] = tf_norm
            changed = True
        # also store in payload.extra.timeframe for compatibility
        if isinstance(extra, dict):
            if extra.get("timeframe") != tf_norm:
                extra["timeframe"] = tf_norm
                payload["extra"] = extra
                changed = True

    # fingerprint
    if not features.get("setup_fingerprint"):
        fp = _stable_fingerprint(
            trade_id=str(d.get("trade_id") or ""),
            symbol=str(d.get("symbol") or ""),
            tf=str(d.get("timeframe") or tf_norm or ""),
            features=features,
        )
        features["setup_fingerprint"] = fp
        payload["features"] = features
        d["payload"] = payload
        changed = True

    return d, True, changed


def main() -> int:
    if not IN_PATH.exists():
        print(f"Missing input: {IN_PATH}")
        return 2

    lines = IN_PATH.read_bytes().splitlines()
    total = 0
    setup_count = 0
    changed_count = 0
    unchanged_count = 0

    out_lines = []
    for b in lines:
        total += 1
        try:
            d = orjson.loads(b)
        except Exception:
            out_lines.append(b)
            continue

        if not isinstance(d, dict):
            out_lines.append(b)
            continue

        d2, is_setup, changed = _backfill_one(d)
        if is_setup:
            setup_count += 1
            if changed:
                changed_count += 1
            else:
                unchanged_count += 1

        out_lines.append(orjson.dumps(d2))

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_bytes(b"\n".join(out_lines) + (b"\n" if out_lines else b""))

    print(f"input={IN_PATH}")
    print(f"output={OUT_PATH}")
    print(f"total_lines={total}")
    print(f"setup_events={setup_count}")
    print(f"changed={changed_count}")
    print(f"unchanged={unchanged_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
