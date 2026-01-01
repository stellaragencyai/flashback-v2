#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Flashback — Setup Memory Builder (Phase 4 canonical)

Builds:
  state/setup_memory.jsonl

From:
  state/ai_events/setups.jsonl
  state/ai_events/outcomes.jsonl

Behavior:
  - Loads JSONL safely (counts bad lines)
  - Filters to relevant ai event types
  - Builds join keys from multiple fields (best-effort, supports payload nesting)
  - Merges setup + outcome into one canonical row per key
  - Writes JSONL with stable fields
  - Reports counts, orphans, corruption

Usage:
  python -m app.tools.setup_memory_builder
  python -m app.tools.setup_memory_builder --strict
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import orjson  # type: ignore
except Exception:  # pragma: no cover
    orjson = None  # type: ignore

from app.core.config import settings


ROOT: Path = settings.ROOT
STATE_DIR: Path = ROOT / "state"
AI_EVENTS_DIR: Path = STATE_DIR / "ai_events"

SETUPS_PATH: Path = AI_EVENTS_DIR / "setups.jsonl"
OUTCOMES_PATH: Path = AI_EVENTS_DIR / "outcomes.jsonl"
OUTPUT_PATH: Path = STATE_DIR / "setup_memory.jsonl"


JOIN_FIELDS: Tuple[str, ...] = (
    "setup_id",
    "trade_id",
    "orderLinkId",
    "order_link_id",
    "client_id",
    "clientId",
)

# Expected event types (schema-aware)
SETUP_EVENT_TYPES = {"setup_context", "setup_record"}
OUTCOME_EVENT_TYPES = {"outcome_record", "outcome_record_enriched"}


@dataclass
class JsonlLoad:
    rows: List[Dict[str, Any]]
    total_lines: int
    bad_lines: int
    path: Path


def _loads(b: bytes) -> Any:
    if orjson is not None:
        return orjson.loads(b)
    import json
    return json.loads(b.decode("utf-8", errors="replace"))


def _dumps(obj: Any) -> bytes:
    if orjson is not None:
        return orjson.dumps(obj)
    import json
    return (json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8")


def _load_jsonl(path: Path) -> JsonlLoad:
    if not path.exists():
        return JsonlLoad(rows=[], total_lines=0, bad_lines=0, path=path)

    rows: List[Dict[str, Any]] = []
    total = 0
    bad = 0

    with path.open("rb") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            total += 1
            try:
                obj = _loads(line)
            except Exception:
                bad += 1
                continue
            if isinstance(obj, dict):
                rows.append(obj)

    return JsonlLoad(rows=rows, total_lines=total, bad_lines=bad, path=path)


def _first_str(d: Dict[str, Any], keys: Tuple[str, ...]) -> Optional[str]:
    for k in keys:
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _join_key(d: Dict[str, Any]) -> Optional[str]:
    # Prefer explicit join fields (top-level)
    k = _first_str(d, JOIN_FIELDS)
    if k:
        return k

    # Support payload nesting
    payload = d.get("payload")
    if isinstance(payload, dict):
        k2 = _first_str(payload, JOIN_FIELDS)
        if k2:
            return k2
        meta2 = payload.get("meta")
        if isinstance(meta2, dict):
            k3 = _first_str(meta2, JOIN_FIELDS)
            if k3:
                return k3

    # Sometimes nested metadata carries ids
    meta = d.get("meta")
    if isinstance(meta, dict):
        k4 = _first_str(meta, JOIN_FIELDS)
        if k4:
            return k4

    # outcome_enriched wraps canonical record
    out = d.get("outcome")
    if isinstance(out, dict):
        k_out = _first_str(out, JOIN_FIELDS)
        if k_out:
            return k_out

        payload_out = out.get("payload")
        if isinstance(payload_out, dict):
            k_out2 = _first_str(payload_out, JOIN_FIELDS)
            if k_out2:
                return k_out2
            meta_out = payload_out.get("meta")
            if isinstance(meta_out, dict):
                k_out3 = _first_str(meta_out, JOIN_FIELDS)
                if k_out3:
                    return k_out3

        meta_out2 = out.get("meta")
        if isinstance(meta_out2, dict):
            k_out4 = _first_str(meta_out2, JOIN_FIELDS)
            if k_out4:
                return k_out4

    # Last-resort synthetic key
    sym = d.get("symbol")
    ts = d.get("ts_ms") or d.get("ts") or d.get("timestamp_ms")
    if isinstance(sym, str) and sym and isinstance(ts, int):
        return f"SYN::{sym}::{ts}"

    return None



def _pick(d: Dict[str, Any], key: str, default: Any = None) -> Any:
    return d.get(key, default)


def _payload(d: Dict[str, Any]) -> Dict[str, Any]:
    p = d.get("payload")
    return p if isinstance(p, dict) else {}


def _merge(setup: Dict[str, Any], outcome: Dict[str, Any], key: str) -> Dict[str, Any]:
    sp = _payload(setup)
    op = _payload(outcome)

    # Setup-side common fields (best-effort)
    symbol = (
        _pick(setup, "symbol")
        or _pick(sp, "symbol")
        or _pick(sp, "market")
        or _pick(sp, "ticker")
    )
    tf = _pick(setup, "timeframe") or _pick(sp, "timeframe") or _pick(sp, "tf")
    strategy = (
        _pick(setup, "strategy")
        or _pick(setup, "strategy_name")
        or _pick(sp, "strategy")
        or _pick(sp, "strategy_name")
        or _pick(sp, "strategy_id")
    )
    side = _pick(setup, "side") or _pick(sp, "side") or _pick(sp, "signal", {}).get("side") if isinstance(_pick(sp, "signal", {}), dict) else None
    ts_ms = _pick(setup, "ts_ms") or _pick(setup, "ts") or _pick(sp, "ts_ms") or _pick(sp, "ts")

    account_label = _pick(setup, "account_label") or _pick(sp, "account_label")
    setup_type = _pick(setup, "setup_type") or _pick(sp, "setup_type")

    # Features live at payload.features in your schema
    features = _pick(setup, "features")
    if not isinstance(features, dict):
        features = _pick(sp, "features")
    if not isinstance(features, dict):
        features = None

    # Outcome-side fields
    pnl_usd = _pick(outcome, "pnl_usd")
    if pnl_usd is None:
        pnl_usd = _pick(op, "pnl_usd")

    r_multiple = _pick(outcome, "r_multiple")
    if r_multiple is None:
        r_multiple = _pick(op, "r_multiple")

    win = _pick(outcome, "win")
    if win is None:
        win = _pick(op, "win")

    exit_reason = _pick(outcome, "exit_reason")
    if exit_reason is None:
        exit_reason = _pick(op, "exit_reason")

    row: Dict[str, Any] = {
        "join_key": key,
        "trade_id": _pick(setup, "trade_id") or _pick(sp, "trade_id") or _pick(outcome, "trade_id") or _pick(op, "trade_id"),
        "setup_id": _pick(setup, "setup_id") or _pick(sp, "setup_id") or _pick(outcome, "setup_id") or _pick(op, "setup_id"),
        "orderLinkId": _pick(setup, "orderLinkId") or _pick(sp, "orderLinkId") or _pick(outcome, "orderLinkId") or _pick(op, "orderLinkId") or _pick(outcome, "order_link_id") or _pick(op, "order_link_id"),
        "ts_ms": ts_ms,
        "symbol": symbol,
        "timeframe": tf,
        "strategy": strategy,
        "account_label": account_label,
        "setup_type": setup_type,
        "side": side,
        "features": features,
        "pnl_usd": pnl_usd,
        "r_multiple": r_multiple,
        "win": win,
        "exit_reason": exit_reason,
        "raw_setup": setup,
        "raw_outcome": outcome,
    }
    return row


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(OUTPUT_PATH), help="Output path for setup_memory.jsonl")
    ap.add_argument("--strict", action="store_true", help="Fail if inputs missing or join coverage is poor")
    ap.add_argument("--max-orphan-ratio", type=float, default=0.25, help="Strict fail if orphans exceed ratio")
    ap.add_argument("--only-enriched-outcomes", action="store_true", help="Only consume outcome_record_enriched (recommended for strict)")
    args = ap.parse_args()

    setups_lr = _load_jsonl(SETUPS_PATH)
    outcomes_lr = _load_jsonl(OUTCOMES_PATH)

    print("\n🧠 SETUP MEMORY BUILDER (Phase 4)\n")
    print(f"Repo root: {ROOT}")
    print(f"Inputs:")
    print(f"  setups:   {SETUPS_PATH} (rows={len(setups_lr.rows)} bad={setups_lr.bad_lines} total={setups_lr.total_lines})")
    print(f"  outcomes: {OUTCOMES_PATH} (rows={len(outcomes_lr.rows)} bad={outcomes_lr.bad_lines} total={outcomes_lr.total_lines})")
    print(f"Output:")
    print(f"  out:      {args.out}\n")

    if args.strict:
        if not SETUPS_PATH.exists() or not OUTCOMES_PATH.exists():
            print("❌ STRICT FAIL: missing required ai_events inputs.")
            return 2

    # Filter to relevant event types (prevents random junk rows from polluting join)
    setups = [r for r in setups_lr.rows if str(r.get("event_type") or "").strip() in SETUP_EVENT_TYPES]
    outcomes_all = [r for r in outcomes_lr.rows if str(r.get("event_type") or "").strip() in OUTCOME_EVENT_TYPES]
    if args.only_enriched_outcomes:
        outcomes = [r for r in outcomes_all if str(r.get("event_type")) == "outcome_record_enriched"]
    else:
        outcomes = outcomes_all

    # Index setups by join key
    setups_by_key: Dict[str, Dict[str, Any]] = {}
    setup_missing_key = 0
    for s in setups:
        k = _join_key(s)
        if not k:
            setup_missing_key += 1
            continue
        setups_by_key.setdefault(k, s)  # first wins (stable)

    # Index outcomes by join key
    outcomes_by_key: Dict[str, Dict[str, Any]] = {}
    outcome_missing_key = 0
    for o in outcomes:
        k = _join_key(o)
        if not k:
            outcome_missing_key += 1
            continue
        outcomes_by_key.setdefault(k, o)  # first wins (stable)

    # Join keys
    keys_all = sorted(set(setups_by_key.keys()) | set(outcomes_by_key.keys()))
    merged: List[Dict[str, Any]] = []
    orphan_setups = 0
    orphan_outcomes = 0

    for k in keys_all:
        s = setups_by_key.get(k)
        o = outcomes_by_key.get(k)
        if s is None:
            orphan_outcomes += 1
            continue
        if o is None:
            orphan_setups += 1
            continue
        merged.append(_merge(s, o, k))

    total_keys = len(keys_all) or 1
    orphan_ratio = (orphan_setups + orphan_outcomes) / float(total_keys)

    print("Join stats:")
    print(f"  setup_missing_key:         {setup_missing_key}")
    print(f"  outcome_missing_key:       {outcome_missing_key}")
    print(f"  unique_setup_keys:         {len(setups_by_key)}")
    print(f"  unique_outcome_keys:       {len(outcomes_by_key)}")
    print(f"  merged_rows:               {len(merged)}")
    print(f"  orphan_setups:             {orphan_setups}")
    print(f"  orphan_outcomes:           {orphan_outcomes}")
    print(f"  orphan_ratio:              {orphan_ratio:.3f}")
    print(f"  only_enriched_outcomes:    {bool(args.only_enriched_outcomes)}\n")

    if args.strict and len(keys_all) > 0 and orphan_ratio > float(args.max_orphan_ratio):
        print(f"❌ STRICT FAIL: orphan_ratio {orphan_ratio:.3f} > {args.max_orphan_ratio:.3f}")
        return 2

    # Write output
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("wb") as f:
        for row in merged:
            b = _dumps(row)
            if not b.endswith(b"\n"):
                b += b"\n"
            f.write(b)

    print(f"✅ Wrote {len(merged)} rows → {out_path}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
