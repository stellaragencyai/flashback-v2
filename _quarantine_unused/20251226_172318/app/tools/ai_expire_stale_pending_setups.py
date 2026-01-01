#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Expire stale pending setups by emitting outcome_record(pnl_usd=0) with exit_reason=SETUP_EXPIRED.

Why this exists
---------------
Your Phase-3 integrity tool FAILs if setups never receive outcomes and they aren't
tracked as pending within TTL.

This reconciler:
- Loads state/ai_events/pending_setups.json
- For each pending setup older than TTL:
    emits outcome_record with pnl_usd=0 and exit_reason=SETUP_EXPIRED
- Uses ai_events_spine.publish_ai_event(...) so the merge happens correctly:
    setup + outcome_record => outcome_enriched
    and pending_setups.json is updated (trade_id removed)

IMPORTANT
---------
When running scripts from app/tools/, Python does NOT automatically include the
project root on sys.path. We add ROOT explicitly so `import app.*` works.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, Tuple


ROOT = Path(__file__).resolve().parents[2]
# Ensure project root is on import path (fixes "No module named 'app.ai'")
sys.path.insert(0, str(ROOT))


try:
    from app.ai.ai_events_spine import build_outcome_record, publish_ai_event  # type: ignore
except Exception as e:
    raise RuntimeError(f"ai_events_spine import failed: {e}")


PENDING_PATH = ROOT / "state" / "ai_events" / "pending_setups.json"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_str(x: Any) -> str:
    try:
        return ("" if x is None else str(x)).strip()
    except Exception:
        return ""


def _load_pending() -> Dict[str, Any]:
    if not PENDING_PATH.exists():
        return {}
    try:
        obj = json.loads(PENDING_PATH.read_text(encoding="utf-8") or "{}")
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _extract_setup_fields(setup: Dict[str, Any]) -> Tuple[str, str, str, str]:
    trade_id = _safe_str(setup.get("trade_id"))
    symbol = _safe_str(setup.get("symbol")).upper() or "UNKNOWN"
    strategy = _safe_str(setup.get("strategy") or setup.get("strategy_name")) or "unknown_strategy"
    account_label = _safe_str(setup.get("account_label")) or "main"
    return trade_id, symbol, strategy, account_label


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ttl-min", type=int, default=20, help="Expire pending older than this many minutes")
    ap.add_argument("--max", type=int, default=5000, help="Safety cap on how many expirations per run")
    args = ap.parse_args()

    ttl_ms = int(args.ttl_min) * 60_000
    now = _now_ms()

    pending = _load_pending()
    print("=== expire_stale_pending_setups ===")
    print("root        :", ROOT)
    print("pending_path:", PENDING_PATH)
    print("pending_entries:", len(pending))
    print("ttl_min:", args.ttl_min)

    stale = []
    for tid, setup in pending.items():
        if not isinstance(setup, dict):
            continue
        ts = setup.get("ts") or setup.get("ts_ms")
        try:
            ts_i = int(ts) if ts is not None else 0
        except Exception:
            ts_i = 0
        if ts_i and (now - ts_i) > ttl_ms:
            stale.append((tid, setup, ts_i))

    stale.sort(key=lambda x: x[2])  # oldest first
    print("stale_found:", len(stale))
    if stale:
        print("sample_stale:", [s[0] for s in stale[:10]])

    wrote = 0
    for _tid, setup, _ts in stale:
        if wrote >= int(args.max):
            break

        trade_id, symbol, strategy, account_label = _extract_setup_fields(setup)
        if not trade_id:
            continue

        evt = build_outcome_record(
            trade_id=trade_id,
            symbol=symbol,
            account_label=account_label,
            strategy=strategy,
            pnl_usd=0.0,
            r_multiple=None,
            win=None,
            exit_reason="SETUP_EXPIRED",
            extra={
                "schema_version": "expire_pending_v1",
                "lifecycle_stage": "SETUP_EXPIRED",
                "lifecycle_role": "RECONCILER",
                "is_final": True,
                "final_authority_expected": "reconciler",
                "expired_after_min": int(args.ttl_min),
            },
        )
        publish_ai_event(evt)
        wrote += 1

    print("expired_emitted:", wrote)
    print("DONE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
