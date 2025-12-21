#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Backfill missing setup_fingerprint in state/ai_events/outcomes.jsonl

- Only touches rows missing setup_fingerprint.
- Computes a deterministic synthetic setup_fingerprint based on:
  trade_id, symbol, account_label, strategy, timeframe
- Marks synthetic fingerprints:
  payload.extra.synthetic_setup_fingerprint = True
  payload.extra.synthetic_reason = "backfill_missing_setup_fingerprint"

Creates:
- Backup: outcomes.jsonl.bak_YYYYMMDD_HHMMSS
- Output is rewritten atomically.
"""

from __future__ import annotations

import time
import hashlib
import json
from pathlib import Path
from typing import Any, Dict

try:
    from app.core.config import settings  # type: ignore
    ROOT = Path(settings.ROOT)  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

OUTCOMES = ROOT / "state" / "ai_events" / "outcomes.jsonl"

def _now_tag() -> str:
    return time.strftime("%Y%m%d_%H%M%S")

def _stable_json(obj: Any) -> str:
    try:
        return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        return str(obj)

def _normalize_timeframe(tf: Any) -> str:
    if tf is None:
        return "unknown"
    try:
        s = str(tf).strip().lower()
    except Exception:
        return "unknown"
    if not s:
        return "unknown"
    if s.endswith(("m","h","d","w")):
        return s
    try:
        n = int(float(s))
        if n > 0:
            return f"{n}m"
    except Exception:
        pass
    return "unknown"

def _compute_setup_fingerprint(*, trade_id: str, symbol: str, account_label: str, strategy: str, setup_type: Any, timeframe: str) -> str:
    core = {
        "trade_id": str(trade_id),
        "symbol": str(symbol).upper(),
        "account_label": str(account_label),
        "strategy": str(strategy),
        "setup_type": (str(setup_type) if setup_type is not None else None),
        "timeframe": timeframe,
        "features": {},  # synthetic backfill: do not invent features
    }
    h = hashlib.sha256()
    h.update(_stable_json(core).encode("utf-8", errors="ignore"))
    return h.hexdigest()

def main() -> int:
    if not OUTCOMES.exists():
        print("BACKFILL_FAIL: missing file:", OUTCOMES)
        return 2

    bak = OUTCOMES.with_suffix(".jsonl.bak_" + _now_tag())
    bak.write_bytes(OUTCOMES.read_bytes())
    print("BACKUP_OK:", bak)

    lines_out = []
    changed = 0
    missing_before = 0

    raw = OUTCOMES.read_bytes().splitlines()
    for i, line in enumerate(raw, start=1):
        s = line.strip()
        if not s:
            continue
        if s[:1] != b"{":
            lines_out.append(line)
            continue
        try:
            obj = json.loads(s.decode("utf-8", errors="ignore"))
        except Exception:
            lines_out.append(line)
            continue
        if not isinstance(obj, dict):
            lines_out.append(line)
            continue

        # detect missing fp (supports outcome_record and outcome_enriched)
        if not obj.get("setup_fingerprint"):
            missing_before += 1

            trade_id = str(obj.get("trade_id") or "").strip()
            symbol = str(obj.get("symbol") or "").strip()
            account_label = str(obj.get("account_label") or "main").strip()
            strategy = str(obj.get("strategy") or "unknown").strip()
            setup_type = obj.get("setup_type")

            tf = _normalize_timeframe(obj.get("timeframe"))
            payload = obj.get("payload") if isinstance(obj.get("payload"), dict) else {}
            extra = payload.get("extra") if isinstance(payload.get("extra"), dict) else {}
            if tf == "unknown" and isinstance(extra, dict):
                tf = _normalize_timeframe(extra.get("timeframe"))

            fp = _compute_setup_fingerprint(
                trade_id=trade_id,
                symbol=symbol,
                account_label=account_label,
                strategy=strategy,
                setup_type=setup_type,
                timeframe=tf,
            )

            obj["setup_fingerprint"] = fp

            if isinstance(payload, dict):
                payload["setup_fingerprint"] = fp
                if isinstance(extra, dict):
                    extra["synthetic_setup_fingerprint"] = True
                    extra["synthetic_reason"] = "backfill_missing_setup_fingerprint"
                    extra["setup_fingerprint"] = fp
                    payload["extra"] = extra
                obj["payload"] = payload

            changed += 1

        lines_out.append(json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))

    OUTCOMES.write_bytes(b"\n".join(lines_out) + b"\n")
    print("BACKFILL_DONE")
    print("missing_fp_before=", missing_before)
    print("rows_changed=", changed)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
