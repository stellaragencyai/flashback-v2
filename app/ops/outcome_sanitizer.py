#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
outcome_sanitizer.py

Sanitizes outcome JSONL rows by applying hard gates and writing:
- kept rows to outcomes.sanitized.v1.jsonl
- rejected rows + reason to outcomes.sanitized.rejects.jsonl
- stats summary to outcomes.sanitized.stats.json

This script is intentionally schema-tolerant:
It supports both legacy keys (account/setup/pnl/entry_ts/exit_ts)
and Flashback outcome v1-ish keys (account_label/setup_type/pnl_usd/ts_open_ms/ts_close_ms).
"""

from __future__ import annotations

import json
import math
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

# ---------------- PATHS ----------------
ROOT = Path(r"C:\flashback")
STATE = ROOT / "state"
AI_EVENTS_DIR = STATE / "ai_events"

INP = AI_EVENTS_DIR / "outcomes.v1.jsonl"
OUT_OK = STATE / "outcomes.sanitized.v1.jsonl"
OUT_BAD = STATE / "outcomes.sanitized.rejects.jsonl"
OUT_STATS = STATE / "outcomes.sanitized.stats.json"

# ---------------- POLICY ----------------
MIN_SAMPLE = 10
MIN_HOLD = 30  # in the same units as timestamps (usually ms)

# ---------------- JSON (optional orjson) ----------------
try:
    import orjson  # type: ignore

    def _loads(line: str) -> Any:
        return orjson.loads(line)

    def _dumps(obj: Any) -> str:
        return orjson.dumps(obj).decode("utf-8")

except Exception:
    def _loads(line: str) -> Any:
        return json.loads(line)

    def _dumps(obj: Any) -> str:
        return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


# ---------------- HELPERS ----------------
def _finite(x: Any) -> bool:
    return isinstance(x, (int, float)) and math.isfinite(float(x))


def _intish(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
        return None


def _floatish(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        v = float(x)
        if not math.isfinite(v):
            return None
        return v
    except Exception:
        return None


def _pick(o: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in o:
            return o.get(k)
    return None


def _norm_account(o: Dict[str, Any]) -> Optional[str]:
    v = _pick(o, "account_label", "account", "subaccount", "acct")
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _norm_symbol(o: Dict[str, Any]) -> Optional[str]:
    v = _pick(o, "symbol")
    if v is None:
        return None
    s = str(v).strip().upper()
    return s or None


def _norm_setup(o: Dict[str, Any]) -> Optional[str]:
    v = _pick(o, "setup_type", "setup")
    if v is None:
        return None
    s = str(v).strip().lower()
    return s or None


def _norm_side(o: Dict[str, Any]) -> Optional[str]:
    v = _pick(o, "side")
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in ("buy", "long"):
        return "buy"
    if s in ("sell", "short"):
        return "sell"
    return s or None


def _norm_entry_ts(o: Dict[str, Any]) -> Optional[int]:
    # Prefer explicit entry timestamps; otherwise accept open timestamps
    v = _pick(o, "entry_ts", "entry_ts_ms", "ts_open_ms", "opened_ms", "open_ts", "open_ts_ms")
    return _intish(v)


def _norm_exit_ts(o: Dict[str, Any]) -> Optional[int]:
    v = _pick(o, "exit_ts", "exit_ts_ms", "ts_close_ms", "closed_ms", "close_ts", "close_ts_ms")
    return _intish(v)


def _norm_pnl(o: Dict[str, Any]) -> Optional[float]:
    v = _pick(o, "pnl_usd", "pnl", "pnlUsd")
    return _floatish(v)


def _norm_expectancy(o: Dict[str, Any]) -> Optional[float]:
    v = _pick(o, "expectancy", "expected_r", "mean_r")
    return _floatish(v)


def _key_for_sample(o: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    # (account, symbol, setup)
    return (_norm_account(o), _norm_symbol(o), _norm_setup(o))


def _reject(bad_fh, counts: Counter, obj: Dict[str, Any], reason: str) -> None:
    counts[reason] += 1
    obj["reject_reason"] = reason
    bad_fh.write(_dumps(obj) + "\n")


def main() -> int:
    # Ensure dirs exist
    STATE.mkdir(parents=True, exist_ok=True)
    AI_EVENTS_DIR.mkdir(parents=True, exist_ok=True)

    if not INP.exists():
        raise SystemExit(f"❌ Missing input: {INP}")

    counts: Counter = Counter()
    stats: Counter = Counter()
    sample_map: Counter = Counter()

    # First pass: count samples per (account, symbol, setup)
    with INP.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                o = _loads(line)
            except Exception:
                counts["BAD_JSON"] += 1
                continue
            if not isinstance(o, dict):
                counts["BAD_JSON"] += 1
                continue
            sample_map[_key_for_sample(o)] += 1

    # Second pass: sanitize
    with (
        INP.open("r", encoding="utf-8") as fin,
        OUT_OK.open("w", encoding="utf-8") as ok,
        OUT_BAD.open("w", encoding="utf-8") as bad,
    ):
        for raw in fin:
            raw = raw.strip()
            if not raw:
                continue

            stats["total"] += 1
            try:
                o = _loads(raw)
            except Exception:
                counts["BAD_JSON"] += 1
                continue
            if not isinstance(o, dict):
                counts["BAD_JSON"] += 1
                continue

            account = _norm_account(o)
            symbol = _norm_symbol(o)
            setup = _norm_setup(o)
            side = _norm_side(o)
            entry_ts = _norm_entry_ts(o)
            exit_ts = _norm_exit_ts(o)
            pnl = _norm_pnl(o)

            # Required fields (logical, not literal key names)
            if not account or not symbol or not setup or not side or entry_ts is None or exit_ts is None or pnl is None:
                _reject(bad, counts, o, "MISSING_FIELD")
                continue

            if exit_ts <= entry_ts:
                _reject(bad, counts, o, "INVALID_TS")
                continue

            if not _finite(pnl):
                _reject(bad, counts, o, "BAD_NUMERIC")
                continue

            hold = exit_ts - entry_ts
            if hold < MIN_HOLD:
                _reject(bad, counts, o, "SHORT_HOLD")
                continue

            key = (account, symbol, setup)
            if sample_map[key] < MIN_SAMPLE:
                _reject(bad, counts, o, "LOW_SAMPLE")
                continue

            exp = _norm_expectancy(o)
            if exp is not None and _finite(exp) and exp < 0:
                _reject(bad, counts, o, "NEG_EXPECTANCY")
                continue

            # If we got here, keep it.
            ok.write(_dumps(o) + "\n")
            stats["kept"] += 1

    # Write stats
    out_stats_obj = {
        "input": str(INP),
        "out_ok": str(OUT_OK),
        "out_bad": str(OUT_BAD),
        "total": int(stats["total"]),
        "kept": int(stats["kept"]),
        "rejected": int(sum(counts.values())),
        "reasons": {k: int(v) for k, v in counts.items()},
        "policy": {"MIN_SAMPLE": MIN_SAMPLE, "MIN_HOLD": MIN_HOLD},
    }
    OUT_STATS.write_text(json.dumps(out_stats_obj, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"OK: Sanitized {int(stats['kept'])} / {int(stats['total'])} outcomes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
