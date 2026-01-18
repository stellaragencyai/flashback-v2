#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Feature Builder — trades + outcomes → unified feature_store.jsonl

Reads:
- state/features_trades.jsonl
- state/ai_events/outcomes.enriched.backfill.jsonl

Writes:
- state/feature_store.jsonl (append-only; progress tracked via app.data.append_store)

Notes:
- Deterministic normalization (best effort)
- Schema enforcement happens on the outgoing rows
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.data.append_store import append_rows, load_progress
from app.data.feature_registry import enforce_schema

# ---------------- JSON ----------------
try:
    import orjson  # type: ignore

    def loads(b: Any) -> Any:
        return orjson.loads(b)

    def dumps(o: Any) -> bytes:
        return orjson.dumps(o)

except Exception:
    import json

    def loads(b: Any) -> Any:
        if isinstance(b, (bytes, bytearray)):
            b = b.decode("utf-8", errors="replace")
        return json.loads(b)

    def dumps(o: Any) -> bytes:
        return json.dumps(o, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


# ---------------- PATHS ----------------
ROOT = Path(__file__).resolve().parents[2]
STATE = ROOT / "state"
AI_EVENTS_DIR = STATE / "ai_events"

FEATURES_TRADES = STATE / "features_trades.jsonl"
OUTCOMES = AI_EVENTS_DIR / "outcomes.enriched.backfill.jsonl"
OUT = STATE / "feature_store.jsonl"

# Ensure dirs exist (avoids weird partial failures)
STATE.mkdir(parents=True, exist_ok=True)
AI_EVENTS_DIR.mkdir(parents=True, exist_ok=True)


# ---------------- HELPERS ----------------
def ffloat(x: Any) -> Optional[float]:
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None


def fint(x: Any) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        return None


def session(hour: Optional[int]) -> str:
    if hour is None:
        return "OTHER"
    if hour < 7:
        return "ASIA"
    if hour < 13:
        return "EU"
    if hour < 21:
        return "US"
    return "OTHER"


def regime(row: Dict[str, Any]) -> str:
    adx = ffloat(row.get("adx")) or 0.0
    atr = ffloat(row.get("atr_pct")) or 0.0
    vz = ffloat(row.get("vol_zscore")) or 0.0

    if adx >= 20:
        return "trend"
    if atr >= 1.0 or abs(vz) >= 1.5:
        return "high_vol"
    if adx < 20 and atr < 1.0:
        return "range"
    return "other"


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    out: List[Dict[str, Any]] = []
    try:
        with path.open("rb") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = loads(line)
                    if isinstance(obj, dict):
                        out.append(obj)
                except Exception:
                    continue
    except Exception:
        return []
    return out


def _coerce_bool(x: Any) -> Optional[bool]:
    if x is None:
        return None
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)):
        return bool(int(x))
    if isinstance(x, str):
        s = x.strip().lower()
        if s in ("true", "t", "1", "yes", "y", "win"):
            return True
        if s in ("false", "f", "0", "no", "n", "loss"):
            return False
    return None


# ---------------- NORMALIZERS ----------------
def normalize_feature_trade(r: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        ts = fint(r.get("ts_open_ms"))
        dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc) if ts else None
        feat = r.get("features") or {}
        if not isinstance(feat, dict):
            feat = {}

        row: Dict[str, Any] = {
            "trade_id": r.get("trade_id"),
            "symbol": r.get("symbol"),
            "strategy_name": r.get("strategy_name"),
            "account_label": r.get("account_label"),
            "mode": r.get("mode"),
            "ts_open_ms": ts,
            "ts_open_iso": dt.isoformat() if dt else None,
            "dow": dt.weekday() if dt else None,
            "hour_utc": dt.hour if dt else None,
            "session": session(dt.hour if dt else None),
            "atr_pct": ffloat(feat.get("atr_pct")),
            "vol_zscore": ffloat(feat.get("volume_zscore")),
            "adx": ffloat(feat.get("adx")),
        }

        row["regime"] = regime(row)

        # Copy remaining features into prefixed namespace
        for k, v in feat.items():
            if k not in ("atr_pct", "volume_zscore", "adx"):
                row[f"f.{k}"] = v

        return row
    except Exception:
        return None


def normalize_outcome(evt: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        if evt.get("event_type") != "outcome_enriched":
            return None

        outcome = evt.get("outcome") if isinstance(evt.get("outcome"), dict) else {}
        op = outcome.get("payload") if isinstance(outcome.get("payload"), dict) else {}
        extra = op.get("extra") if isinstance(op.get("extra"), dict) else {}

        ts = fint(extra.get("opened_ms"))
        dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc) if ts else None

        # Try to pull regime fields if they exist in extra (otherwise regime() falls back)
        atr_pct = ffloat(extra.get("atr_pct"))
        vol_z = ffloat(extra.get("vol_zscore"))
        if vol_z is None:
            vol_z = ffloat(extra.get("volume_zscore"))
        adx = ffloat(extra.get("adx"))

        row: Dict[str, Any] = {
            "trade_id": evt.get("trade_id"),
            "symbol": evt.get("symbol"),
            "strategy_name": evt.get("strategy"),
            "account_label": evt.get("account_label"),
            "mode": extra.get("mode"),
            "ts_open_ms": ts,
            "ts_open_iso": dt.isoformat() if dt else None,
            "dow": dt.weekday() if dt else None,
            "hour_utc": dt.hour if dt else None,
            "session": session(dt.hour if dt else None),
            "entry_price": ffloat(extra.get("entry_price")),
            "exit_price": ffloat(extra.get("exit_price")),
            "pnl_usd": ffloat(op.get("pnl_usd")),
            "r_multiple": ffloat(op.get("r_multiple")),
            "win": _coerce_bool(op.get("win")),
            "atr_pct": atr_pct,
            "vol_zscore": vol_z,
            "adx": adx,
        }

        row["regime"] = regime(row)
        return row
    except Exception:
        return None


# ---------------- BUILD ----------------
def main() -> None:
    progress = load_progress() or {}
    last_ts = progress.get("last_ts")
    last_ts_i = fint(last_ts) or 0

    rows: List[Dict[str, Any]] = []
    max_ts = last_ts_i

    # Feature trades
    for r in load_jsonl(FEATURES_TRADES):
        o = normalize_feature_trade(r)
        if not o:
            continue
        ts = o.get("ts_open_ms")
        ts_i = fint(ts)
        if ts_i is None:
            continue
        if ts_i > last_ts_i:
            rows.append(o)
            if ts_i > max_ts:
                max_ts = ts_i

    # Outcomes
    for e in load_jsonl(OUTCOMES):
        o = normalize_outcome(e)
        if not o:
            continue
        ts = o.get("ts_open_ms")
        ts_i = fint(ts)
        if ts_i is None:
            continue
        if ts_i > last_ts_i:
            rows.append(o)
            if ts_i > max_ts:
                max_ts = ts_i

    if not rows:
        print(f"[feature_builder] appended=0 last_ts={max_ts} (no new rows)")
        return

    enforce_schema(rows)
    append_rows(OUT, rows, max_ts)
    print(f"[feature_builder] appended={len(rows)} last_ts={max_ts}")


if __name__ == "__main__":
    main()
