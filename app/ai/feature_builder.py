#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Feature Builder — trades + outcomes → unified feature_store.jsonl

Reads:
- state/features_trades.jsonl
- lane-aware outcome files under state/ai_events*/...

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
from typing import Any, Dict, Iterator, List, Optional, Tuple

from app.data.append_store import append_rows, load_progress, save_progress
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
FEATURES_TRADES = STATE / "features_trades.jsonl"
OUT = STATE / "feature_store.jsonl"

# Ensure dirs exist (avoids weird partial failures)
STATE.mkdir(parents=True, exist_ok=True)


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


def iter_jsonl(path: Path, start_offset: int = 0) -> Tuple[Iterator[Dict[str, Any]], Dict[str, int]]:
    if not path.exists():
        return iter(()), {"offset": 0}

    try:
        file_size = int(path.stat().st_size)
    except Exception:
        file_size = 0

    offset = max(0, int(start_offset or 0))
    if offset > file_size:
        offset = 0

    state = {"offset": offset}

    def _iter() -> Iterator[Dict[str, Any]]:
        try:
            with path.open("rb") as f:
                if state["offset"] > 0:
                    f.seek(state["offset"])
                for line in f:
                    state["offset"] = int(f.tell())
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = loads(line)
                    except Exception:
                        continue
                    if isinstance(obj, dict):
                        yield obj
        except Exception:
            return

    return _iter(), state


def _candidate_outcome_paths() -> List[Path]:
    out: List[Path] = []
    seen: set[str] = set()
    patterns = [
        "ai_events/outcomes.enriched.backfill.jsonl",
        "ai_events/outcomes.v1.jsonl",
        "ai_events/*/outcomes.v1.jsonl",
        "ai_events_*/outcomes.v1.jsonl",
        "ai_decision_outcomes.v1.jsonl",
    ]
    for pattern in patterns:
        try:
            matches = STATE.glob(pattern)
        except Exception:
            matches = []
        for path in matches:
            try:
                if not path.is_file():
                    continue
            except Exception:
                continue
            key = str(path).lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(path)
    return sorted(out, key=lambda p: str(p).lower())


def _source_offsets(progress: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    raw = progress.get("source_offsets")
    return dict(raw) if isinstance(raw, dict) else {}


def _offset_for_path(progress: Dict[str, Any], path: Path) -> int:
    key = str(path.resolve())
    slot = _source_offsets(progress).get(key)
    if not isinstance(slot, dict):
        return 0
    try:
        return int(slot.get("offset") or 0)
    except Exception:
        return 0


def _remember_offset(progress: Dict[str, Any], path: Path, offset: int) -> None:
    key = str(path.resolve())
    bucket = _source_offsets(progress)
    bucket[key] = {
        "offset": int(max(0, offset)),
        "updated_ms": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
    }
    progress["source_offsets"] = bucket


def _outcome_identity(evt: Dict[str, Any]) -> str:
    outcome_id = str(evt.get("outcome_id") or "").strip()
    if outcome_id:
        return f"oid:{outcome_id}"
    trade_id = str(evt.get("trade_id") or "").strip()
    ts = evt.get("closed_ts_ms") or evt.get("ts_ms") or evt.get("ts") or ""
    reason = evt.get("close_reason") or evt.get("exit_reason") or ""
    return f"tid:{trade_id}|ts:{ts}|reason:{reason}"


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
        if str(evt.get("schema_version") or "").strip() == "outcome.v1" or str(evt.get("event_type") or "").strip() == "trade_outcome":
            ts = fint(evt.get("opened_ts_ms"))
            dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc) if ts else None
            pnl = ffloat(evt.get("pnl_usd"))
            win = _coerce_bool(evt.get("win"))
            if win is None and pnl is not None:
                win = pnl > 0
            row: Dict[str, Any] = {
                "trade_id": evt.get("trade_id"),
                "symbol": evt.get("symbol"),
                "strategy_name": evt.get("strategy_name") or evt.get("strategy"),
                "account_label": evt.get("account_label"),
                "mode": evt.get("mode"),
                "ts_open_ms": ts,
                "ts_open_iso": dt.isoformat() if dt else None,
                "dow": dt.weekday() if dt else None,
                "hour_utc": dt.hour if dt else None,
                "session": session(dt.hour if dt else None),
                "entry_price": ffloat(evt.get("entry_px")),
                "exit_price": ffloat(evt.get("exit_px")),
                "pnl_usd": pnl,
                "r_multiple": ffloat(evt.get("r_multiple")),
                "win": win,
                "atr_pct": ffloat(evt.get("atr_pct")),
                "vol_zscore": ffloat(evt.get("vol_zscore") or evt.get("volume_zscore")),
                "adx": ffloat(evt.get("adx")),
            }
            row["regime"] = regime(row)
            return row

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
    trade_iter, trade_state = iter_jsonl(FEATURES_TRADES, _offset_for_path(progress, FEATURES_TRADES))
    for r in trade_iter:
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
    _remember_offset(progress, FEATURES_TRADES, int(trade_state.get("offset", 0)))

    # Outcomes
    seen_outcomes: set[str] = set()
    for outcome_path in _candidate_outcome_paths():
        outcome_iter, outcome_state = iter_jsonl(outcome_path, _offset_for_path(progress, outcome_path))
        for e in outcome_iter:
            ident = _outcome_identity(e)
            if ident in seen_outcomes:
                continue
            seen_outcomes.add(ident)
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
        _remember_offset(progress, outcome_path, int(outcome_state.get("offset", 0)))

    if not rows:
        progress["last_ts"] = max_ts
        save_progress(progress)
        print(f"[feature_builder] appended=0 last_ts={max_ts} (no new rows)")
        return

    enforce_schema(rows)
    append_rows(OUT, rows, max_ts)
    progress["last_ts"] = max_ts
    save_progress(progress)
    print(f"[feature_builder] appended={len(rows)} last_ts={max_ts}")


if __name__ == "__main__":
    main()
