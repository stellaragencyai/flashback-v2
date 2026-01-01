#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Feature Store Builder (v2, resilient & schema-light)

Purpose
-------
Turn raw trade / feature logs into a training-ready feature store:

    Input (preferred):
        state/features_trades.jsonl

        Rows like (loosely):
            {
              "trade_id": "...",
              "ts_open_ms": 1733472000123,
              "symbol": "BTCUSDT",
              "sub_uid": "524630315",
              "strategy_name": "Sub1_Trend",
              "setup_type": "breakout" | "reversion" | ...,
              "mode": "PAPER" | "LIVE_CANARY" | "LIVE_FULL" | "UNKNOWN",
              "features": { ... }
            }

    Fallback input (if features_trades.jsonl is missing):
        state/trades_log.jsonl

        Older executor / journal style rows. We best-effort map them into
        the same normalized structure.

    Output:
        state/feature_store.jsonl

        Each row is a flattened, normalized dict:

            {
              "trade_id": "...",
              "symbol": "BTCUSDT",
              "sub_uid": "524630315",
              "account_label": "flashback01" | "main" | null,
              "strategy_name": "Sub1_Trend",
              "setup_type": "breakout" | "unknown",
              "mode": "PAPER" | "LIVE_CANARY" | "LIVE_FULL" | "UNKNOWN",

              "ts_open_ms": 1733472000123,
              "ts_open_iso": "2025-12-06T12:34:56Z",
              "dow": 2,
              "hour_utc": 14,
              "session": "EU" | "US" | "ASIA" | "OTHER",

              # direct passthrough numeric features (if present)
              "rr_est": float | null,
              "atr_pct": float | null,
              "vol_zscore": float | null,
              "adx": float | null,

              # nested features flattened under "f."
              "f.<key>": value,
              ...
            }

Notes
-----
- This is deliberately forgiving: missing fields are tolerated.
- If you change the logging schema later, this script should still not crash.
"""

from __future__ import annotations

import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

# orjson preferred, fallback to stdlib json
try:
    import orjson as _orjson  # type: ignore

    def _loads(b: bytes) -> Any:
        return _orjson.loads(b)

    def _dumps(obj: Any) -> bytes:
        return _orjson.dumps(obj)

except Exception:  # pragma: no cover
    import json as _json

    def _loads(b: bytes) -> Any:  # type: ignore
        if isinstance(b, (bytes, bytearray)):
            b = b.decode("utf-8")
        return _json.loads(b)

    def _dumps(obj: Any) -> bytes:  # type: ignore
        return _json.dumps(obj, separators=(",", ":")).encode("utf-8")

# Try central ROOT config
try:
    from app.core.config import settings
    ROOT: Path = settings.ROOT  # type: ignore
except Exception:  # pragma: no cover
    ROOT = Path(__file__).resolve().parents[2]

STATE_DIR = ROOT / "state"
FEATURES_TRADES_PATH = STATE_DIR / "features_trades.jsonl"
TRADES_LOG_PATH = STATE_DIR / "trades_log.jsonl"
FEATURE_STORE_PATH = STATE_DIR / "feature_store.jsonl"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_jsonl(path: Path, max_rows: Optional[int] = None) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    out: List[Dict[str, Any]] = []
    with path.open("rb") as f:
        for line in f:
            if max_rows is not None and len(out) >= max_rows:
                break
            line = line.strip()
            if not line:
                continue
            try:
                obj = _loads(line)
            except Exception:
                continue
            if isinstance(obj, dict):
                out.append(obj)
    return out


def _to_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return None


def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        val = float(x)
        if math.isnan(val) or math.isinf(val):
            return None
        return val
    except Exception:
        return None


def _infer_session(hour_utc: int) -> str:
    """
    Very rough session tag based on UTC hour.
    """
    if hour_utc < 0 or hour_utc > 23:
        return "OTHER"
    if 0 <= hour_utc < 7:
        return "ASIA"
    if 7 <= hour_utc < 13:
        return "EU"
    if 13 <= hour_utc < 21:
        return "US"
    return "OTHER"


def _normalize_mode(raw: Any) -> str:
    m = str(raw or "").upper().strip()
    if m in ("PAPER", "LIVE_CANARY", "LIVE_FULL"):
        return m
    return "UNKNOWN"

# ────────────────────────────────────────────────────────────────────
# Regime Tagging Helper (for regime-aware models)
# ────────────────────────────────────────────────────────────────────

def compute_regime_tag(row: dict[str, Any]) -> str:
    """
    Assign a simple regime label based on regime indicators:
      - trend: strong trend (adx >= 20)
      - high_vol: high volatility (atr_pct >= 1.0)
      - range: range / low trend (adx < 20 and atr_pct < 1.0)
      - other: fallback
    You can tune thresholds to your liking.
    """
    try:
        adx = float(row.get("adx") or 0.0)
    except Exception:
        adx = 0.0

    try:
        atr_pct = float(row.get("atr_pct") or 0.0)
    except Exception:
        atr_pct = 0.0

    try:
        vol_z = float(row.get("vol_zscore") or 0.0)
    except Exception:
        vol_z = 0.0

    # Strong trending
    if adx >= 20:
        return "trend"
    # High volatility (but not trending)
    if atr_pct >= 1.0 or abs(vol_z) >= 1.5:
        return "high_vol"
    # Likely range or subdued move
    if adx < 20 and atr_pct < 1.0:
        return "range"
    # Fallback catch-all
    return "other"

# ---------------------------------------------------------------------------
# Normalization from different sources
# ---------------------------------------------------------------------------

def _normalize_from_features_trades(row: Dict[str, Any]) -> Dict[str, Any]:
    trade_id = row.get("trade_id") or row.get("id") or row.get("tradeId")
    symbol = str(row.get("symbol") or "").upper().strip()
    sub_uid = str(row.get("sub_uid") or row.get("subUid") or row.get("subAccountId") or "") or None
    account_label = row.get("account_label") or row.get("label") or None
    strat_name = row.get("strategy_name") or row.get("strategy") or "unknown"
    setup_type = row.get("setup_type") or "unknown"
    mode = _normalize_mode(row.get("mode"))

    ts_open_ms = _to_int(row.get("ts_open_ms") or row.get("opened_at_ms") or row.get("open_ts_ms"))

    if ts_open_ms is not None:
        dt = datetime.fromtimestamp(ts_open_ms / 1000.0, tz=timezone.utc)
        ts_open_iso = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        dow = dt.weekday()
        hour_utc = dt.hour
    else:
        ts_open_iso = None
        dow = None
        hour_utc = None

    session = _infer_session(hour_utc) if hour_utc is not None else "OTHER"

    features = row.get("features") or {}
    if not isinstance(features, dict):
        features = {}

    base: Dict[str, Any] = {
        "trade_id": trade_id,
        "symbol": symbol,
        "sub_uid": sub_uid,
        "account_label": account_label,
        "strategy_name": strat_name,
        "setup_type": setup_type,
        "mode": mode,
        "ts_open_ms": ts_open_ms,
        "ts_open_iso": ts_open_iso,
        "dow": dow,
        "hour_utc": hour_utc,
        "session": session,
    }

    # Common numeric features (if present)
    base["rr_est"] = _to_float(features.get("rr_est") or features.get("rr_expected"))
    base["atr_pct"] = _to_float(features.get("atr_pct") or features.get("atr_perc"))
    base["vol_zscore"] = _to_float(features.get("vol_zscore") or features.get("volume_zscore"))
    base["adx"] = _to_float(features.get("adx"))
    
    
    

    # Flatten the rest under f.
    flat = dict(base)
    
    # ▶ Now add the regime tag
    flat["regime"] = compute_regime_tag(flat)
    
    for k, v in features.items():
        key = str(k)
        if key in ("rr_est", "rr_expected", "atr_pct", "atr_perc", "vol_zscore", "volume_zscore", "adx"):
            continue
        flat[f"f.{key}"] = v

    return flat


def _normalize_from_trades_log(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Best-effort mapping from older trades_log schema into the same shape.
    """
    trade_id = row.get("trade_id") or row.get("id") or row.get("orderLinkId")
    symbol = str(row.get("symbol") or "").upper().strip()
    sub_uid = str(row.get("sub_uid") or row.get("subAccountId") or "") or None
    account_label = row.get("account_label") or None
    strat_name = row.get("strategy_name") or row.get("strategy") or "unknown"
    setup_type = row.get("setup_type") or row.get("tag") or "unknown"
    mode = _normalize_mode(row.get("mode"))

    ts_open_ms = _to_int(
        row.get("ts_open_ms")
        or row.get("created_at_ms")
        or row.get("open_ts_ms")
        or row.get("execTimeMs")
    )

    if ts_open_ms is not None:
        dt = datetime.fromtimestamp(ts_open_ms / 1000.0, tz=timezone.utc)
        ts_open_iso = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        dow = dt.weekday()
        hour_utc = dt.hour
    else:
        ts_open_iso = None
        dow = None
        hour_utc = None

    session = _infer_session(hour_utc) if hour_utc is not None else "OTHER"

    base: Dict[str, Any] = {
        "trade_id": trade_id,
        "symbol": symbol,
        "sub_uid": sub_uid,
        "account_label": account_label,
        "strategy_name": strat_name,
        "setup_type": setup_type,
        "mode": mode,
        "ts_open_ms": ts_open_ms,
        "ts_open_iso": ts_open_iso,
        "dow": dow,
        "hour_utc": hour_utc,
        "session": session,
    }

    # These older rows might already have some numeric features on top-level
    base["rr_est"] = _to_float(row.get("rr_est") or row.get("rr_expected"))
    base["atr_pct"] = _to_float(row.get("atr_pct") or row.get("atr_perc"))
    base["vol_zscore"] = _to_float(row.get("vol_zscore") or row.get("volume_zscore"))
    base["adx"] = _to_float(row.get("adx"))

    # Anything that looks like a "feature" can be flattened with a prefix
    flat = dict(base)
    
    # Regime tag based on numeric indicators
    flat["regime"] = compute_regime_tag(flat)

    # Anything that looks like a "feature" can be flattened with a prefix
    for k, v in row.items():
        key = str(k)
        if key in flat:
            continue
        if key.startswith("f.") or key.startswith("feature_"):
            flat[f"f.{key}"] = v
    return flat


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_feature_store(max_rows: Optional[int] = None) -> None:
    """
    Public entrypoint used by app.tools.feature_store_builder.

    Steps:
      1) Load from features_trades.jsonl if present, else from trades_log.jsonl.
      2) Normalize rows into a consistent feature_store schema.
      3) Write feature_store.jsonl atomically.
      4) Print a small summary.
    """
    print(f"[feature_builder] ROOT: {ROOT}")
    print(f"[feature_builder] STATE_DIR: {STATE_DIR}")

    src_rows: List[Dict[str, Any]] = []
    source = None

    if FEATURES_TRADES_PATH.exists():
        src_rows = _load_jsonl(FEATURES_TRADES_PATH, max_rows=max_rows)
        source = "features_trades"
    elif TRADES_LOG_PATH.exists():
        src_rows = _load_jsonl(TRADES_LOG_PATH, max_rows=max_rows)
        source = "trades_log"
    else:
        print(
            "[feature_builder] No input sources found "
            f"({FEATURES_TRADES_PATH} or {TRADES_LOG_PATH}). Nothing to build."
        )
        return

    print(f"[feature_builder] Loaded {len(src_rows)} rows from {source}.")

    out_rows: List[Dict[str, Any]] = []
    symbols = Counter()
    strategies = Counter()
    modes = Counter()

    for r in src_rows:
        try:
            if source == "features_trades":
                norm = _normalize_from_features_trades(r)
            else:
                norm = _normalize_from_trades_log(r)
        except Exception as e:
            print(f"[feature_builder] ERROR normalizing row: {e}")
            continue

        out_rows.append(norm)

        sym = str(norm.get("symbol") or "UNKNOWN")
        symbols.update([sym])
        strategies.update([str(norm.get("strategy_name") or "unknown")])
        modes.update([_normalize_mode(norm.get("mode"))])

    # Write output
    FEATURE_STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = FEATURE_STORE_PATH.with_suffix(".jsonl.tmp")

    with tmp_path.open("wb") as f:
        for row in out_rows:
            f.write(_dumps(row) + b"\n")

    tmp_path.replace(FEATURE_STORE_PATH)

    print(f"[feature_builder] Wrote {len(out_rows)} rows -> {FEATURE_STORE_PATH}")

    # Tiny summary
    if out_rows:
        print("[feature_builder] Top symbols:")
        for sym, cnt in symbols.most_common(10):
            print(f"  - {sym:10s}: {cnt:6d}")

        print("[feature_builder] Top strategies:")
        for name, cnt in strategies.most_common(10):
            print(f"  - {name:20s}: {cnt:6d}")

        print("[feature_builder] Mode distribution:")
        total_modes = sum(modes.values()) or 1
        for m, cnt in modes.items():
            pct = 100.0 * cnt / total_modes
            print(f"  - {m:12s}: {cnt:6d} ({pct:5.1f}%)")
    else:
        print("[feature_builder] No rows after normalization (input was empty or invalid).")

    print("[feature_builder] Done.")


def main() -> None:
    """
    CLI wrapper for manual runs:

        python -m app.ai.feature_builder
    """
    build_feature_store()


if __name__ == "__main__":
    main()
