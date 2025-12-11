#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Setup Outcomes Builder (trade_id spine)

Purpose
-------
Bridge between raw AI events and training-ready setup memory:

    INPUTS:
      - state/ai_events/setups.jsonl   (SetupContext events)
      - state/ai_events/outcomes.jsonl (OutcomeRecord events)

    OUTPUT:
      - state/setup_outcomes.jsonl

Each output row represents ONE trade_id and is the merged view:

  {
    "trade_id": "...",
    "symbol": "BTCUSDT",
    "account_label": "main",
    "strategy_name": "Sub1_Trend",
    "side": "LONG" | "SHORT" | "UNKNOWN",

    "opened_at_ms": ...,
    "closed_at_ms": ...,
    "risk_pct": float | null,
    "mode": "PAPER" | "LIVE_CANARY" | "LIVE_FULL" | "UNKNOWN",

    "features": {...},        # from SetupContext.features
    "sub_uid": "...",         # if provided
    "timeframe": "15m" | "...",

    "journal": {
      "result": "WIN" | "LOSS" | "BREAKEVEN" | "UNKNOWN",
      "realized_rr": float | null,
      "realized_pnl": float | null,
      "rating_score": int | null,
      "rating_reason": str | null,
      "duration_ms": int | null,
      "duration_human": str | null
    }
  }

Notes
-----
- This script is *purely* a join / aggregation step.
- setup_memory.py consumes state/setup_outcomes.jsonl and adds final labels.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import orjson

try:
    from app.core.config import settings  # type: ignore
    from app.core.logger import get_logger  # type: ignore
except Exception:  # pragma: no cover
    # Fallbacks for ad-hoc runs
    class _DummySettings:  # type: ignore
        ROOT = Path(__file__).resolve().parents[2]

    settings = _DummySettings()  # type: ignore

    import logging

    def get_logger(name: str):  # type: ignore
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name)


ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])
STATE_DIR: Path = ROOT / "state"
AI_EVENTS_DIR: Path = STATE_DIR / "ai_events"

STATE_DIR.mkdir(parents=True, exist_ok=True)
AI_EVENTS_DIR.mkdir(parents=True, exist_ok=True)

SETUPS_PATH: Path = AI_EVENTS_DIR / "setups.jsonl"
OUTCOMES_PATH: Path = AI_EVENTS_DIR / "outcomes.jsonl"
OUTPUT_PATH: Path = STATE_DIR / "setup_outcomes.jsonl"

log = get_logger("setup_outcomes_builder")


# ----------------- helpers -----------------


def _load_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        log.warning("[setup_outcomes] %s does not exist; treating as empty.", path)
        return []
    with path.open("rb") as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            try:
                row = orjson.loads(raw)
            except Exception:
                continue
            if isinstance(row, dict):
                yield row


def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def _to_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        return None


def _fmt_duration(ms: Optional[int]) -> Optional[str]:
    if ms is None or ms < 0:
        return None
    seconds = ms / 1000.0
    minutes = int(seconds // 60)
    hours = int(minutes // 60)
    minutes = minutes % 60
    if hours > 0:
        return f"{hours}h{minutes}m"
    return f"{minutes}m"


def _infer_side_from_features(features: Dict[str, Any]) -> str:
    sig = features.get("signal") or {}
    if not isinstance(sig, dict):
        return "UNKNOWN"
    side_raw = (sig.get("side") or sig.get("positionSide") or "").upper()
    if side_raw in ("BUY", "LONG"):
        return "LONG"
    if side_raw in ("SELL", "SHORT"):
        return "SHORT"
    return "UNKNOWN"


# ----------------- indexing -----------------


def _index_setups(rows: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for ev in rows:
        if ev.get("event_type") != "setup_context":
            continue
        tid = str(ev.get("trade_id") or "").strip()
        if not tid:
            continue
        # last event wins if duplicates
        out[tid] = ev
    return out


def _group_outcomes(rows: Iterable[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    for ev in rows:
        if ev.get("event_type") != "outcome_record":
            continue
        tid = str(ev.get("trade_id") or "").strip()
        if not tid:
            continue
        out.setdefault(tid, []).append(ev)
    return out


# ----------------- core transform -----------------


def _build_outcome_row(
    trade_id: str,
    setup_ev: Dict[str, Any],
    outcome_evs: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if not outcome_evs:
        return None

    symbol = str(setup_ev.get("symbol") or "").upper()
    account_label = str(setup_ev.get("account_label") or "main")
    strategy_name = str(setup_ev.get("strategy") or "unknown")

    payload = setup_ev.get("payload") or {}
    if not isinstance(payload, dict):
        payload = {}

    features = payload.get("features") or {}
    if not isinstance(features, dict):
        features = {}

    extra = payload.get("extra") or {}
    if not isinstance(extra, dict):
        extra = {}

    # Times
    ts_open_ms = _to_int(setup_ev.get("ts"))
    ts_close_ms_candidates = [_to_int(ev.get("ts")) for ev in outcome_evs]
    ts_close_ms_candidates = [x for x in ts_close_ms_candidates if x is not None]

    ts_close_ms: Optional[int] = max(ts_close_ms_candidates) if ts_close_ms_candidates else None
    duration_ms: Optional[int] = None
    if ts_open_ms is not None and ts_close_ms is not None and ts_close_ms >= ts_open_ms:
        duration_ms = ts_close_ms - ts_open_ms

    # Risk / RR
    risk_pct = _to_float(features.get("risk_pct"))
    risk_usd = _to_float(features.get("risk_usd"))

    pnl_total = 0.0
    for ev in outcome_evs:
        op = ev.get("payload") or {}
        try:
            pnl_piece = float(op.get("pnl_usd") or 0.0)
        except Exception:
            pnl_piece = 0.0
        pnl_total += pnl_piece

    realized_rr: Optional[float] = None
    if risk_usd is not None and abs(risk_usd) > 1e-8:
        realized_rr = pnl_total / risk_usd

    # Result classification
    if pnl_total > 0:
        result = "WIN"
    elif pnl_total < 0:
        result = "LOSS"
    elif abs(pnl_total) < 1e-8:
        result = "BREAKEVEN"
    else:
        result = "UNKNOWN"

    side = _infer_side_from_features(features)

    # Mode / metadata
    mode = str(extra.get("mode") or "").upper() or "UNKNOWN"
    timeframe = extra.get("timeframe")
    sub_uid = extra.get("sub_uid")

    duration_human = _fmt_duration(duration_ms)

    journal = {
        "result": result,
        "realized_rr": realized_rr,
        "realized_pnl": pnl_total,
        "rating_score": None,
        "rating_reason": None,
        "duration_ms": duration_ms,
        "duration_human": duration_human,
    }

    row: Dict[str, Any] = {
        "trade_id": trade_id,
        "symbol": symbol,
        "account_label": account_label,
        "strategy_name": strategy_name,
        "side": side,
        "opened_at_ms": ts_open_ms,
        "closed_at_ms": ts_close_ms,
        "risk_pct": risk_pct,
        "features": features,
        "mode": mode,
        "sub_uid": sub_uid,
        "timeframe": timeframe,
        "journal": journal,
    }

    return row


def build_setup_outcomes() -> None:
    log.info("[setup_outcomes] Loading setups from %s ...", SETUPS_PATH)
    setups = list(_load_jsonl(SETUPS_PATH))
    log.info("[setup_outcomes] Loaded %d setup events.", len(setups))

    log.info("[setup_outcomes] Loading outcomes from %s ...", OUTCOMES_PATH)
    outcomes = list(_load_jsonl(OUTCOMES_PATH))
    log.info("[setup_outcomes] Loaded %d outcome events.", len(outcomes))

    setups_by_trade = _index_setups(setups)
    outcomes_by_trade = _group_outcomes(outcomes)

    trade_ids = sorted(set(setups_by_trade.keys()) & set(outcomes_by_trade.keys()))

    missing_outcomes = sorted(set(setups_by_trade.keys()) - set(outcomes_by_trade.keys()))
    missing_setups = sorted(set(outcomes_by_trade.keys()) - set(setups_by_trade.keys()))

    if missing_outcomes:
        log.warning(
            "[setup_outcomes] %d trade_ids have SetupContext but no OutcomeRecord (ignored).",
            len(missing_outcomes),
        )
    if missing_setups:
        log.warning(
            "[setup_outcomes] %d trade_ids have OutcomeRecord but no SetupContext (ignored).",
            len(missing_setups),
        )

    merged: List[Dict[str, Any]] = []
    for tid in trade_ids:
        try:
            row = _build_outcome_row(tid, setups_by_trade[tid], outcomes_by_trade[tid])
            if row:
                merged.append(row)
        except Exception as e:  # pragma: no cover
            log.exception("[setup_outcomes] Error building row for trade_id=%s: %r", tid, e)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("wb") as f:
        for row in merged:
            f.write(orjson.dumps(row) + b"\n")

    log.info(
        "[setup_outcomes] Wrote %d merged trades -> %s",
        len(merged),
        OUTPUT_PATH,
    )
    log.info("[setup_outcomes] Done.")


if __name__ == "__main__":
    build_setup_outcomes()
