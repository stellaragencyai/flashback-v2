#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Setup Outcomes Builder (trade_id spine) v1.2

Fixes vs v1.0
-------------
- Consumes BOTH outcome_enriched (preferred) and outcome_record (fallback).
- Accepts strategy field drift: `strategy` OR `strategy_name`.
- Timeframe is read from setup.timeframe or setup.payload.extra.timeframe.
- If enriched exists for a trade_id, it wins (single-row truth).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import orjson

try:
    from app.core.config import settings  # type: ignore
    from app.core.logger import get_logger  # type: ignore
except Exception:  # pragma: no cover
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


def _get_strategy(ev: Dict[str, Any]) -> str:
    s = ev.get("strategy")
    if isinstance(s, str) and s.strip():
        return s.strip()
    s2 = ev.get("strategy_name")
    if isinstance(s2, str) and s2.strip():
        return s2.strip()
    return "unknown"


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


def _timeframe_from_setup(setup_ev: Dict[str, Any]) -> Any:
    if "timeframe" in setup_ev:
        return setup_ev.get("timeframe")
    payload = setup_ev.get("payload") or {}
    if isinstance(payload, dict):
        extra = payload.get("extra") or {}
        if isinstance(extra, dict) and "timeframe" in extra:
            return extra.get("timeframe")
    return None


# ----------------- indexing -----------------

def _index_setups(rows: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for ev in rows:
        if ev.get("event_type") != "setup_context":
            continue
        tid = str(ev.get("trade_id") or "").strip()
        if not tid:
            continue
        out[tid] = ev
    return out


def _index_outcomes(rows: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    One-trade_id truth:
    - Prefer outcome_enriched if present
    - Else last outcome_record
    """
    out: Dict[str, Dict[str, Any]] = {}
    for ev in rows:
        et = ev.get("event_type")
        if et not in ("outcome_enriched", "outcome_record"):
            continue
        tid = str(ev.get("trade_id") or "").strip()
        if not tid:
            continue

        prev = out.get(tid)
        if prev is None:
            out[tid] = ev
            continue

        # Enriched always wins
        if ev.get("event_type") == "outcome_enriched":
            out[tid] = ev
            continue

        # If we already have enriched, keep it
        if prev.get("event_type") == "outcome_enriched":
            continue

        # Both are outcome_record: last wins
        out[tid] = ev
    return out


# ----------------- core transform -----------------

def _build_row(trade_id: str, setup_ev: Dict[str, Any], out_ev: Dict[str, Any]) -> Dict[str, Any]:
    symbol = str(setup_ev.get("symbol") or out_ev.get("symbol") or "").upper()
    account_label = str(setup_ev.get("account_label") or out_ev.get("account_label") or "main")
    strategy_name = _get_strategy(setup_ev) if _get_strategy(setup_ev) != "unknown" else _get_strategy(out_ev)

    payload = setup_ev.get("payload") or {}
    if not isinstance(payload, dict):
        payload = {}

    features = payload.get("features") or {}
    if not isinstance(features, dict):
        features = {}

    extra = payload.get("extra") or {}
    if not isinstance(extra, dict):
        extra = {}

    # times
    ts_open_ms = _to_int(setup_ev.get("ts"))
    ts_close_ms = _to_int(out_ev.get("ts"))

    duration_ms: Optional[int] = None
    if ts_open_ms is not None and ts_close_ms is not None and ts_close_ms >= ts_open_ms:
        duration_ms = ts_close_ms - ts_open_ms

    # timeframe
    timeframe = _timeframe_from_setup(setup_ev)

    # risk fields
    risk_pct = _to_float(features.get("risk_pct"))
    risk_usd = _to_float(features.get("risk_usd"))

    # stats
    realized_pnl: Optional[float] = None
    realized_rr: Optional[float] = None
    result = "UNKNOWN"

    if out_ev.get("event_type") == "outcome_enriched":
        stats = out_ev.get("stats") or {}
        if isinstance(stats, dict):
            realized_pnl = _to_float(stats.get("pnl_usd"))
            realized_rr = _to_float(stats.get("r_multiple"))
            win = stats.get("win")
            if win is True:
                result = "WIN"
            elif win is False:
                result = "LOSS"
            else:
                # fallback on pnl sign
                if realized_pnl is not None:
                    if realized_pnl > 0:
                        result = "WIN"
                    elif realized_pnl < 0:
                        result = "LOSS"
                    else:
                        result = "BREAKEVEN"
    else:
        op = out_ev.get("payload") or {}
        if not isinstance(op, dict):
            op = {}
        realized_pnl = _to_float(op.get("pnl_usd"))
        # Compute RR if possible
        if realized_pnl is not None and risk_usd is not None and abs(risk_usd) > 1e-8:
            realized_rr = realized_pnl / risk_usd
        if realized_pnl is not None:
            if realized_pnl > 0:
                result = "WIN"
            elif realized_pnl < 0:
                result = "LOSS"
            else:
                result = "BREAKEVEN"

    side = _infer_side_from_features(features)

    mode = str(extra.get("mode") or "").upper() or "UNKNOWN"
    sub_uid = extra.get("sub_uid")

    journal = {
        "result": result,
        "realized_rr": realized_rr,
        "realized_pnl": realized_pnl,
        "rating_score": None,
        "rating_reason": None,
        "duration_ms": duration_ms,
        "duration_human": _fmt_duration(duration_ms),
    }

    return {
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


def build_setup_outcomes() -> None:
    log.info("[setup_outcomes] Loading setups from %s ...", SETUPS_PATH)
    setups = list(_load_jsonl(SETUPS_PATH))
    log.info("[setup_outcomes] Loaded %d setup events.", len(setups))

    log.info("[setup_outcomes] Loading outcomes from %s ...", OUTCOMES_PATH)
    outcomes = list(_load_jsonl(OUTCOMES_PATH))
    log.info("[setup_outcomes] Loaded %d outcome events.", len(outcomes))

    setups_by_trade = _index_setups(setups)
    outcomes_by_trade = _index_outcomes(outcomes)

    trade_ids = sorted(set(setups_by_trade.keys()) & set(outcomes_by_trade.keys()))

    missing_outcomes = sorted(set(setups_by_trade.keys()) - set(outcomes_by_trade.keys()))
    missing_setups = sorted(set(outcomes_by_trade.keys()) - set(setups_by_trade.keys()))

    if missing_outcomes:
        log.warning("[setup_outcomes] %d trade_ids have SetupContext but no Outcome (ignored).", len(missing_outcomes))
    if missing_setups:
        log.warning("[setup_outcomes] %d trade_ids have Outcome but no SetupContext (ignored).", len(missing_setups))

    merged: List[Dict[str, Any]] = []
    for tid in trade_ids:
        try:
            merged.append(_build_row(tid, setups_by_trade[tid], outcomes_by_trade[tid]))
        except Exception as e:  # pragma: no cover
            log.exception("[setup_outcomes] Error building row for trade_id=%s: %r", tid, e)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("wb") as f:
        for row in merged:
            f.write(orjson.dumps(row) + b"\n")

    log.info("[setup_outcomes] Wrote %d merged trades -> %s", len(merged), OUTPUT_PATH)
    log.info("[setup_outcomes] Done.")


if __name__ == "__main__":
    build_setup_outcomes()
