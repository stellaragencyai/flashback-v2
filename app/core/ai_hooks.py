#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 / Flashback — AI Hooks

Thin convenience layer on top of:
- app.core.ai_schema
- app.core.ai_store (optional; falls back to stub if missing)

Purpose:
- Give bots a simple, stable API to log AI-relevant events:
    • log_signal_from_engine(...)
    • log_order_basic(...)
    • log_trade_summary_basic(...)

If ai_store is NOT available yet, this module will:
    - Not crash.
    - Print lightweight [AI_STORE_STUB] messages instead of persisting data.

ADDED:
- Lightweight JSONL mirroring into state/ai_events:
    • Signals -> "setup" events in setups.jsonl
    • Trade summaries -> "outcome" events in outcomes.jsonl
"""

from __future__ import annotations

import time
import uuid
from pathlib import Path
import json
from typing import Optional, List, Dict, Any

import orjson

from app.core.ai_schema import (
    SignalLog,
    OrderLog,
    TradeSummaryLog,
)

# ============================
# Phase 5: Training Idempotency
# ============================

_TRAINED_TRADES_PATH = Path("data/trained_trades.json")


def _load_trained_trades() -> set[str]:
    try:
        if _TRAINED_TRADES_PATH.exists():
            with _TRAINED_TRADES_PATH.open("r", encoding="utf-8") as f:
                return set(json.load(f))
    except Exception:
        pass
    return set()


def _mark_trained_once(trade_id: str) -> bool:
    trained = _load_trained_trades()
    if trade_id in trained:
        return False

    trained.add(trade_id)
    _TRAINED_TRADES_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _TRAINED_TRADES_PATH.open("w", encoding="utf-8") as f:
        json.dump(sorted(trained), f, indent=2)

    return True



# ---------- ai_store (real or stub) ----------

_HAS_AI_STORE = False

try:
    # Try to use the real store implementation if you have it wired.
    from app.core import ai_store as _ai_store  # type: ignore
    _HAS_AI_STORE = True
except Exception:
    # Fallback: stubbed store that just prints to stdout so nothing crashes.
    class _StubAIStore:
        def log_signal(self, payload: SignalLog) -> None:
            sid = payload.get("signal_id")
            sym = payload.get("symbol")
            tf = payload.get("timeframe")
            side = payload.get("side")
            print(f"[AI_STORE_STUB] log_signal: {sid} {sym} {tf} {side}")

        def log_order(self, payload: OrderLog) -> None:
            oid = payload.get("order_id")
            sym = payload.get("symbol")
            print(f"[AI_STORE_STUB] log_order: {oid} {sym}")

        def log_trade_summary(self, payload: TradeSummaryLog) -> None:
            tid = payload.get("trade_id")
            sym = payload.get("symbol")
            outcome = payload.get("outcome")
            print(f"[AI_STORE_STUB] log_trade_summary: {tid} {sym} outcome={outcome}")

    _ai_store = _StubAIStore()  # type: ignore


def _now_ms() -> int:
    return int(time.time() * 1000)


# ============================================================================
# AI EVENTS JSONL MIRROR (state/ai_events)
# ============================================================================

try:
    # Prefer central config root if available
    from app.core.config import settings
    ROOT: Path = settings.ROOT  # type: ignore
except Exception:
    # Fallback: derive project root from this file location
    ROOT = Path(__file__).resolve().parents[2]

STATE_DIR: Path = ROOT / "state"
AI_EVENTS_DIR: Path = STATE_DIR / "ai_events"
SETUPS_PATH: Path = AI_EVENTS_DIR / "setups.jsonl"
OUTCOMES_PATH: Path = AI_EVENTS_DIR / "outcomes.jsonl"


def _ensure_ai_events_dir() -> None:
    """
    Ensure state/ai_events exists. Never raises on failure in normal flow;
    errors will be surfaced when writing.
    """
    try:
        AI_EVENTS_DIR.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        # Last resort: do not crash trading because of logging dir issues.
        print(f"[AI_EVENTS] WARNING: could not create dir {AI_EVENTS_DIR}: {exc}")


def _append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    """
    Append one JSON object as a line to the given path. Fail-soft to avoid
    breaking bots due to logging problems.
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("ab") as f:
            f.write(orjson.dumps(payload))
            f.write(b"\n")
    except Exception as exc:
        print(f"[AI_EVENTS] WARNING: failed to append to {path}: {exc}")


def _mirror_signal_to_ai_events(
    *,
    signal_payload: SignalLog,
) -> None:
    """
    Mirror a signal-log payload into the ai_events "setup" stream.

    This is intentionally lightweight and tolerant to missing fields.
    """
    _ensure_ai_events_dir()

    ts_ms = signal_payload.get("ts_ms") or _now_ms()
    signal_id = signal_payload.get("signal_id")

    event: Dict[str, Any] = {
        "type": "setup",
        "version": 1,
        "ts_ms": ts_ms,
        "signal_id": signal_id,
        "symbol": signal_payload.get("symbol"),
        "timeframe": signal_payload.get("timeframe"),
        "side": signal_payload.get("side"),
        "source": signal_payload.get("source"),
        "confidence": signal_payload.get("confidence"),
        "stop_hint_price": signal_payload.get("stop_hint_price"),
        "owner": signal_payload.get("owner"),
        "sub_uid": signal_payload.get("sub_uid"),
        "strategy_role": signal_payload.get("strategy_role"),
        "regime_tags": signal_payload.get("regime_tags") or [],
        "extra": signal_payload.get("extra") or {},
    }

    _append_jsonl(SETUPS_PATH, event)


def _mirror_trade_to_ai_events(
    *,
    trade_payload: TradeSummaryLog,
) -> None:
    """
    Mirror a trade-summary payload into the ai_events "outcome" stream.
    """
    _ensure_ai_events_dir()

    ts_ms = trade_payload.get("closed_ts_ms") or _now_ms()

    event: Dict[str, Any] = {
        "type": "outcome",
        "version": 1,
        "ts_ms": ts_ms,
        "trade_id": trade_payload.get("trade_id"),
        "symbol": trade_payload.get("symbol"),
        # timeframe is not part of TradeSummaryLog schema; can be inferred
        # later by joining to signals or strategy metadata if needed.
        "side": trade_payload.get("side"),
        "sub_uid": trade_payload.get("sub_uid"),
        "owner": trade_payload.get("owner"),
        "strategy_role": trade_payload.get("strategy_role"),
        "outcome": trade_payload.get("outcome"),
        "r_multiple": trade_payload.get("r_multiple"),
        "realized_pnl": trade_payload.get("realized_pnl"),
        "signal_id": trade_payload.get("signal_id"),
        "entry_price": trade_payload.get("entry_price"),
        "exit_price": trade_payload.get("exit_price"),
        "max_favorable_excursion_r": trade_payload.get("max_favorable_excursion_r"),
        "max_adverse_excursion_r": trade_payload.get("max_adverse_excursion_r"),
        "holding_ms": trade_payload.get("holding_ms"),
        "exit_reason": trade_payload.get("exit_reason"),
        "regime_at_entry": trade_payload.get("regime_at_entry"),
        "regime_at_exit": trade_payload.get("regime_at_exit"),
        "extra": trade_payload.get("extra") or {},
    }

    _append_jsonl(OUTCOMES_PATH, event)


# ============================================================================
# SIGNALS
# ============================================================================

def log_signal_from_engine(
    *,
    symbol: str,
    timeframe: str,
    side: str,
    source: str = "signal_engine",
    confidence: Optional[float] = None,
    stop_hint: Optional[float] = None,
    owner: Optional[str] = None,
    sub_uid: Optional[str] = None,
    strategy_role: Optional[str] = None,
    regime_tags: Optional[List[str]] = None,
    extra: Optional[Dict[str, Any]] = None,
    signal_id: Optional[str] = None,
    ts_ms: Optional[int] = None,
) -> str:
    """
    Convenience wrapper for logging a signal.

    Returns:
        signal_id (str) that was used/stored.
    """
    if signal_id is None:
        # Slightly human-readable ID: <uuid4>-<symbol>-<tf>
        signal_id = f"{uuid.uuid4().hex}_{symbol}_{timeframe}"

    if ts_ms is None:
        ts_ms = _now_ms()

    payload: SignalLog = {
        "signal_id": signal_id,
        "ts_ms": ts_ms,
        "symbol": symbol,
        "timeframe": timeframe,
        "side": side,
        "source": source,
        "confidence": confidence,
        "stop_hint_price": stop_hint,
        "owner": owner,
        "sub_uid": sub_uid,
        "strategy_role": strategy_role,
        "regime_tags": regime_tags or [],
        "extra": extra or {},
    }

    # 1) Persist into AI store (SQLite or stub)
    _ai_store.log_signal(payload)

    # 2) Mirror into ai_events JSONL (setup stream)
    _mirror_signal_to_ai_events(signal_payload=payload)

    return signal_id


# ============================================================================
# ORDERS
# ============================================================================

def log_order_basic(
    *,
    order_id: str,
    symbol: str,
    side: str,
    order_type: str,
    qty: float,
    price: float,
    signal_id: Optional[str] = None,
    sub_uid: Optional[str] = None,
    owner: Optional[str] = None,
    strategy_role: Optional[str] = None,
    exit_profile: Optional[str] = None,
    reduce_only: Optional[bool] = None,
    post_only: Optional[bool] = None,
    extra: Optional[Dict[str, Any]] = None,
    ts_ms: Optional[int] = None,
) -> None:
    """
    Convenience wrapper to log an order when you place it.
    """
    if ts_ms is None:
        ts_ms = _now_ms()

    payload: OrderLog = {
        "order_id": order_id,
        "ts_ms": ts_ms,
        "symbol": symbol,
        "side": side,
        "order_type": order_type,
        "qty": float(qty),
        "price": float(price),
        "signal_id": signal_id,
        "sub_uid": sub_uid,
        "owner": owner,
        "strategy_role": strategy_role,
        "exit_profile": exit_profile,
        "reduce_only": reduce_only,
        "post_only": post_only,
        "extra": extra or {},
    }

    _ai_store.log_order(payload)
    # NOTE: Orders are *not* mirrored to ai_events by design here.
    # They are more granular than the setup/outcome abstraction.


# ============================================================================
# TRADES (CLOSED ROUND-TRIPS)
# ============================================================================

def log_trade_summary_basic(
    *,
    trade_id: str,
    sub_uid: str,
    symbol: str,
    side: str,
    opened_ts_ms: int,
    closed_ts_ms: int,
    outcome: str,
    r_multiple: float,
    realized_pnl: float,
    signal_id: Optional[str] = None,
    owner: Optional[str] = None,
    strategy_role: Optional[str] = None,
    entry_price: Optional[float] = None,
    exit_price: Optional[float] = None,
    max_favorable_excursion_r: Optional[float] = None,
    max_adverse_excursion_r: Optional[float] = None,
    holding_ms: Optional[int] = None,
    exit_reason: Optional[str] = None,
    regime_at_entry: Optional[str] = None,
    regime_at_exit: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Convenience wrapper to log a fully closed trade.

    Typically called from:
    - TP/SL manager when a position hits TP / SL
    - Trade journal when you manually close a trade
    """
    payload: TradeSummaryLog = {
        "trade_id": trade_id,
        "sub_uid": sub_uid,
        "symbol": symbol,
        "side": side,
        "opened_ts_ms": opened_ts_ms,
        "closed_ts_ms": closed_ts_ms,
        "outcome": outcome,
        "r_multiple": float(r_multiple),
        "realized_pnl": float(realized_pnl),
        "signal_id": signal_id,
        "owner": owner,
        "strategy_role": strategy_role,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "max_favorable_excursion_r": max_favorable_excursion_r,
        "max_adverse_excursion_r": max_adverse_excursion_r,
        "holding_ms": holding_ms,
        "exit_reason": exit_reason,
        "regime_at_entry": regime_at_entry,
        "regime_at_exit": regime_at_exit,
        "extra": extra or {},
    }

    # 1) Persist into AI store (SQLite or stub)
    _ai_store.log_trade_summary(payload)

    # 2) Mirror into ai_events JSONL (outcome stream)
    _mirror_trade_to_ai_events(trade_payload=payload)
