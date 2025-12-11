#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Events Spine (disk-logging version, v2.2 upgraded)

Purpose
-------
Single place to construct and publish AI-related events.

Events are:
    - Written directly to disk as JSONL:
        state/ai_events/setups.jsonl
        state/ai_events/outcomes.jsonl
    - Optionally pushed into ai_events_bus for in-process consumers.

This removes the need for a separate ai_journal_worker process just to
drain an in-memory deque across processes (which is impossible).

This module ALSO exposes a `main()` loop so supervisor_ai_stack can run it
as an "ai_journal" worker that just emits heartbeats while disk logging
is done inline by publish_ai_event().

UPGRADE (Option-1 style)
------------------------
- Maintain a crash-safe pending registry of setups:

      state/ai_events/pending_setups.json

- When an outcome arrives:
      • Write raw outcome to outcomes_raw.jsonl
      • Try to find matching setup by trade_id
      • If found, merge into an enriched outcome:
            stats.pnl_usd, stats.r_multiple, stats.win
        and append to outcomes.jsonl
      • Remove that trade_id from pending registry

v2.2 Upgrade Summary
--------------------
- Introduced typed SetupRecord / OutcomeRecord schemas in app.core.bus_types.
- build_setup_context() now supports optional setup_type, timeframe, ai_profile
  as top-level fields for better AI learning.
- publish_ai_event() remains backwards compatible with older callers that
  only pass event_type + payload.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, Optional

import orjson

from app.core.bus_types import (  # type: ignore
    ai_events_bus,
    SetupRecord,
    OutcomeRecord,
)

# ---------------------------------------------------------------------------
# Logging (robust) & heartbeat
# ---------------------------------------------------------------------------

try:
    from app.core.log import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging
    import sys

    def get_logger(name: str) -> "logging.Logger":  # type: ignore
        logger_ = logging.getLogger(name)
        if not logger_.handlers:
            handler = logging.StreamHandler(sys.stdout)
            fmt = logging.Formatter(
                "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
            )
            handler.setFormatter(fmt)
            logger_.addHandler(handler)
        logger_.setLevel(logging.INFO)
        return logger_

log = get_logger("ai_events_spine")

try:
    from app.core.flashback_common import record_heartbeat  # type: ignore
except Exception:  # pragma: no cover
    def record_heartbeat(name: str) -> None:  # type: ignore[override]
        return None
try:
    from app.core.config import settings  # type: ignore
    ROOT: Path = settings.ROOT  # type: ignore
except Exception:
    # Fallback: derive project root from this file location
    ROOT = Path(__file__).resolve().parents[2]


def _now_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------------------------------------------------------
# Paths for AI event logs
# ---------------------------------------------------------------------------

STATE_DIR: Path = ROOT / "state"
AI_EVENTS_DIR: Path = STATE_DIR / "ai_events"

STATE_DIR.mkdir(parents=True, exist_ok=True)
AI_EVENTS_DIR.mkdir(parents=True, exist_ok=True)

SETUPS_PATH: Path = AI_EVENTS_DIR / "setups.jsonl"
OUTCOMES_PATH: Path = AI_EVENTS_DIR / "outcomes.jsonl"           # legacy / enriched
OUTCOMES_RAW_PATH: Path = AI_EVENTS_DIR / "outcomes_raw.jsonl"   # raw execution outcomes

# Pending setups registry (crash-safe)
PENDING_REGISTRY_PATH: Path = AI_EVENTS_DIR / "pending_setups.json"


def _append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    """
    Append a single JSON object as one line to the given file.
    """
    try:
        with path.open("ab") as f:
            f.write(orjson.dumps(payload))
            f.write(b"\n")
    except Exception as e:
        # Best-effort logging; do NOT crash callers.
        try:
            log.warning("[ai_events] Failed to append event to %s: %r", path, e)
        except Exception:
            # Worst case: totally silent
            pass


# ---------------------------------------------------------------------------
# Pending registry helpers (crash-safe setups cache)
# ---------------------------------------------------------------------------

def _load_pending() -> Dict[str, Any]:
    """
    Load the pending setups registry from disk.

    Shape:
        {
          "<trade_id>": {<setup_event_dict>},
          ...
        }
    """
    if not PENDING_REGISTRY_PATH.exists():
        return {}
    try:
        txt = PENDING_REGISTRY_PATH.read_text(encoding="utf-8")
        data = json.loads(txt or "{}")
        if isinstance(data, dict):
            return data
        return {}
    except Exception:
        return {}


def _save_pending(reg: Dict[str, Any]) -> None:
    """
    Persist the pending registry. Overwrites atomically from our perspective.
    """
    try:
        PENDING_REGISTRY_PATH.write_text(
            json.dumps(reg, indent=2, sort_keys=True),
            encoding="utf-8",
        )
    except Exception as e:
        log.warning("[ai_events] Failed to save pending registry: %r", e)


def _merge_setup_and_outcome(
    setup_event: Dict[str, Any],
    outcome_event: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Merge a setup_context event + outcome_record into one enriched outcome.

    - Computes:
        stats.pnl_usd
        stats.r_multiple (if risk_usd available)
        stats.win (if r_multiple computed)
    - Embeds full setup and outcome payloads for AI training.
    """
    try:
        setup_payload = setup_event.get("payload", {}) or {}
        outcome_payload = outcome_event.get("payload", {}) or {}

        features = setup_payload.get("features", {}) or {}
        pnl_usd = outcome_payload.get("pnl_usd", 0.0)

        # risk_usd should have been attached by executor_v2 feature logger
        risk_usd = features.get("risk_usd")
        r_multiple = None
        if risk_usd is not None:
            try:
                r_multiple = float(pnl_usd) / float(risk_usd) if float(risk_usd) != 0 else None
            except Exception:
                r_multiple = None

        win: Optional[bool] = None
        if r_multiple is not None:
            win = r_multiple > 0

        enriched: Dict[str, Any] = {
            "event_type": "outcome_enriched",
            "ts": _now_ms(),
            "trade_id": setup_event.get("trade_id") or outcome_event.get("trade_id"),
            "symbol": setup_event.get("symbol") or outcome_event.get("symbol"),
            "account_label": setup_event.get("account_label")
            or outcome_event.get("account_label"),
            "strategy": setup_event.get("strategy") or outcome_event.get("strategy"),
            "setup_type": setup_event.get("setup_type"),
            "timeframe": setup_event.get("timeframe"),
            "ai_profile": setup_event.get("ai_profile"),
            "setup": setup_event,
            "outcome": outcome_event,
            "stats": {
                "pnl_usd": float(pnl_usd),
                "r_multiple": float(r_multiple) if r_multiple is not None else None,
                "win": win,
            },
        }
        return enriched
    except Exception as e:
        log.warning("[ai_events] Failed to merge setup/outcome: %r", e)
        # Fallback: just return the raw outcome so at least something is logged.
        return outcome_event


# ---------------------------------------------------------------------------
# Publisher
# ---------------------------------------------------------------------------

def publish_ai_event(event: Dict[str, Any]) -> None:
    """
    Push a raw AI event:
      - ensure it has a timestamp
      - write it to the appropriate JSONL file(s)
      - update pending registry for setups
      - attempt setup/outcome merge for outcomes
      - append to in-process bus for any in-process consumers

    Expected keys:
        event["event_type"] : "setup_context" | "outcome_record" | ...
        event["ts"]         : epoch_ms (int), auto-filled if missing

    This function remains tolerant of partially-specified events so that
    older callers don't break. Newer callers should prefer the typed
    builders: build_setup_context() and build_outcome_record().
    """
    if not isinstance(event, dict):
        return

    if "event_type" not in event:
        return

    if "ts" not in event:
        event["ts"] = _now_ms()

    etype = event.get("event_type")

    if etype == "setup_context":
        # 1) Legacy behavior: write to setups.jsonl
        _append_jsonl(SETUPS_PATH, event)

        # 2) Store in pending registry by trade_id
        trade_id = event.get("trade_id")
        if trade_id:
            try:
                pending = _load_pending()
                pending[str(trade_id)] = event
                _save_pending(pending)
            except Exception as e:
                log.warning(
                    "[ai_events] Failed to update pending registry for trade_id=%r: %r",
                    trade_id,
                    e,
                )

    elif etype == "outcome_record":
        # 1) Always append raw outcome (for debugging & backfill)
        _append_jsonl(OUTCOMES_RAW_PATH, event)

        trade_id = event.get("trade_id")
        if trade_id:
            # Attempt merge with existing setup
            try:
                pending = _load_pending()
                setup_evt = pending.get(str(trade_id))
            except Exception:
                setup_evt = None

            if setup_evt:
                # We have a matching setup → build enriched record
                enriched = _merge_setup_and_outcome(setup_evt, event)
                _append_jsonl(OUTCOMES_PATH, enriched)
                # Remove from pending
                try:
                    pending.pop(str(trade_id), None)
                    _save_pending(pending)
                except Exception as e:
                    log.warning(
                        "[ai_events] Failed to remove trade_id=%r from pending registry: %r",
                        trade_id,
                        e,
                    )
            else:
                # Outcome before setup (race / restart) → just log raw to legacy outcomes.jsonl
                _append_jsonl(OUTCOMES_PATH, event)
        else:
            # No trade_id at all → just keep raw
            _append_jsonl(OUTCOMES_PATH, event)

    else:
        # Unknown types can be wired later; for now we ignore them on disk
        pass

    # Still push into the in-process bus for any same-process listeners
    try:
        ai_events_bus.append(event)
    except Exception:
        # If something is wrong with the bus, don't kill disk logging.
        pass


# ---------------------------------------------------------------------------
# Build Setup Context Event
# ---------------------------------------------------------------------------

def build_setup_context(
    *,
    trade_id: str,
    symbol: str,
    account_label: str,
    strategy: str,
    features: Dict[str, Any],
    setup_type: Optional[str] = None,
    timeframe: Optional[str] = None,
    ai_profile: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> SetupRecord:
    """
    Build a canonical SetupContext event.

    Parameters
    ----------
    trade_id : unique trade identifier (same as orderLinkId / executor trade_id)
    symbol : e.g. "BTCUSDT"
    account_label : e.g. "main", "flashback07"
    strategy : human-readable strategy label (e.g. "Sub1_Trend")
    features : dict of feature values at setup/open (numeric/categorical)
    setup_type : optional label like "trend_pullback", "breakout_high"
    timeframe : optional timeframe string like "5m", "15m"
    ai_profile : optional AI profile name like "trend_v1"
    extra : optional misc fields (mode, sub_uid, etc.)

    Notes
    -----
    - Older callers that don't pass setup_type/timeframe/ai_profile
      still work; those fields will just be omitted.
    """
    payload: SetupRecord = {
        "event_type": "setup_context",
        "ts": _now_ms(),
        "trade_id": trade_id,
        "symbol": symbol,
        "account_label": account_label,
        "strategy": strategy,
        "payload": {
            "features": features or {},
        },
    }

    if setup_type is not None:
        payload["setup_type"] = setup_type
    if timeframe is not None:
        payload["timeframe"] = timeframe
    if ai_profile is not None:
        payload["ai_profile"] = ai_profile

    if extra:
        # Attach under payload["extra"] to keep features clean.
        payload["payload"]["extra"] = extra

    return payload


# ---------------------------------------------------------------------------
# Build Outcome Event
# ---------------------------------------------------------------------------

def build_outcome_record(
    *,
    trade_id: str,
    symbol: str,
    account_label: str,
    strategy: str,
    pnl_usd: float,
    r_multiple: Optional[float] = None,
    win: Optional[bool] = None,
    exit_reason: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> OutcomeRecord:
    """
    Build a canonical OutcomeRecord event.

    Parameters
    ----------
    trade_id : same id used for SetupContext
    symbol : e.g. "BTCUSDT"
    account_label : "main", "flashback07", etc.
    strategy : human-readable strategy label
    pnl_usd : realized PnL in USDT (paper or live)
    r_multiple : realized R (optional; can also be computed in merge step)
    win : True/False if known (optional)
    exit_reason : e.g. "tp_hit", "sl_hit", "manual_flatten"
    extra : any additional fields to attach under payload["extra"]
    """
    payload: OutcomeRecord = {
        "event_type": "outcome_record",
        "ts": _now_ms(),
        "trade_id": trade_id,
        "symbol": symbol,
        "account_label": account_label,
        "strategy": strategy,
        "payload": {
            "pnl_usd": float(pnl_usd),
            "r_multiple": float(r_multiple) if r_multiple is not None else None,
            "win": bool(win) if win is not None else None,
            "exit_reason": exit_reason,
        },
    }

    if extra:
        payload["payload"]["extra"] = extra

    return payload


# ---------------------------------------------------------------------------
# Minimal main() so supervisor_ai_stack can run this worker
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Minimal loop:
      - Just emits heartbeats on "ai_events_spine"
      - All real event logging is done via publish_ai_event()

    This keeps supervisor_ai_stack happy and gives you liveness telemetry
    without any extra plumbing.
    """
    log.info("AI Events Spine loop started (disk logger + heartbeat only, v2.2).")

    while True:
        try:
            record_heartbeat("ai_events_spine")
        except Exception:
            # Never crash over heartbeat issues.
            pass
        time.sleep(10)


if __name__ == "__main__":
    main()
