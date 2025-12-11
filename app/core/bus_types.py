#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Core bus types

Purpose
-------
Central place to define in-process "buses" used by various workers.

Right now we only need:
    - ai_events_bus: deque of AI-related events, consumed by
      app.ai.ai_events_spine (and any future AI workers).

Later you can extend this with:
    - order_events_bus
    - trade_events_bus
    - heartbeat_bus
    etc.

Design
------
- Buses are simple deques in memory.
- Writers push events with .append(...)
- Readers (workers) pop with .popleft() in a loop.

AI Event Shapes
---------------
For AI learning we standardize two main event shapes:

    SetupRecord (event_type="setup_context"):
        - trade_id: unique trade identifier
        - symbol, account_label, strategy
        - optional setup_type, timeframe, ai_profile
        - payload.features: dict of numeric/categorical features
        - payload.extra: optional misc metadata

    OutcomeRecord (event_type="outcome_record"):
        - trade_id: same identifier as SetupRecord
        - symbol, account_label, strategy
        - payload.pnl_usd, r_multiple, win, exit_reason
        - payload.extra: optional misc metadata

These map directly onto:
    state/ai_events/setups.jsonl
    state/ai_events/outcomes.jsonl
"""

from __future__ import annotations

from collections import deque
from typing import Any, Deque, Dict, Optional  # noqa: F401
from typing import Literal, TypedDict


# ---------------------------------------------------------------------------
# Typed schemas for AI events
# ---------------------------------------------------------------------------

class SetupRecord(TypedDict, total=False):
    """
    Canonical AI Setup event.

    Top-level keys mirror what ai_events_spine logs to setups.jsonl.
    Additional fields can live under payload["features"] and payload["extra"].
    """
    event_type: Literal["setup_context"]
    ts: int
    trade_id: str
    symbol: str
    account_label: str
    strategy: str

    # Optional but recommended for learning:
    setup_type: str            # e.g. "trend_pullback", "breakout_high"
    timeframe: str             # e.g. "5m", "15m"
    ai_profile: str            # e.g. "trend_v1", "breakout_v1"

    # Free-form but structured container for features + metadata
    payload: Dict[str, Any]


class OutcomeRecord(TypedDict, total=False):
    """
    Canonical AI Outcome event.

    Linked to a SetupRecord by trade_id. The enriched outcomes.jsonl file
    may embed both the original setup + outcome and computed stats.
    """
    event_type: Literal["outcome_record"]
    ts: int
    trade_id: str
    symbol: str
    account_label: str
    strategy: str

    # payload:
    #   pnl_usd: float
    #   r_multiple: Optional[float]
    #   win: Optional[bool]
    #   exit_reason: Optional[str]
    #   extra: Optional[Dict[str, Any]]
    payload: Dict[str, Any]


# Generic AI event type for the bus; at runtime this is just a dict.
AIEvent = Dict[str, Any]


# ---------------------------------------------------------------------------
# AI events bus
# ---------------------------------------------------------------------------

# Event shape is flexible at runtime, but expected to match one of:
#   - SetupRecord (event_type="setup_context")
#   - OutcomeRecord (event_type="outcome_record")
#
# See app.ai.ai_events_spine for canonical builders.
ai_events_bus: Deque[AIEvent] = deque()

# ---------------------------------------------------------------------------
# Placeholder for future buses (documented, but not required yet)
# ---------------------------------------------------------------------------
#
# Example (uncomment/extend when you actually use them):
#
# order_events_bus: Deque[Dict[str, Any]] = deque()
# trade_events_bus: Deque[Dict[str, Any]] = deque()
# heartbeat_bus: Deque[Dict[str, Any]] = deque()
#
# Each worker that uses them should import from here, not define
# its own separate deque, so everything stays on a single spine.
