#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Core bus types

Purpose
-------
Central place to define in-process "buses" used by various workers.

Right now we need:
    - ai_events_bus: deque of AI-related events
    - memory_bus: deque of canonical MemoryRecord events (Phase 4+)

Design
------
- Buses are simple deques in memory.
- Writers push events with .append(...)
- Readers (workers) pop with .popleft() in a loop.
"""

from __future__ import annotations

from collections import deque
from typing import Any, Deque, Dict, Optional  # noqa: F401
from typing import Literal, TypedDict


# ---------------------------------------------------------------------------
# Typed schemas for AI events (Phase 3)
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

    Linked to a SetupRecord by trade_id.
    """
    event_type: Literal["outcome_record"]
    ts: int
    trade_id: str
    symbol: str
    account_label: str
    strategy: str

    payload: Dict[str, Any]


# ---------------------------------------------------------------------------
# Typed schema for Memory (Phase 4)
# ---------------------------------------------------------------------------

class MemoryRecord(TypedDict, total=False):
    """
    Canonical AI Memory record (Phase 4 contract).

    This is what the learning layer should consume. It is intentionally:
      - bounded
      - append/merge-stats only
      - policy-stamped
      - fingerprint-indexed

    NOTE: Memory is derived from Phase 3 artifacts; it does NOT rewrite history.
    """
    event_type: Literal["memory_record"]
    ts: int

    # contract + versioning
    schema_version: int

    # deterministic identity
    memory_id: str
    setup_fingerprint: str
    policy_hash: str

    # scopes
    timeframe: str
    symbol_scope: str        # "ANY" or a symbol like "BTCUSDT"
    account_scope: str       # "global" or a specific account label

    # lightweight stats (merge-stats only)
    stats: Dict[str, Any]

    # lifecycle guardrails
    lifecycle: Dict[str, Any]

    # optional for search/filtering
    tags: Any
    notes: str


# Generic runtime event types
AIEvent = Dict[str, Any]
MemoryEvent = Dict[str, Any]


# ---------------------------------------------------------------------------
# Buses
# ---------------------------------------------------------------------------

ai_events_bus: Deque[AIEvent] = deque()

# Phase 4: canonical memory bus for in-process consumers (optional)
memory_bus: Deque[MemoryEvent] = deque()
