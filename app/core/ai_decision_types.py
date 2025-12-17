#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Flashback — AI Decision Types (Phase 4)

Purpose:
- Canonical decision contract returned by pilot_decide()
- Stable enums + schema versioning
"""

from __future__ import annotations

from typing import Any, Dict, Literal, TypedDict, Optional


DECISION_SCHEMA_VERSION = 1

DecisionCode = Literal[
    "COLD_START",
    "BLOCKED_BY_GATES",
    "ALLOW_TRADE",
    "ALLOW_REDUCED_SIZE",
    "ALLOW_FULL_SIZE",
]


class ProposedAction(TypedDict, total=False):
    """
    Proposed action is DRY by default: it is a suggestion, NOT execution.
    """
    action_type: Literal["PROPOSE_TRADE"]
    symbol: str
    side: Literal["buy", "sell"]
    size_multiplier: float          # e.g. 0.25 reduced, 1.0 full
    risk_R_cap: float               # cap risk in R terms
    confidence: float               # 0..1
    reason: str
    tags: Any
    extra: Dict[str, Any]


class PilotDecision(TypedDict, total=False):
    schema_version: int
    ts: int
    decision: DecisionCode
    tier_used: str

    # memory (if any)
    memory: Dict[str, Any]

    # gating details (always present)
    gates: Dict[str, Any]

    # optional action proposal (DRY suggestion)
    proposed_action: Optional[ProposedAction]
