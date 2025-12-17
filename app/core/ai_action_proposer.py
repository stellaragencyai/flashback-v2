#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Flashback — AI Action Proposer (Phase 4) v1.1

Purpose
- Convert a PilotDecision into a proposed DRY action
- Does NOT execute orders

Contract
- Input: setup_event (setup_context-like dict), decision (PilotDecision)
- Output: ProposedAction or None
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from app.core.ai_decision_types import PilotDecision, ProposedAction


def _safe_float(x: Any, default: float) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def propose_from_decision(
    setup_event: Dict[str, Any],
    decision: PilotDecision,
) -> Optional[ProposedAction]:
    """
    Deterministic proposal builder.

    Rules (simple, explainable):
    - Only propose when decision == ALLOW_TRADE
    - Size scales by tier + sample size n
      Tier A:
        n >= 5  -> 1.00
        n 2..4  -> 0.50
      Tier B:
        n >= 5  -> 0.50
        n 2..4  -> 0.25
      Otherwise -> no proposal
    - risk_R_cap scales down when r_mean is barely above threshold
    """
    code = str(decision.get("decision") or "").strip().upper()
    if code != "ALLOW_TRADE":
        return None

    symbol = str(setup_event.get("symbol") or "").strip().upper()
    if not symbol:
        return None

    # For now we prove pipeline. Side can come from policy later.
    side = "buy"

    mem = decision.get("memory") or {}
    stats = mem.get("stats") or {}
    gates = decision.get("gates") or {}

    tier = str(decision.get("tier_used") or "NONE").strip().upper()
    n = 0
    try:
        n = int(stats.get("n") or 0)
    except Exception:
        n = 0

    r_mean = stats.get("r_mean")
    r_mean_f = _safe_float(r_mean, 0.0)

    min_r_mean = _safe_float(gates.get("min_r_mean"), 0.10)

    # -------------------------
    # Size scaling (deterministic)
    # -------------------------
    size_mult: float = 0.0

    if tier == "A":
        if n >= 5:
            size_mult = 1.0
        elif n >= 2:
            size_mult = 0.5
    elif tier == "B":
        if n >= 5:
            size_mult = 0.5
        elif n >= 2:
            size_mult = 0.25
    else:
        size_mult = 0.0

    if size_mult <= 0.0:
        return None

    # -------------------------
    # Risk cap scaling
    # -------------------------
    # If r_mean is only barely above threshold, cap risk harder.
    # This prevents "technically passed" garbage from sizing up.
    risk_R_cap = 1.0
    if r_mean_f < (min_r_mean + 0.05):
        risk_R_cap = 0.5

    # Confidence: prefer explicit "score" if present, else fallback.
    confidence = _safe_float(mem.get("score"), 0.6)
    confidence = max(0.0, min(1.0, confidence))

    reason = str((gates.get("reason") or "memory_gated_ok")).strip() or "memory_gated_ok"

    return {
        "action_type": "PROPOSE_TRADE",
        "symbol": symbol,
        "side": side,
        "size_multiplier": float(size_mult),
        "risk_R_cap": float(risk_R_cap),
        "confidence": float(confidence),
        "reason": reason,
        "tags": ["phase4", "shadow_mode", "proposed_only"],
        "extra": {
            "tier_used": decision.get("tier_used"),
            "memory_id": mem.get("memory_id"),
            "n": n,
            "r_mean": r_mean,
        },
    }
