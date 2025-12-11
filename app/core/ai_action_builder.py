#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Action Builder

Helpers for building schema-compliant AIAction dicts for ai_pilot and
other decision-making components.

Goal:
    - Stop each bot from inventing its own random action shape.
    - Produce AIAction objects compatible with:
        • app.core.ai_action_schema
        • app.core.ai_action_guard
        • app.tools.ai_action_router (→ ExecSignal queue)

This module does NOT write to Bybit or create ExecSignals directly.
It only returns dicts ready to be JSON-encoded into AI_ACTIONS_PATH.
"""

from __future__ import annotations

import time
import uuid
from typing import Any, Dict, List, Optional

from app.core.ai_action_schema import AIAction  # type: ignore


def _now_ms() -> int:
    return int(time.time() * 1000)


def normalize_side(side_raw: str) -> str:
    """
    Normalize various side strings to 'long' / 'short'.

    Examples:
        "buy"   -> "long"
        "Buy"   -> "long"
        "sell"  -> "short"
        "Short" -> "short"

    Any unknown / garbage input raises ValueError instead of silently
    defaulting to 'long'. If you pass nonsense here, the caller is wrong.
    """
    s = (side_raw or "").strip().lower()
    if s in ("buy", "long"):
        return "long"
    if s in ("sell", "short"):
        return "short"
    raise ValueError(f"normalize_side: unsupported side value: {side_raw!r}")


def build_trade_action_from_sample(
    *,
    account_label: str,
    symbol: str,
    side: str,
    reason: str = "sample_policy",
    risk_R: float = 1.0,
    expected_R: float = 2.0,
    size_fraction: float = 1.0,
    confidence: float = 0.5,
    tags: Optional[List[str]] = None,
    model_id: str = "SAMPLE_POLICY_V1",
    extra: Optional[Dict[str, Any]] = None,
) -> AIAction:
    """
    Build a simple, trade-bearing AIAction suitable for use in sample policies.

    This is intentionally conservative:
        - type = "open"
        - risk_R = 1.0
        - expected_R = 2.0
        - size_fraction = 1.0
        - dry_run = True

    It does NOT encode exact prices or sizing; that is left to the router /
    executor stack downstream.

    The returned dict is expected to conform to app.core.ai_action_schema.AIAction.
    """
    ts_ms = _now_ms()
    side_norm = normalize_side(side)
    sym = symbol.strip().upper()

    action_id = f"ai_sample_{uuid.uuid4().hex}"

    action: AIAction = {
        "ts_ms": ts_ms,
        "account_label": account_label,
        "action_id": action_id,

        # Trade-bearing AI decision
        "type": "open",
        "symbol": sym,
        "side": side_norm,  # "long" / "short"

        # Risk semantics
        "risk_mode": "R",
        "risk_R": float(risk_R),
        "expected_R": float(expected_R),
        "size_fraction": float(size_fraction),

        # No direct price hints in this basic builder
        "entry_hint": None,
        "sl_hint": None,
        "tp_hint": None,

        # Soft meta
        "confidence": float(confidence),
        "reason": reason,
        "tags": list(tags or []),

        # Safety: AI always DRY_RUN at this layer; router/executor decide later
        "dry_run": True,
        "model_id": model_id,

        "extra": extra or {},
    }

    return action
