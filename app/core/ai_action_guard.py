#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Action Guard (minimal, robust version)

Centralized guardrail logic for AI actions emitted by ai_pilot and
consumed by ai_action_router.

This version:
    - Avoids heavy config / ROOT dependencies at import time.
    - Reads its config from environment variables only.
    - Uses ai_action_schema helpers:
        • is_heartbeat(...)
        • missing_trade_fields(...)

It does NOT place orders. It only validates and classifies AI actions.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from app.core.ai_action_schema import (
    is_heartbeat,
    missing_trade_fields,
)


# ---------------------------------------------------------------------------
# Config model
# ---------------------------------------------------------------------------

@dataclass
class GuardConfig:
    """
    Derived config for AI action guard.
    """
    allowed_symbols: List[str]
    require_whitelist: bool
    max_notional_pct: float


@dataclass
class GuardResult:
    """
    Result of validating a single AI action.
    """
    ok: bool
    # High-level classification
    is_heartbeat: bool
    is_trade_like: bool
    # If not ok, reason codes
    reasons: List[str]
    # Possibly corrected / normalized copy
    action: Dict[str, Any]


# ---------------------------------------------------------------------------
# Config loader (env only)
# ---------------------------------------------------------------------------

def load_guard_config() -> GuardConfig:
    """
    Lightweight config loader.

    Reads from environment variables ONLY:

        AI_ALLOWED_SYMBOLS   (comma-separated symbols)
        AI_REQUIRE_WHITELIST (true/false)
        AI_MAX_NOTIONAL_PCT  (float)

    This avoids importing app.core.config at module import time.
    """
    raw_syms = os.getenv("AI_ALLOWED_SYMBOLS", "").strip()
    if raw_syms:
        allowed_symbols = [s.strip().upper() for s in raw_syms.split(",") if s.strip()]
    else:
        allowed_symbols = []

    require_whitelist_raw = os.getenv("AI_REQUIRE_WHITELIST", "false").strip().lower()
    require_whitelist = require_whitelist_raw in ("1", "true", "yes", "y")

    try:
        max_notional_pct = float(os.getenv("AI_MAX_NOTIONAL_PCT", "40"))
    except Exception:
        max_notional_pct = 40.0

    return GuardConfig(
        allowed_symbols=allowed_symbols,
        require_whitelist=bool(require_whitelist),
        max_notional_pct=max_notional_pct,
    )


# ---------------------------------------------------------------------------
# Core guard function
# ---------------------------------------------------------------------------

def guard_action(
    raw_action: Dict[str, Any],
    cfg: Optional[GuardConfig] = None,
) -> GuardResult:
    """
    Validate and lightly normalize a single AI action.

    This does NOT perform R->size conversions; it only enforces structural
    sanity and global symbol/risk constraints.

    Returns:
        GuardResult with ok flag, reasons, classification, and possibly
        slightly normalized action.
    """
    if cfg is None:
        cfg = load_guard_config()

    # Shallow copy so we don't mutate caller state
    action = dict(raw_action)
    reasons: List[str] = []

    # --- Legacy compatibility patch --------------------------
    # Older ai_pilot writes "label" instead of "account_label".
    if "account_label" not in action and "label" in action:
        action["account_label"] = action["label"]
    # --------------------------------------------------------

    # Basic heartbeat vs trade-like classification
    hb = is_heartbeat(action)

    # If it's a pure heartbeat/noop, we require at least an account_label.
    is_trade_like = False
    if hb:
        if not action.get("account_label"):
            reasons.append("missing_account_label_for_heartbeat")
            return GuardResult(
                ok=False,
                is_heartbeat=True,
                is_trade_like=False,
                reasons=reasons,
                action=action,
            )
        return GuardResult(
            ok=True,
            is_heartbeat=True,
            is_trade_like=False,
            reasons=reasons,
            action=action,
        )

    # Treat anything else as trade-like and check required fields
    is_trade_like = True
    missing = missing_trade_fields(action)
    if missing:
        reasons.append(f"missing_required_fields: {sorted(missing.keys())}")

    # Symbol whitelist enforcement
    sym = str(action.get("symbol") or "").upper()
    if not sym:
        reasons.append("symbol_empty")
    else:
        if cfg.allowed_symbols and cfg.require_whitelist and sym not in cfg.allowed_symbols:
            reasons.append(f"symbol_not_in_whitelist: {sym}")

    # Risk sanity checks
    risk_r = action.get("risk_R")
    try:
        if risk_r is not None and float(risk_r) <= 0:
            reasons.append("risk_R_non_positive")
    except Exception:
        reasons.append("risk_R_non_numeric")

    expected_r = action.get("expected_R")
    try:
        if expected_r is not None and float(expected_r) <= 0:
            reasons.append("expected_R_non_positive")
    except Exception:
        reasons.append("expected_R_non_numeric")

    # Size fraction sanity
    sf = action.get("size_fraction")
    if sf is not None:
        try:
            sf_f = float(sf)
            if sf_f <= 0 or sf_f > 1.5:
                reasons.append("size_fraction_out_of_range")
        except Exception:
            reasons.append("size_fraction_non_numeric")

    ok = not reasons

    return GuardResult(
        ok=ok,
        is_heartbeat=hb,
        is_trade_like=is_trade_like,
        reasons=reasons,
        action=action,
    )
