#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Action Guard (compat)

This module is imported by ai_action_router (and possibly legacy callers).
Your current file is a district strategy whitelist (non-Flashback).
We keep it for backward compatibility, but we also provide the API that
Flashback expects: load_guard_config().

Design goals:
- Fail-soft: never crash the stack due to missing config.
- Backward-compatible: retain enforce_strategy_policy().
"""

from __future__ import annotations

from typing import Any, Dict, List


# ---------------------------------------------------------------------------
# Legacy import (fail-soft)
# ---------------------------------------------------------------------------

try:
    from app.core.ai_profile import get_district_profile  # type: ignore
except Exception:
    def get_district_profile(district_id: str) -> dict:
        return {}


# ---------------------------------------------------------------------------
# Legacy function kept (district strategy whitelist)
# ---------------------------------------------------------------------------

def enforce_strategy_policy(district_id, strategy_id, pair):
    profile = get_district_profile(district_id)

    if not profile:
        raise RuntimeError(f"Unknown district {district_id}")

    if strategy_id not in profile.get("allowed_strategies", []):
        raise PermissionError(
            f"Strategy {strategy_id} not allowed for district {district_id}"
        )

    if pair not in profile.get("allowed_pairs", []):
        raise PermissionError(
            f"Pair {pair} not allowed for district {district_id}"
        )


# ---------------------------------------------------------------------------
# COMPAT SHIM (2026-01-01): expected by ai_action_router + legacy callers
# ---------------------------------------------------------------------------

def load_guard_config(account_label: str = "main") -> Dict[str, Any]:
    """
    Return AI action guard config for an account label.

    Fail-soft defaults: allow actions unless explicitly blocked.
    Router can layer additional checks later.
    """
    # Try modern/alternate function names if this codebase has them elsewhere.
    try:
        candidates = [
            "get_guard_config",
            "get_action_guard_config",
            "resolve_guard_config",
            "load_action_guard_config",
            "guard_config",
        ]
        g = globals()
        for name in candidates:
            fn = g.get(name)
            if callable(fn):
                try:
                    out = fn(account_label)
                except TypeError:
                    out = fn()
                except Exception:
                    continue

                if isinstance(out, dict):
                    out.setdefault("account_label", account_label)
                    return out
    except Exception:
        pass

    # Safe fallback
    return {
        "account_label": account_label,
        "enabled": True,
        "mode": "allow_by_default",
        "deny_actions": [],
        "allow_actions": [],
        "max_actions_per_minute": 120,
    }
