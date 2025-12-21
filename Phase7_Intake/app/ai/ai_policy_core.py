#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Core AI Policy (scaffold v1.1)

Now snapshot-aware:
- If ai_state["safety"]["is_safe"] is False -> do nothing (no actions)
"""

from __future__ import annotations

from typing import Any, Dict, List


def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def evaluate_state(ai_state: Dict[str, Any]) -> List[Dict[str, Any]]:
    actions: List[Dict[str, Any]] = []

    label = str(ai_state.get("label", "unknown"))
    dry_run = bool(ai_state.get("dry_run", True))

    # Hard safety gate
    safety = ai_state.get("safety") or {}
    if safety.get("is_safe") is False:
        actions.append(
            {
                "type": "risk_alert",
                "reason": "snapshot_unsafe",
                "label": label,
                "dry_run": dry_run,
                "details": {
                    "reasons": safety.get("reasons") or [],
                    "thresholds_sec": safety.get("thresholds_sec") or {},
                },
            }
        )
        return actions

    account = ai_state.get("account") or {}
    positions = ai_state.get("positions") or []

    mmr = _to_float(account.get("mmr_pct"), 0.0)
    open_positions = int(account.get("open_positions") or 0)

    MMR_WARN = 70.0

    if mmr > MMR_WARN:
        actions.append(
            {
                "type": "risk_alert",
                "reason": "mmr_high",
                "label": label,
                "mmr_pct": mmr,
                "open_positions": open_positions,
                "dry_run": dry_run,
            }
        )

    if isinstance(positions, list):
        for p in positions:
            if not isinstance(p, dict):
                continue
            symbol = p.get("symbol")
            side = p.get("side") or p.get("positionSide")
            size = p.get("size") or p.get("qty")

            if not symbol or not side or size in (None, "", 0, "0"):
                continue

            actions.append(
                {
                    "type": "monitor_position",
                    "reason": "core_policy_stub",
                    "label": label,
                    "symbol": str(symbol),
                    "side": str(side),
                    "size": str(size),
                    "dry_run": dry_run,
                }
            )

    return actions
