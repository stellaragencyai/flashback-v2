#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Core AI Policy (scaffold v1)

This is the main policy hook for real decision logic.

Input: ai_state from ai_pilot._build_ai_state():
    {
      "label": "main",
      "dry_run": True,
      "account": {
          "equity_usdt": "1234.56",
          "mmr_pct": "23.4",
          "open_positions": 2,
      },
      "positions": [ ...flattened list... ],
      "buses": {...},
      "raw_snapshot": {... full ai_state_bus snapshot ...},
    }

Output: list of "actions" (metadata only; no direct orders):

    {
      "type": "monitor_position" | "risk_alert" | ...,
      "reason": "mmr_high" | "stub_core_policy" | ...,
      "label": "<ACCOUNT_LABEL>",
      "symbol": "BTCUSDT",
      "side": "Buy",
      "size": "0.005",
      "dry_run": true,
      ... optional extra fields ...
    }
"""

from __future__ import annotations

from typing import Any, Dict, List


def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def evaluate_state(ai_state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Core policy scaffold.

    Current stub logic:
      1) Emit a risk_alert if mmr_pct above a soft threshold.
      2) Emit a monitor_position action for each open position.
    """
    actions: List[Dict[str, Any]] = []

    label = str(ai_state.get("label", "unknown"))
    dry_run = bool(ai_state.get("dry_run", True))

    account = ai_state.get("account") or {}
    positions = ai_state.get("positions") or []

    # 1) Simple risk alert on high MMR
    mmr_raw = account.get("mmr_pct")
    mmr = _to_float(mmr_raw, 0.0)
    open_positions = int(account.get("open_positions") or 0)

    # Soft threshold: can tune later or drive from config
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

    # 2) Per-position monitoring actions (stub)
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
