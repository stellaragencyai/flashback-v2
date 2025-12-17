#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Sample AI Policy (toy, fixed for ai_state v2 + deterministic test action)

What this does
- Reads positions from ai_state["snapshot_v2"]["positions"]["by_symbol"] (AI Pilot v2.8)
- Emits actions shaped for ai_pilot._run_sample_policy (needs symbol + side)
- If no positions exist, can emit ONE deterministic test action (DRY-RUN only)
  controlled by env: AI_PILOT_SAMPLE_FORCE_ACTION (default: true)
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional


def _env_bool(name: str, default: str = "true") -> bool:
    raw = os.getenv(name, default)
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")


def _normalize_side(side: Any) -> Optional[str]:
    """
    Normalize various Bybit-ish side strings into 'buy'/'sell'.
    """
    if side is None:
        return None
    s = str(side).strip().lower()
    if not s:
        return None
    if s in ("buy", "long"):
        return "buy"
    if s in ("sell", "short"):
        return "sell"
    # Common title-cased Bybit outputs: "Buy"/"Sell"
    if s == "buy":
        return "buy"
    if s == "sell":
        return "sell"
    return None


def evaluate_state(ai_state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Returns a list of simple "raw actions" for ai_pilot._run_sample_policy.
    Each action MUST include at least:
      - symbol
      - side  ('buy' or 'sell')
    Optional:
      - confidence
      - reason
    """
    actions: List[Dict[str, Any]] = []

    label = str(ai_state.get("label", "unknown") or "unknown")
    dry_run = bool(ai_state.get("dry_run", True))

    snap = ai_state.get("snapshot_v2") or {}
    pos_block = snap.get("positions") or {}
    pos_map = pos_block.get("by_symbol") or {}

    # Emit one action per open position (toy behavior)
    if isinstance(pos_map, dict):
        for symbol, row in pos_map.items():
            if not isinstance(row, dict):
                continue

            sym = str(symbol).upper().strip()
            if not sym:
                continue

            side_raw = row.get("side") or row.get("positionSide")
            size_raw = row.get("size") or row.get("qty")

            side = _normalize_side(side_raw)
            if side is None:
                continue

            try:
                if size_raw is not None and float(size_raw) == 0.0:
                    continue
            except Exception:
                # if size isn't parseable, we still allow it (it's a toy policy)
                pass

            actions.append(
                {
                    "symbol": sym,
                    "side": side,
                    "confidence": 0.55,
                    "reason": f"sample_policy_open_position({label})",
                }
            )

    # Deterministic test action (so we can validate ai_actions.jsonl writing)
    # Only allowed in DRY-RUN to avoid accidental live trading.
    force_action = _env_bool("AI_PILOT_SAMPLE_FORCE_ACTION", "true")
    if dry_run and force_action and not actions:
        actions.append(
            {
                "symbol": os.getenv("AI_PILOT_SAMPLE_TEST_SYMBOL", "BTCUSDT").strip().upper() or "BTCUSDT",
                "side": os.getenv("AI_PILOT_SAMPLE_TEST_SIDE", "buy").strip().lower() or "buy",
                "confidence": float(os.getenv("AI_PILOT_SAMPLE_TEST_CONF", "0.60") or "0.60"),
                "reason": "sample_policy_forced_test_action",
            }
        )

    return actions
