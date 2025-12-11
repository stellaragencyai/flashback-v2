#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Sample AI Policy (toy, fixed for ai_state v2)

Realistic dummy policy:
- Reads the *real* position structure from ai_state_bus / ai_pilot.
- Emits one advisory action per open position.
"""

from __future__ import annotations

from typing import Any, Dict, List


def evaluate_state(ai_state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Minimal sample policy (fixed).

    ai_state structure reference:
        ai_state = {
          "label": "main",
          "dry_run": True,
          "account": {...},
          "positions": [...],          # flattened list (pilot convenience)
          "buses": {...},
          "raw_snapshot": {
              "positions": {
                  "raw": [...],
                  "by_symbol": {
                      "BTCUSDT": { ... full bybit row ... },
                      "ETHUSDT": { ... },
                  }
              },
              ...
          }
        }
    """

    actions: List[Dict[str, Any]] = []

    label = str(ai_state.get("label", "unknown"))
    dry_run = bool(ai_state.get("dry_run", True))

    # Pull proper upstream positions map
    snap = ai_state.get("raw_snapshot") or {}
    pos_block = snap.get("positions") or {}
    pos_map = pos_block.get("by_symbol") or {}

    if not isinstance(pos_map, dict):
        return actions

    for symbol, row in pos_map.items():
        if not isinstance(row, dict):
            continue

        sym = str(symbol).upper()

        side = row.get("side") or row.get("positionSide")  # depending on Bybit row
        size = row.get("size") or row.get("qty")

        if not sym or not side or not size:
            continue
        try:
            if float(size) == 0:
                continue
        except Exception:
            pass

        actions.append(
            {
                "type": "advice_only",
                "reason": "sample_policy",
                "label": label,
                "symbol": sym,
                "side": str(side),
                "size": str(size),
                "dry_run": dry_run,
            }
        )

    return actions
