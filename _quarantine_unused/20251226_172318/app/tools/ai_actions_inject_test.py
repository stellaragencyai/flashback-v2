#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Actions Inject Test

Purpose
-------
Inject a single, well-formed AIAction into the AI actions JSONL file
(AI_ACTIONS_PATH) so you can verify the full pipeline:

    ai_pilot (or injector) -> ai_actions.jsonl
        -> ai_action_router + guard_action(...)
        -> ExecSignal queue (state/exec_signals.jsonl)
        -> exec_signals_inspect

This is a TEST TOOL, not part of live AI inference.
"""

from __future__ import annotations

import os
import time
import uuid
from pathlib import Path
from typing import Dict, Any

import orjson

try:
    from app.core.config import settings  # type: ignore
    default_actions_path = getattr(settings, "AI_ACTIONS_PATH", "state/ai_actions.jsonl")
except Exception:
    default_actions_path = "state/ai_actions.jsonl"

env_actions_path = os.getenv("AI_ACTIONS_PATH", "").strip()
_actions_path_str = env_actions_path or default_actions_path

ACTIONS_FILE = Path(_actions_path_str).resolve()
ACTIONS_FILE.parent.mkdir(parents=True, exist_ok=True)


def build_test_action() -> Dict[str, Any]:
    """
    Build a single, schema-compliant trade-like AIAction.

    This is intentionally simple and conservative:
        - type = "open"
        - side = "long"
        - symbol = "BTCUSDT"
        - risk_R = 1.0
        - expected_R = 2.0
        - size_fraction = 1.0
        - dry_run = True

    The guard + router should ACCEPT this and turn it into an ExecSignal.
    """
    ts_ms = int(time.time() * 1000)

    action_id = f"test_action_{uuid.uuid4().hex}"

    action: Dict[str, Any] = {
        "ts_ms": ts_ms,
        "account_label": "main",
        "action_id": action_id,

        "type": "open",        # trade-bearing
        "symbol": "BTCUSDT",
        "side": "long",

        "risk_mode": "R",
        "risk_R": 1.0,
        "expected_R": 2.0,
        "size_fraction": 1.0,

        "entry_hint": None,
        "sl_hint": None,
        "tp_hint": None,

        "confidence": 0.85,
        "reason": "inject_test_open_long",
        "tags": ["inject_test", "smoke", "btc"],
        "dry_run": True,
        "model_id": "INJECT_TEST_V1",

        "extra": {
            "note": "synthetic test AIAction to validate router + guard + exec queue",
        },
    }

    return action


def main() -> None:
    print(f"[ai_actions_inject_test] AI_ACTIONS_PATH: {ACTIONS_FILE}")

    action = build_test_action()
    payload = orjson.dumps(action)

    try:
        with ACTIONS_FILE.open("ab") as f:
            f.write(payload)
            f.write(b"\n")
    except Exception as exc:
        print(f"[ai_actions_inject_test] ERROR writing to {ACTIONS_FILE}: {exc}")
        return

    print("[ai_actions_inject_test] Injected test AIAction:")
    print(action)
    print("\nNow watch ai_action_router logs and then inspect exec_signals via:")
    print("    python -m app.tools.exec_signals_inspect")


if __name__ == "__main__":
    main()
