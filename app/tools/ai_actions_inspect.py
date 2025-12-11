#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Actions Inspector

Purpose
-------
Read the AI actions JSONL file (state/ai_actions.jsonl) and print a
human-readable summary of the most recent actions.

This is READ-ONLY and has ZERO effect on trading. It's just for debugging
what ai_pilot is actually emitting.

Usage (from project root):
    python -m app.tools.ai_actions_inspect
    python -m app.tools.ai_actions_inspect 50   # show last 50 actions
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List

import orjson

try:
    from app.core.config import settings
    ROOT: Path = settings.ROOT  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

STATE_DIR = ROOT / "state"
ACTIONS_PATH = STATE_DIR / "ai_actions.jsonl"


def _load_last_n_actions(path: Path, n: int) -> List[Dict[str, Any]]:
    if not path.exists():
        print(f"[ai_actions_inspect] No actions file at {path}")
        return []

    # Read all lines, then take the last n. For now, file is small enough
    # that this is fine. If it grows huge later, we can switch to a
    # streaming tail approach.
    try:
        with path.open("rb") as f:
            lines = f.readlines()
    except Exception as exc:
        print(f"[ai_actions_inspect] ERROR reading {path}: {exc}")
        return []

    result: List[Dict[str, Any]] = []
    for raw in lines[-n:]:
        raw = raw.strip()
        if not raw:
            continue
        try:
            obj = orjson.loads(raw)
            if isinstance(obj, dict):
                result.append(obj)
        except Exception as exc:
            print(f"[ai_actions_inspect] WARNING: failed to parse line: {exc}")
            continue

    return result


def _fmt_float(val: Any, default: str = "-") -> str:
    try:
        return f"{float(val):.4f}"
    except Exception:
        return default


def _print_actions(actions: List[Dict[str, Any]]) -> None:
    if not actions:
        print("[ai_actions_inspect] No actions to display.")
        return

    print(f"[ai_actions_inspect] Showing {len(actions)} most recent actions:\n")

    for idx, a in enumerate(actions, start=1):
        ts_ms = a.get("ts_ms") or a.get("timestamp_ms")
        action_type = a.get("type") or a.get("action_type") or "?"
        symbol = a.get("symbol") or a.get("sym") or "?"
        side = a.get("side") or a.get("direction") or "?"
        reason = a.get("reason") or a.get("tag") or a.get("label") or ""
        confidence = a.get("confidence")
        risk_r = a.get("risk_r")
        exp_r = a.get("expected_r")

        print(f"#{idx}")
        print(f"  ts_ms      : {ts_ms}")
        print(f"  type       : {action_type}")
        print(f"  symbol     : {symbol}")
        print(f"  side       : {side}")
        print(f"  confidence : {_fmt_float(confidence)}")
        print(f"  risk_R     : {_fmt_float(risk_r)}")
        print(f"  expected_R : {_fmt_float(exp_r)}")
        if reason:
            print(f"  reason     : {reason}")
        # dump extra if present
        extra = a.get("extra") or a.get("meta") or {}
        if extra:
            # Keep this compact; we don't want to explode the log.
            print(f"  extra_keys : {list(extra.keys())[:8]}")
        print("")


def main() -> None:
    # Determine how many actions to show (default 25)
    try:
        n = int(sys.argv[1]) if len(sys.argv) > 1 else 25
    except Exception:
        n = 25

    print(f"[ai_actions_inspect] ROOT:         {ROOT}")
    print(f"[ai_actions_inspect] ACTIONS_PATH: {ACTIONS_PATH}")
    print(f"[ai_actions_inspect] Last N:       {n}\n")

    actions = _load_last_n_actions(ACTIONS_PATH, n)
    _print_actions(actions)


if __name__ == "__main__":
    main()
