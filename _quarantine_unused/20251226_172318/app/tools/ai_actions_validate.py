#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Actions Validator

Purpose
-------
Validate the contents of state/ai_actions.jsonl against the canonical
AIAction schema (app.core.ai_action_schema).

This does NOT change anything; it's purely diagnostic.

It will:
    - Load all actions from ai_actions.jsonl
    - Classify them as:
        • heartbeat / noop / empty
        • trade-like-but-broken (missing fields)
        • valid trade-like actions (future)
    - Print a summary so you can see how "intelligent" your ai_pilot
      outputs actually are right now.

Usage (from project root):
    python -m app.tools.ai_actions_validate
"""

from __future__ import annotations

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

from app.core.ai_action_schema import (
    is_heartbeat,
    missing_trade_fields,
)


def _load_actions(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        print(f"[ai_actions_validate] No actions file at {path}")
        return []

    actions: List[Dict[str, Any]] = []
    try:
        with path.open("rb") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = orjson.loads(line)
                    if isinstance(obj, dict):
                        actions.append(obj)
                except Exception as exc:
                    print(f"[ai_actions_validate] WARNING: failed to parse line: {exc}")
    except Exception as exc:
        print(f"[ai_actions_validate] ERROR reading {path}: {exc}")
        return []

    return actions


def main() -> None:
    print(f"[ai_actions_validate] ROOT:         {ROOT}")
    print(f"[ai_actions_validate] ACTIONS_PATH: {ACTIONS_PATH}")
    print("")

    actions = _load_actions(ACTIONS_PATH)
    total = len(actions)
    print(f"[ai_actions_validate] Loaded {total} actions\n")

    if not actions:
        return

    heartbeat_count = 0
    trade_like_broken = 0
    valid_trade_like = 0

    # Collect a few samples of broken actions to show missing fields
    broken_samples: List[Dict[str, Any]] = []

    for a in actions:
        if is_heartbeat(a):
            heartbeat_count += 1
            continue

        # For now, treat anything that is not obvious heartbeat as
        # "trade-like" and check required fields.
        missing = missing_trade_fields(a)
        if missing:
            trade_like_broken += 1
            if len(broken_samples) < 5:
                sample = dict(a)
                sample["_missing_fields"] = list(missing.keys())
                broken_samples.append(sample)
        else:
            valid_trade_like += 1

    print(f"[ai_actions_validate] Heartbeat / noop / empty actions: {heartbeat_count}")
    print(f"[ai_actions_validate] Trade-like but missing fields:    {trade_like_broken}")
    print(f"[ai_actions_validate] Valid trade-like actions:         {valid_trade_like}")
    print("")

    if broken_samples:
        print("[ai_actions_validate] Sample broken trade-like actions (up to 5):\n")
        for idx, s in enumerate(broken_samples, start=1):
            print(f"--- Broken action #{idx} ---")
            ts_ms = s.get("ts_ms") or s.get("timestamp_ms")
            print(f"ts_ms: {ts_ms}")
            print(f"missing_fields: {s.get('_missing_fields')}")
            # print a trimmed view of the action
            trimmed = {k: v for k, v in s.items() if k not in ("_missing_fields", "extra", "meta")}
            print(f"payload: {trimmed}")
            print("")

    print("[ai_actions_validate] Done. Use this as a whip to beat ai_pilot into emitting real actions later.")


if __name__ == "__main__":
    main()
