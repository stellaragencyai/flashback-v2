#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Status CLI

Quick health overview:
  - Reads state/heartbeats.json (if present)
  - Shows age of core state files (positions, executions, AI events)
  - Emits a simple GREEN / YELLOW / RED status.

This is a *read-only* tool, safe to run any time.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Dict, Tuple

try:
    import orjson  # type: ignore
except Exception:  # fallback
    import json as orjson  # type: ignore


ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "state"
HB_PATH = STATE_DIR / "heartbeats.json"


GREEN_MAX = 5.0    # seconds
YELLOW_MAX = 30.0  # seconds


def load_heartbeats() -> Dict[str, float]:
    if not HB_PATH.exists():
        return {}
    try:
        raw = HB_PATH.read_bytes()
        data = orjson.loads(raw)
        if isinstance(data, dict):
            return {str(k): float(v) for k, v in data.items()}
    except Exception:
        pass
    return {}


def age_to_status(age: float) -> Tuple[str, str]:
    if age < 0:
        age = 0.0
    if age <= GREEN_MAX:
        return "🟢", "GREEN"
    if age <= YELLOW_MAX:
        return "🟡", "YELLOW"
    return "🔴", "RED"


def file_age(path: Path) -> float:
    if not path.exists():
        return float("inf")
    now = time.time()
    mtime = path.stat().st_mtime
    return max(0.0, now - mtime)


def print_section(title: str) -> None:
    print("")
    print(title)
    print("-" * len(title))


def main() -> int:
    print("=== Flashback Status ===")
    print(f"ROOT: {ROOT}")
    print(f"STATE_DIR: {STATE_DIR}")

    now = time.time()

    # ------------------------------------------------------------------
    # 1) Heartbeats
    # ------------------------------------------------------------------
    hbs = load_heartbeats()
    print_section("Process Heartbeats")

    if not hbs:
        print("No heartbeats.json found or file empty.")
    else:
        for name in sorted(hbs.keys()):
            ts = float(hbs[name])
            age = max(0.0, now - ts)
            icon, status = age_to_status(age)
            print(f"{icon} {name:24s} age={age:6.1f}s [{status}]")

    # ------------------------------------------------------------------
    # 2) Core state file ages
    # ------------------------------------------------------------------
    print_section("Core State Files")

    core_files = {
        "positions_bus.json": STATE_DIR / "positions_bus.json",
        "ws_executions.jsonl": STATE_DIR / "ws_executions.jsonl",
        "ai_setups.jsonl": STATE_DIR / "ai_events" / "setups.jsonl",
        "ai_outcomes.jsonl": STATE_DIR / "ai_events" / "outcomes.jsonl",
    }

    any_red = False

    for label, path in core_files.items():
        age = file_age(path)
        icon, status = age_to_status(age if age != float("inf") else 1e9)
        if age == float("inf"):
            print(f"{icon} {label:24s} MISSING [{status}]   -> {path}")
            if status == "RED":
                any_red = True
        else:
            print(f"{icon} {label:24s} age={age:6.1f}s [{status}] -> {path}")
            if status == "RED":
                any_red = True

    print("")
    if any_red:
        print("OVERALL STATUS: 🔴 RED (some components stale or missing)")
        return 1

    print("OVERALL STATUS: ✅ OK (no RED components detected)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
