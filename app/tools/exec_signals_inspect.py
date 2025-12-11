#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Exec Signals Inspector

Purpose
-------
Read the execution signals JSONL file (e.g. state/exec_signals.jsonl)
and print a human-readable summary of the most recent signals.

This is READ-ONLY and has ZERO effect on trading.

Usage (from project root):
    python -m app.tools.exec_signals_inspect
    python -m app.tools.exec_signals_inspect 50   # show last 50
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

# Default path for exec signals; can be overridden by env/settings later.
try:
    from app.core.config import settings  # type: ignore
    default_exec_path = getattr(settings, "EXEC_SIGNALS_PATH", "state/exec_signals.jsonl")
except Exception:
    default_exec_path = "state/exec_signals.jsonl"

EXEC_SIGNALS_PATH = Path(default_exec_path).resolve()


def _load_last_n(path: Path, n: int) -> List[Dict[str, Any]]:
    if not path.exists():
        print(f"[exec_signals_inspect] No exec signals file at {path}")
        return []

    try:
        with path.open("rb") as f:
            lines = f.readlines()
    except Exception as exc:
        print(f"[exec_signals_inspect] ERROR reading {path}: {exc}")
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
            print(f"[exec_signals_inspect] WARNING: failed to parse line: {exc}")
            continue

    return result


def _fmt_float(val: Any, default: str = "-") -> str:
    try:
        return f"{float(val):.4f}"
    except Exception:
        return default


def _print_signals(signals: List[Dict[str, Any]]) -> None:
    if not signals:
        print("[exec_signals_inspect] No signals to display.")
        return

    print(f"[exec_signals_inspect] Showing {len(signals)} most recent signals:\n")

    for idx, s in enumerate(signals, start=1):
        ts_ms = s.get("ts_ms")
        account_label = s.get("account_label")
        symbol = s.get("symbol")
        side = s.get("side")
        action = s.get("action")
        qty = s.get("qty")
        order_type = s.get("order_type")
        tif = s.get("time_in_force")
        price = s.get("price")
        dry_run = s.get("dry_run")

        print(f"#{idx}")
        print(f"  ts_ms       : {ts_ms}")
        print(f"  account     : {account_label}")
        print(f"  symbol      : {symbol}")
        print(f"  side        : {side}")
        print(f"  action      : {action}")
        print(f"  qty         : {_fmt_float(qty)}")
        print(f"  order_type  : {order_type}")
        print(f"  time_in_forc: {tif}")
        print(f"  price       : {_fmt_float(price)}")
        print(f"  dry_run     : {dry_run}")
        tags = s.get("tags") or []
        if tags:
            print(f"  tags        : {tags[:8]}")
        print("")


def main() -> None:
    try:
        n = int(sys.argv[1]) if len(sys.argv) > 1 else 25
    except Exception:
        n = 25

    print(f"[exec_signals_inspect] ROOT:              {ROOT}")
    print(f"[exec_signals_inspect] EXEC_SIGNALS_PATH: {EXEC_SIGNALS_PATH}")
    print(f"[exec_signals_inspect] Last N:            {n}\n")

    signals = _load_last_n(EXEC_SIGNALS_PATH, n)
    _print_signals(signals)


if __name__ == "__main__":
    main()
