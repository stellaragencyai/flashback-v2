#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Multi-Account Smoke Test

Goals
-----
- Build a small synthetic signals file with:
    * One "fake" BUY signal per strategy in strategies.yaml.
- Feed it through the real executor in PAPER mode via executor_replay.

This lets us confirm:
    - strategy_gate wiring
    - per-subaccount routing
    - AI gate + policy usage
    - feature logging + AI events
without touching live orders.

Usage
-----
    python -m app.tools.multi_account_smoke
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import sys
import time

try:
    import yaml  # type: ignore
except Exception:
    print("[multi_account_smoke] ERROR: PyYAML not installed. Run `pip install pyyaml`.")
    sys.exit(1)

try:
    from app.core.config import settings
except ImportError:
    from core.config import settings  # type: ignore

from app.tools.executor_replay import replay_file  # type: ignore

ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])
CONFIG_DIR: Path = ROOT / "config"
SIGNALS_DIR: Path = ROOT / "signals"

STRATS_FILE: Path = CONFIG_DIR / "strategies.yaml"
SMOKE_FILE: Path = SIGNALS_DIR / "multi_account_smoke.jsonl"


def _load_yaml(path: Path) -> Any:
    if not path.exists():
        print(f"[multi_account_smoke] ERROR: {path} does not exist.")
        return None
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)  # type: ignore


def _extract_strategies(data: Any) -> List[Dict[str, Any]]:
    if not isinstance(data, dict):
        return []
    subs = data.get("subaccounts")
    if isinstance(subs, list):
        return subs
    return []


def _build_fake_signal(strat: Dict[str, Any]) -> Dict[str, Any]:
    name = strat.get("name", "UnknownStrat")
    symbols = strat.get("symbols") or []
    tfs = strat.get("timeframes") or strat.get("time_frames") or []

    if not symbols or not tfs:
        # Skip completely broken ones
        return {}

    symbol = str(symbols[0])
    tf = str(tfs[0])

    # Use a semi-realistic price so tick logic doesn't blow up.
    # We don't care about exact value for the smoke test.
    fake_price = 100.0

    ts = int(time.time() * 1000)

    return {
        "ts_ms": ts,
        "symbol": symbol,
        "timeframe": tf,
        "side": "buy",
        "price": fake_price,
        "setup_type": "smoke_test",
        "debug": {
            "source": "multi_account_smoke",
            "strategy_name": name,
        },
    }


def build_smoke_signals() -> int:
    data = _load_yaml(STRATS_FILE)
    strats = _extract_strategies(data)

    if not strats:
        print("[multi_account_smoke] No strategies found in strategies.yaml")
        return 0

    SIGNALS_DIR.mkdir(parents=True, exist_ok=True)

    written = 0
    with SMOKE_FILE.open("wb") as f:
        for strat in strats:
            sig = _build_fake_signal(strat)
            if not sig:
                continue
            f.write(json.dumps(sig).encode("utf-8") + b"\n")
            written += 1

    print(f"[multi_account_smoke] Wrote {written} synthetic signals -> {SMOKE_FILE}")
    return written


def main() -> None:
    count = build_smoke_signals()
    if count <= 0:
        print("[multi_account_smoke] Nothing to replay; aborting.")
        return

    print("[multi_account_smoke] Starting dry-run replay through executor_v2...")
    import asyncio

    asyncio.run(replay_file(SMOKE_FILE, max_lines=None))
    print("[multi_account_smoke] Smoke test complete.")


if __name__ == "__main__":
    main()
