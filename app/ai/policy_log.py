#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Policy Decision Log

Purpose
-------
Append every AI gate decision to a JSONL file so you can later:

    - Inspect how often each strategy is being blocked/allowed.
    - See score distributions by strategy / symbol / mode.
    - Correlate policy behavior with later trade outcomes.

Output:
    state/ai_policy_decisions.jsonl

Each row:
    {
      "ts_ms": 1733640000123,
      "strategy_id": "Sub1_Trend (sub 524630315)",
      "allow": true,
      "score": 0.73,
      "reason": "passes_simple_policy",
      "symbol": "BTCUSDT",
      "timeframe": "5",
      "mode": "UNKNOWN" | "PAPER" | "LIVE_CANARY" | "LIVE_FULL",
      "raw_signal": {...}   # optional
    }
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional
import time

try:
    import orjson  # type: ignore
except Exception:
    orjson = None  # type: ignore
    import json as _json  # type: ignore

    def _dumps(obj: Any) -> bytes:
        return _json.dumps(obj, separators=(",", ":")).encode("utf-8")
else:

    def _dumps(obj: Any) -> bytes:
        return orjson.dumps(obj)  # type: ignore


try:
    from app.core.config import settings  # type: ignore

    ROOT: Path = settings.ROOT  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

STATE_DIR = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

POLICY_LOG_PATH = STATE_DIR / "ai_policy_decisions.jsonl"


def _normalize_mode(signal: Dict[str, Any]) -> str:
    mode = str(signal.get("mode") or "").upper().strip()
    if mode in ("PAPER", "LIVE_CANARY", "LIVE_FULL"):
        return mode
    return "UNKNOWN"


def record_policy_decision(
    strategy_id: str,
    allow: bool,
    score: Optional[float],
    reason: str,
    signal: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Append a single policy decision row to state/ai_policy_decisions.jsonl.
    Best-effort; failures are swallowed so trading is never blocked by logging.
    """
    try:
        ts_ms = int(time.time() * 1000)

        sym = None
        tf = None
        mode = "UNKNOWN"

        if isinstance(signal, dict):
            sym = signal.get("symbol")
            tf = signal.get("timeframe") or signal.get("tf")
            mode = _normalize_mode(signal)

        row: Dict[str, Any] = {
            "ts_ms": ts_ms,
            "strategy_id": strategy_id,
            "allow": bool(allow),
            "score": float(score) if score is not None else None,
            "reason": str(reason),
            "symbol": str(sym or "").upper() or None,
            "timeframe": str(tf) if tf is not None else None,
            "mode": mode,
        }

        if isinstance(signal, dict):
            row["raw_signal"] = signal

        with POLICY_LOG_PATH.open("ab") as f:
            f.write(_dumps(row) + b"\n")
    except Exception:
        # Never crash caller due to logging issues.
        return
