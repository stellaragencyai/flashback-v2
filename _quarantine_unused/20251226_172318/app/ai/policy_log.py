#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Policy Decision Log (legacy adapter)

Reality check:
-------------
You already have a canonical policy audit log in:
    state/ai_policy_log.jsonl

This module exists because older code (like executor_v2) may still import:
    from app.ai.policy_log import record_policy_decision

So we keep it stable, best-effort, never-fail, and we write to the SAME
canonical log file to avoid forks in observability.

Row format:
-----------
This module logs a compact row that is compatible with analysis tools, and
it does NOT block trading if logging fails.
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

# Canonical log path (matches ai_executor_gate.py)
POLICY_LOG_PATH = STATE_DIR / "ai_policy_log.jsonl"


def _normalize_mode(signal: Dict[str, Any]) -> str:
    mode = str(signal.get("mode") or "").upper().strip()
    if mode in ("PAPER", "LIVE_CANARY", "LIVE_FULL"):
        return mode
    return "UNKNOWN"


def _normalize_timeframe(tf: Any, default: str = "5m") -> str:
    s = ""
    try:
        s = str(tf).strip().lower()
    except Exception:
        s = ""
    if not s:
        return default
    if s.endswith(("m", "h", "d", "w")):
        return s
    if s.isdigit():
        return f"{s}m"
    return s or default


def record_policy_decision(
    strategy_id: str,
    allow: bool,
    score: Optional[float],
    reason: str,
    signal: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Append a single policy decision row to state/ai_policy_log.jsonl.
    Best-effort; failures are swallowed so trading is never blocked by logging.
    """
    try:
        ts_ms = int(time.time() * 1000)

        sym = None
        tf_raw = None
        mode = "UNKNOWN"
        raw_signal_out: Optional[Dict[str, Any]] = None

        if isinstance(signal, dict):
            sym = signal.get("symbol")
            tf_raw = signal.get("timeframe") or signal.get("tf")
            mode = _normalize_mode(signal)

            raw_signal_out = dict(signal)
            if "timeframe" in raw_signal_out or "tf" in raw_signal_out:
                raw_signal_out["timeframe"] = _normalize_timeframe(
                    raw_signal_out.get("timeframe") or raw_signal_out.get("tf"),
                    default="5m",
                )

        tf = _normalize_timeframe(tf_raw, default="5m") if tf_raw is not None else None

        row: Dict[str, Any] = {
            "ts_ms": ts_ms,
            "strategy_name": str(strategy_id),
            "allow": bool(allow),
            "score": float(score) if score is not None else None,
            "reason": str(reason),
            "symbol": str(sym or "").upper() or None,
            "timeframe": tf,
            "mode": mode,
            "source": "policy_log.record_policy_decision",
        }

        if raw_signal_out is not None:
            row["raw_signal"] = raw_signal_out

        with POLICY_LOG_PATH.open("ab") as f:
            f.write(_dumps(row) + b"\n")
    except Exception:
        return
