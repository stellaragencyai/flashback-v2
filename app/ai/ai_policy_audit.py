#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Policy Audit Logger (v1)

Purpose
-------
Append one JSONL row per AI gate decision so we can later inspect:

    - which strategies are being blocked / allowed
    - AI scores vs min_ai_score
    - context (account_label, sub_uid, symbol, timeframe, mode, side)

Output:
    state/ai_policy_log.jsonl

This is intentionally append-only and lightweight.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict

import orjson

try:
    from app.core.config import settings
except ImportError:
    from core.config import settings  # type: ignore

ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])
STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

LOG_PATH: Path = STATE_DIR / "ai_policy_log.jsonl"


def _now_ms() -> int:
    return int(time.time() * 1000)


def log_policy_decision(
    *,
    strat_id: str,
    signal: Dict[str, Any],
    ctx: Dict[str, Any],
    decision: Dict[str, Any],
) -> None:
    """
    Parameters
    ----------
    strat_id : str
        Human-readable strategy label (e.g. "Sub2_Breakout (sub 524633243)").
    signal : dict
        Raw signal dict passed into executor_v2.
    ctx : dict
        Small context dict, usually:
            {
                "account_label": "...",
                "sub_uid": "...",
                "symbol": "...",
                "timeframe": "5",
                "side": "Buy"/"Sell"/"long"/"short",
                "mode": "PAPER" | "LIVE_CANARY" | "LIVE_FULL" | "OFF"/"UNKNOWN",
            }
    decision : dict
        Final AI gate result from run_ai_gate(), expected keys:
            {
                "allow": bool,
                "score": float | None,
                "reason": str,
                "features": dict,
                "min_score": float | None,
                "raw_allow": bool | None,
                "raw_score": float | None,
                "raw_reason": str | None,
            }
    """
    row = {
        "ts_ms": _now_ms(),
        "strategy": strat_id,
        "context": {
            "account_label": ctx.get("account_label"),
            "sub_uid": ctx.get("sub_uid"),
            "symbol": ctx.get("symbol"),
            "timeframe": ctx.get("timeframe"),
            "side": ctx.get("side"),
            "mode": ctx.get("mode"),
        },
        "decision": {
            "allow": bool(decision.get("allow", True)),
            "score": decision.get("score"),
            "reason": decision.get("reason"),
            "min_score": decision.get("min_score"),
            "raw_allow": decision.get("raw_allow"),
            "raw_score": decision.get("raw_score"),
            "raw_reason": decision.get("raw_reason"),
        },
        # Keep features very lightweight here: only ai_score + a few basic fields
        "features_snapshot": {
            "ai_score": decision.get("score"),
            "est_rr": (decision.get("features") or {}).get("est_rr"),
            "equity_usd": (decision.get("features") or {}).get("equity_usd"),
            "risk_usd": (decision.get("features") or {}).get("risk_usd"),
            "risk_pct": (decision.get("features") or {}).get("risk_pct"),
        },
        # Optional: minimal signal info for later debugging
        "signal_meta": {
            "symbol": signal.get("symbol"),
            "timeframe": signal.get("timeframe") or signal.get("tf"),
            "setup_type": signal.get("setup_type"),
        },
    }

    try:
        with LOG_PATH.open("ab") as f:
            f.write(orjson.dumps(row) + b"\n")
    except Exception:
        # Never crash executor for logging failures.
        return
