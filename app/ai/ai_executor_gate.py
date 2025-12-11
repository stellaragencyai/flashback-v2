#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Executor AI Gate (v3.0, with policy audit logging)

Purpose
-------
Central decision point for: "Should this AI-driven setup be allowed to execute?"

Responsibilities:
  - Apply per-strategy policy thresholds (min score, modes allowed, etc.).
  - Optionally consider execution mode (PAPER / LIVE_CANARY / LIVE_FULL).
  - Return a structured decision payload.
  - Append EVERY decision (allow or deny) to state/ai_policy_log.jsonl for audit.

Typical usage from executor_v2:

    from app.ai.executor_ai_gate import ai_gate_decide, load_setup_policy

    policy = load_setup_policy()
    policy_cfg = policy.get(strategy_name, policy.get("__default__", {}))

    decision = ai_gate_decide(
        strategy_name=strategy_name,
        symbol=symbol,
        account_label=account_label,
        mode=mode,                    # PAPER / LIVE_CANARY / LIVE_FULL
        features=features_dict,       # snapshot at setup time
        raw_score=score,              # classifier score (0..1) or None
        policy_cfg=policy_cfg,
        trade_id=trade_id,
    )

    if not decision["allow"]:
        # skip trade, maybe log decision["reason"]
        return

Config
------
We expect setup_policy.json to look roughly like:

  {
    "__default__": {
      "min_score": 0.0,
      "min_score_live": 0.6,
      "min_score_canary": 0.5,
      "enabled_modes": ["PAPER", "LIVE_CANARY", "LIVE_FULL"]
    },
    "Sub1_Trend": {
      "min_score_live": 0.65,
      "min_score_canary": 0.55
    },
    ...
  }

Any missing keys fall back to __default__ values.

Outputs
-------
ai_gate_decide(...) returns a dict:

  {
    "allow": bool,
    "reason": "ok" | "<policy_block_reason>",
    "strategy_name": str,
    "symbol": str,
    "account_label": str,
    "mode": str,
    "trade_id": str | None,

    "score": float | None,
    "min_score": float | None,
    "min_score_live": float | None,
    "min_score_canary": float | None,

    "policy_flags": {...},   # raw policy cfg for this strategy
    "ts_ms": int,            # decision timestamp (ms)
  }

Every decision is appended to: state/ai_policy_log.jsonl
You can later analyze this via a separate stats script.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import orjson

try:
    from app.core.config import settings  # type: ignore
    from app.core.logger import get_logger  # type: ignore
except Exception:  # pragma: no cover
    # Fallbacks for ad-hoc runs
    class _DummySettings:  # type: ignore
        ROOT = Path(__file__).resolve().parents[2]

    settings = _DummySettings()  # type: ignore

    import logging

    def get_logger(name: str):  # type: ignore
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name)


ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])
STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

POLICY_PATH: Path = STATE_DIR / "setup_policy.json"
POLICY_LOG_PATH: Path = STATE_DIR / "ai_policy_log.jsonl"

log = get_logger("executor_ai_gate")

# Optional kill switch for policy logging
AI_POLICY_LOG_DISABLE = os.getenv("AI_POLICY_LOG_DISABLE", "false").lower() in ("1", "true", "yes")


# ---------------------------------------------------------------------------
# Policy loading
# ---------------------------------------------------------------------------

def load_setup_policy() -> Dict[str, Any]:
    """
    Load setup_policy.json from state/.

    If it doesn't exist or is invalid, returns a safe default.
    """
    if not POLICY_PATH.exists():
        log.warning("[ai_gate] %s not found; using default permissive policy.", POLICY_PATH)
        return {
            "__default__": {
                "min_score": 0.0,
                "min_score_live": 0.6,
                "min_score_canary": 0.5,
                "enabled_modes": ["PAPER", "LIVE_CANARY", "LIVE_FULL"],
            }
        }

    try:
        raw = POLICY_PATH.read_bytes()
        data = orjson.loads(raw)
        if not isinstance(data, dict):
            raise ValueError("setup_policy.json is not an object")
        return data
    except Exception as e:  # pragma: no cover
        log.exception("[ai_gate] Failed to load %s: %r", POLICY_PATH, e)
        return {
            "__default__": {
                "min_score": 0.0,
                "min_score_live": 0.6,
                "min_score_canary": 0.5,
                "enabled_modes": ["PAPER", "LIVE_CANARY", "LIVE_FULL"],
            }
        }


def _resolve_policy_for_strategy(policy: Dict[str, Any], strategy_name: str) -> Dict[str, Any]:
    default_cfg = policy.get("__default__", {})
    strat_cfg = policy.get(strategy_name, {})
    if not isinstance(default_cfg, dict):
        default_cfg = {}
    if not isinstance(strat_cfg, dict):
        strat_cfg = {}
    merged = dict(default_cfg)
    merged.update(strat_cfg)
    return merged


# ---------------------------------------------------------------------------
# Logging helper
# ---------------------------------------------------------------------------

def _append_policy_log(decision: Dict[str, Any]) -> None:
    """
    Append a single JSONL line to ai_policy_log.jsonl.
    """
    if AI_POLICY_LOG_DISABLE:
        return

    try:
        POLICY_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with POLICY_LOG_PATH.open("ab") as f:
            f.write(orjson.dumps(decision) + b"\n")
    except Exception as e:  # pragma: no cover
        log.exception("[ai_gate] Failed to append to %s: %r", POLICY_LOG_PATH, e)


# ---------------------------------------------------------------------------
# Core gating logic
# ---------------------------------------------------------------------------

def _normalize_mode(mode: Any) -> str:
    m = str(mode or "").upper().strip()
    if m in ("PAPER", "LIVE_CANARY", "LIVE_FULL"):
        return m
    return "UNKNOWN"


def _extract_min_scores(policy_cfg: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    def _to_float(x: Any) -> Optional[float]:
        if x is None:
            return None
        try:
            return float(x)
        except Exception:
            return None

    min_score = _to_float(policy_cfg.get("min_score"))
    min_score_live = _to_float(policy_cfg.get("min_score_live"))
    min_score_canary = _to_float(policy_cfg.get("min_score_canary"))
    return min_score, min_score_live, min_score_canary


def ai_gate_decide(
    *,
    strategy_name: str,
    symbol: str,
    account_label: str,
    mode: str,
    features: Dict[str, Any],
    raw_score: Optional[float],
    policy_cfg: Dict[str, Any],
    trade_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Core AI gate function.

    Parameters
    ----------
    strategy_name : str
        Logical strategy identifier (e.g. "Sub1_Trend").
    symbol : str
        Trading symbol (e.g. "BTCUSDT").
    account_label : str
        Account label (e.g. "main", "flashback07").
    mode : str
        Execution mode: PAPER / LIVE_CANARY / LIVE_FULL (or garbage).
    features : dict
        Feature dict at setup time (not deeply inspected here, but logged).
    raw_score : float | None
        Model score in [0, 1], or None if no model or prediction failed.
    policy_cfg : dict
        Policy config for this strategy (merged with __default__).
    trade_id : str | None
        Optional trade_id for traceability.

    Returns
    -------
    dict
        A structured decision payload with .allow and .reason fields.
    """
    ts_ms = int(time.time() * 1000)
    mode_norm = _normalize_mode(mode)
    symbol_norm = str(symbol or "").upper()

    min_score, min_score_live, min_score_canary = _extract_min_scores(policy_cfg)
    enabled_modes = policy_cfg.get("enabled_modes") or policy_cfg.get("modes") or ["PAPER", "LIVE_CANARY", "LIVE_FULL"]
    enabled_modes = [str(m).upper() for m in enabled_modes if isinstance(m, (str, bytes))]

    score: Optional[float]
    try:
        score = float(raw_score) if raw_score is not None else None
    except Exception:
        score = None

    # ------------------------------------------------------------------
    # Start with optimistic allow; block as rules fail.
    # ------------------------------------------------------------------
    allow = True
    reason = "ok"

    # 1) Mode allowed?
    if mode_norm not in enabled_modes:
        allow = False
        reason = "mode_not_enabled"

    # 2) Missing score handling
    if allow:
        if score is None:
            # configurable behavior via policy; default: block live, allow paper
            missing_ok_modes = policy_cfg.get("missing_score_allow_modes", ["PAPER"])
            missing_ok_modes = [str(m).upper() for m in missing_ok_modes if isinstance(m, (str, bytes))]
            if mode_norm not in missing_ok_modes:
                allow = False
                reason = "missing_score_blocked"

    # 3) Threshold check
    if allow and score is not None:
        # choose threshold based on mode
        effective_min: Optional[float] = min_score
        if mode_norm == "LIVE_FULL" and min_score_live is not None:
            effective_min = min_score_live
        elif mode_norm == "LIVE_CANARY" and min_score_canary is not None:
            effective_min = min_score_canary

        if effective_min is not None and score < effective_min:
            allow = False
            reason = "score_below_min"

    decision: Dict[str, Any] = {
        "allow": allow,
        "reason": reason,
        "strategy_name": strategy_name,
        "symbol": symbol_norm,
        "account_label": account_label,
        "mode": mode_norm,
        "trade_id": trade_id,
        "score": score,
        "min_score": min_score,
        "min_score_live": min_score_live,
        "min_score_canary": min_score_canary,
        "policy_flags": {
            "enabled_modes": enabled_modes,
            "missing_score_allow_modes": policy_cfg.get("missing_score_allow_modes", ["PAPER"]),
        },
        "ts_ms": ts_ms,
    }

    _append_policy_log(decision)
    return decision


# Convenience alias for older code that might use a shorter name.
def should_allow_trade(
    *,
    strategy_name: str,
    symbol: str,
    account_label: str,
    mode: str,
    features: Dict[str, Any],
    raw_score: Optional[float],
    policy_cfg: Dict[str, Any],
    trade_id: Optional[str] = None,
) -> bool:
    """
    Thin wrapper that runs ai_gate_decide and returns decision["allow"].

    Use ai_gate_decide if you need the full decision payload.
    """
    decision = ai_gate_decide(
        strategy_name=strategy_name,
        symbol=symbol,
        account_label=account_label,
        mode=mode,
        features=features,
        raw_score=raw_score,
        policy_cfg=policy_cfg,
        trade_id=trade_id,
    )
    return decision["allow"]
