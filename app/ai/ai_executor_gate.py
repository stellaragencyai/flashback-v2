#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Executor AI Gate (v3.1, schema-adaptive, with policy audit logging)

Purpose
-------
Central decision point for: "Should this AI-driven setup be allowed to execute?"

Responsibilities:
  - Apply per-strategy policy thresholds (min score, modes allowed, etc.).
  - Consider execution mode (PAPER / LIVE_CANARY / LIVE_FULL).
  - Accept an upstream "precheck" (classifier hard block) and make it auditable.
  - Return a structured decision payload.
  - Append EVERY decision (allow or deny) to state/ai_policy_log.jsonl for audit.

Policy source:
--------------
state/setup_policy.json

Supports TWO schemas:

A) v1 (your current setup_memory_policy schema):
{
  "schema_version": 1,
  "defaults": {
    "risk_multiplier": 1.0,
    "min_ai_score": 0.0
  },
  "strategies": {
    "StratA": {"risk_multiplier": 0.75, "min_ai_score": 0.55}
  }
}

B) mapping-style schema (legacy-friendly):
{
  "__default__": {
     "min_score": 0.0,
     "min_score_live": 0.6,
     "min_score_canary": 0.5,
     "enabled_modes": ["PAPER","LIVE_CANARY","LIVE_FULL"],
     "missing_score_allow_modes": ["PAPER"]
  },
  "StratA": {"min_score": 0.55}
}

Logging:
--------
Writes to: state/ai_policy_log.jsonl
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
AI_POLICY_LOG_INCLUDE_FEATURES = os.getenv("AI_POLICY_LOG_INCLUDE_FEATURES", "true").lower() in ("1", "true", "yes")


# ---------------------------------------------------------------------------
# Policy loading (schema-adaptive)
# ---------------------------------------------------------------------------

def _default_mapping_policy() -> Dict[str, Any]:
    return {
        "__default__": {
            "min_score": 0.0,
            "min_score_live": 0.6,
            "min_score_canary": 0.5,
            "enabled_modes": ["PAPER", "LIVE_CANARY", "LIVE_FULL"],
            "missing_score_allow_modes": ["PAPER"],
        }
    }


def _normalize_policy_schema(data: Any) -> Dict[str, Any]:
    """
    Convert supported policy schemas into a mapping-style dict:

      { "__default__": {...}, "StrategyX": {...}, ... }

    Returns a safe default if input is invalid.
    """
    if not isinstance(data, dict):
        return _default_mapping_policy()

    # If it already looks like mapping schema, keep it.
    if "__default__" in data and isinstance(data.get("__default__"), dict):
        return data

    # v1 schema: {"defaults": {...}, "strategies": {...}}
    defaults = data.get("defaults")
    strategies = data.get("strategies")
    if isinstance(defaults, dict) and isinstance(strategies, dict):
        min_ai = defaults.get("min_ai_score", 0.0)
        try:
            min_ai_f = float(min_ai)
        except Exception:
            min_ai_f = 0.0

        mapped: Dict[str, Any] = {
            "__default__": {
                "min_score": min_ai_f,
                # v1 does not define mode-specific mins; leave None
                "min_score_live": None,
                "min_score_canary": None,
                "enabled_modes": ["PAPER", "LIVE_CANARY", "LIVE_FULL"],
                "missing_score_allow_modes": ["PAPER"],
            }
        }

        for k, v in strategies.items():
            if not isinstance(k, str):
                continue
            if not isinstance(v, dict):
                continue
            raw_min = v.get("min_ai_score", None)
            try:
                raw_min_f = float(raw_min) if raw_min is not None else None
            except Exception:
                raw_min_f = None
            if raw_min_f is None:
                continue
            mapped[k] = {"min_score": raw_min_f}

        return mapped

    return _default_mapping_policy()


def load_setup_policy() -> Dict[str, Any]:
    """
    Load setup_policy.json from state/.

    If it doesn't exist or is invalid, returns a safe default.
    Output is normalized to mapping schema.
    """
    if not POLICY_PATH.exists():
        log.warning("[ai_gate] %s not found; using default policy.", POLICY_PATH)
        return _default_mapping_policy()

    try:
        raw = POLICY_PATH.read_bytes()
        data = orjson.loads(raw)
        return _normalize_policy_schema(data)
    except Exception as e:  # pragma: no cover
        log.exception("[ai_gate] Failed to load %s: %r", POLICY_PATH, e)
        return _default_mapping_policy()


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
    precheck_allow: Optional[bool] = None,
    precheck_reason: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Core AI gate function. Returns a structured decision payload.

    precheck_allow/precheck_reason:
      - If upstream classifier says "block", we enforce block here
        so policy logs reflect real execution.
    """
    ts_ms = int(time.time() * 1000)
    mode_norm = _normalize_mode(mode)
    symbol_norm = str(symbol or "").upper()

    min_score, min_score_live, min_score_canary = _extract_min_scores(policy_cfg)

    enabled_modes = policy_cfg.get("enabled_modes") or policy_cfg.get("modes") or ["PAPER", "LIVE_CANARY", "LIVE_FULL"]
    enabled_modes = [str(m).upper() for m in enabled_modes if isinstance(m, (str, bytes))]

    missing_ok_modes = policy_cfg.get("missing_score_allow_modes", ["PAPER"])
    missing_ok_modes = [str(m).upper() for m in missing_ok_modes if isinstance(m, (str, bytes))]

    score: Optional[float]
    try:
        score = float(raw_score) if raw_score is not None else None
    except Exception:
        score = None

    allow = True
    reason = "ok"

    # 0) Upstream precheck hard block?
    if precheck_allow is False:
        allow = False
        r = str(precheck_reason or "classifier_block").strip()
        reason = f"precheck_block:{r}" if r else "precheck_block"

    # 1) Mode allowed?
    if allow and mode_norm not in enabled_modes:
        allow = False
        reason = "mode_not_enabled"

    # 2) Missing score handling
    if allow and score is None:
        if mode_norm not in missing_ok_modes:
            allow = False
            reason = "missing_score_blocked"

    # 3) Threshold check
    if allow and score is not None:
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
        "strategy_name": str(strategy_name),
        "symbol": symbol_norm,
        "account_label": str(account_label),
        "mode": mode_norm,
        "trade_id": trade_id,
        "score": score,
        "min_score": min_score,
        "min_score_live": min_score_live,
        "min_score_canary": min_score_canary,
        "policy_flags": {
            "enabled_modes": enabled_modes,
            "missing_score_allow_modes": missing_ok_modes,
        },
        "ts_ms": ts_ms,
    }

    if AI_POLICY_LOG_INCLUDE_FEATURES:
        decision["features"] = features if isinstance(features, dict) else {}
    else:
        decision["features"] = {}

    _append_policy_log(decision)
    return decision


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
    precheck_allow: Optional[bool] = None,
    precheck_reason: Optional[str] = None,
) -> bool:
    """
    Thin wrapper that runs ai_gate_decide and returns decision["allow"].
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
        precheck_allow=precheck_allow,
        precheck_reason=precheck_reason,
    )
    return bool(decision.get("allow", False))


def resolve_policy_cfg_for_strategy(policy: Dict[str, Any], strategy_name: str) -> Dict[str, Any]:
    """
    Public helper for callers (executor) to get merged per-strategy cfg.
    """
    try:
        return _resolve_policy_for_strategy(policy, strategy_name)
    except Exception:
        return _default_mapping_policy().get("__default__", {})
