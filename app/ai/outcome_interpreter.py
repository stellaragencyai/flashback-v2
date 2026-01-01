#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Outcome Interpreter (v2.1)

Purpose
-------
Deterministic interpreter that converts a *finalized* trade close into:
- normalized outcome label (win/loss/scratch/abort/invalid)
- bounded learning signal: reward ∈ [-1.0, +1.0]
- penalty (0..1), confidence_signal (0..1), memory_weight (0..1)

Hard invariants
---------------
- Finality-only: OPEN/PARTIAL outcomes are ignored (reward=0, memory_weight=0)
- Structural failure overrides PnL: any rule breach/kill-switch/liquidation => reward = -1
- Reward bounded and monotonic in R-multiple, not raw PnL dollars
- Time-adjusted: longer exposure mildly reduces magnitude (never boosts)

This module:
- Does NOT write files
- Does NOT execute trades
- Is replay-safe and pure
"""

from __future__ import annotations

from typing import Any, Dict, Optional, List
import math
import re


# ---------------------------------------------------------------------
# Close reason normalization map
# ---------------------------------------------------------------------

_CLOSE_REASON_MAP: Dict[str, Dict[str, Any]] = {
    # Take profit variants
    "tp": {"result": "win", "win": True, "exit_quality": "good", "confidence": 0.95, "tags": ["tp"]},
    "take_profit": {"result": "win", "win": True, "exit_quality": "good", "confidence": 0.95, "tags": ["tp"]},
    "tp_hit": {"result": "win", "win": True, "exit_quality": "good", "confidence": 0.95, "tags": ["tp"]},
    "tp_forced": {"result": "win", "win": True, "exit_quality": "good", "confidence": 0.90, "tags": ["tp", "forced"]},

    # Stop loss variants
    "sl": {"result": "loss", "win": False, "exit_quality": "poor", "confidence": 0.95, "tags": ["sl"]},
    "stop_loss": {"result": "loss", "win": False, "exit_quality": "poor", "confidence": 0.95, "tags": ["sl"]},
    "sl_hit": {"result": "loss", "win": False, "exit_quality": "poor", "confidence": 0.95, "tags": ["sl"]},

    # Neutral / scratch
    "scratch": {"result": "scratch", "win": None, "exit_quality": "neutral", "confidence": 0.80, "tags": ["scratch"]},
    "breakeven": {"result": "scratch", "win": None, "exit_quality": "neutral", "confidence": 0.80, "tags": ["scratch"]},
    "be": {"result": "scratch", "win": None, "exit_quality": "neutral", "confidence": 0.75, "tags": ["scratch"]},

    # Forced / safety closes (final, but treated conservatively)
    "manual": {"result": "abort", "win": None, "exit_quality": "neutral", "confidence": 0.40, "tags": ["manual", "test"]},
    "manual_close": {"result": "abort", "win": None, "exit_quality": "neutral", "confidence": 0.40, "tags": ["manual", "test"]},
    "timeout": {"result": "abort", "win": None, "exit_quality": "forced", "confidence": 0.60, "tags": ["timeout"]},
    "force_close": {"result": "abort", "win": None, "exit_quality": "forced", "confidence": 0.70, "tags": ["force_close"]},
    "breaker": {"result": "abort", "win": None, "exit_quality": "forced", "confidence": 0.70, "tags": ["policy", "breaker"]},
    "kill_switch": {"result": "abort", "win": None, "exit_quality": "forced", "confidence": 0.70, "tags": ["policy", "kill_switch"]},
    "reset": {"result": "abort", "win": None, "exit_quality": "forced", "confidence": 0.50, "tags": ["system", "reset"]},

    # Structural failure / catastrophic outcomes
    "liquidation": {"result": "loss", "win": False, "exit_quality": "forced", "confidence": 0.98, "tags": ["liquidation", "structural_fail"]},
    "risk_violation": {"result": "abort", "win": None, "exit_quality": "forced", "confidence": 0.85, "tags": ["risk", "structural_fail"]},
    "gate_bypass": {"result": "abort", "win": None, "exit_quality": "forced", "confidence": 0.85, "tags": ["policy", "structural_fail"]},
}

_VERSION = "2.1"


# ---------------------------------------------------------------------
# Helpers (pure)
# ---------------------------------------------------------------------

_TOKEN_SPLIT_RE = re.compile(r"[:|,;]+")

def _norm_reason(close_reason: Optional[str]) -> Optional[str]:
    """
    Normalize close_reason into a stable key.
    Handles variants like:
      "tp", "TP_HIT:ladder", "reason:sl_hit|extra", " tp "
    """
    if close_reason is None:
        return None
    s = str(close_reason).strip().lower()
    if not s:
        return None

    # strip common prefixes
    for prefix in ("reason=", "reason:", "close_reason=", "close_reason:"):
        if s.startswith(prefix):
            s = s[len(prefix):].strip()

    # keep first token before any delimiter
    s = _TOKEN_SPLIT_RE.split(s)[0].strip()

    # collapse common shorthand/aliases
    if s in ("t/p", "takeprofit", "take-profit"):
        s = "take_profit"
    if s in ("s/l", "stoploss", "stop-loss"):
        s = "stop_loss"

    return s or None


def _as_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        x = float(v)
        if not math.isfinite(x):
            return None
        return x
    except Exception:
        return None


def _as_bool(v: Any) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("true", "1", "yes", "y", "on"):
        return True
    if s in ("false", "0", "no", "n", "off"):
        return False
    return None


def _is_final(close_reason_key: Optional[str], extra: Optional[Dict[str, Any]]) -> bool:
    """
    Finality-only, conservative:
    - explicit OPEN/PARTIAL => not final
    - explicit CLOSED/DONE/FILLED/FINAL => final
    - boolean flags honored
    - fallback: require known final reason OR explicit close timestamp fields
    """
    if extra:
        # explicit flags
        for k in ("is_final", "final", "is_closed", "closed"):
            if k in extra:
                b = _as_bool(extra.get(k))
                if b is not None:
                    return bool(b)

        status = str(extra.get("status", "")).strip().upper()
        if status in ("OPEN", "PARTIAL", "PENDING", "WORKING"):
            return False
        if status in ("CLOSED", "FILLED", "DONE", "FINAL", "CLOSE", "CLOSE_FILLED"):
            return True

        # explicit timestamps commonly present on final rows
        for k in ("close_ts", "exit_ts", "closed_ts", "closed_at_ms", "close_time_ms"):
            if extra.get(k) is not None:
                return True

    # fallback: if we have a mapped reason, assume final
    if close_reason_key and close_reason_key in _CLOSE_REASON_MAP:
        return True

    # unknown => not final (safer than poisoning learning)
    return False


def _detect_structural_fail(extra: Optional[Dict[str, Any]], tags: List[str]) -> bool:
    if "structural_fail" in tags or "liquidation" in tags:
        return True
    if not extra:
        return False

    # direct flags
    for k in (
        "rule_breach",
        "policy_violation",
        "gate_bypass",
        "risk_violation",
        "kill_switch",
        "breaker",
        "forced_flatten",
        "emergency_close",
        "liquidation",
        "order_reject_loop",
        "ws_desync",
        "execution_race",
        "invalid_state",
    ):
        if bool(extra.get(k)):
            return True

    # scan text/tags blobs for structural tokens
    blob = " ".join([
        str(extra.get("reason", "")),
        str(extra.get("close_reason", "")),
        str(extra.get("final_status", "")),
        str(extra.get("status", "")),
        " ".join(map(str, extra.get("tags", []) or [])),
    ]).lower()

    tokens = (
        "bypass", "violation", "liquidat", "kill", "breaker",
        "forced_flatten", "emergency", "invalid_state", "race", "desync",
        "risk", "gate"
    )
    return any(tok in blob for tok in tokens)


def _quality_weight(exit_quality: str) -> float:
    q = (exit_quality or "").lower()
    return {
        "good": 1.00,
        "optimal": 1.00,
        "neutral": 0.65,
        "forced": 0.55,
        "poor": 0.45,
        "failed": 0.45,
        "unknown": 0.50,
    }.get(q, 0.50)


def _time_factor(exposure_sec: Optional[float]) -> float:
    """
    Mild penalty for holding risk longer. Never boosts short holds.
    Penalty is intentionally gentle so it nudges capital-efficiency without dominating.
    """
    if exposure_sec is None:
        return 1.0
    sec = max(0.0, float(exposure_sec))
    hours = sec / 3600.0
    return 1.0 / (1.0 + 0.25 * math.log1p(max(0.0, hours)))


def _infer_r_multiple(pnl: Optional[float], extra: Optional[Dict[str, Any]]) -> Optional[float]:
    """
    Prefer explicit r_multiple.
    Else use pnl / risk_usd.
    Else use pnl_pct as weak proxy.
    Else last-resort: pnl/100 (VERY weak; keeps magnitudes small).
    """
    if extra:
        r = _as_float(extra.get("r_multiple"))
        if r is not None:
            return r

        risk_usd = _as_float(extra.get("risk_usd"))
        if pnl is not None and risk_usd is not None and risk_usd > 0:
            return float(pnl) / float(risk_usd)

        pnl_pct = _as_float(extra.get("pnl_pct"))
        if pnl_pct is not None:
            return pnl_pct / 100.0

    if pnl is None:
        return None

    # last resort: weak normalization, bounded later
    return float(pnl) / 100.0


def _bounded_reward_from_r(r: float) -> float:
    # tanh maps smoothly to (-1, +1), monotonic, bounded
    return float(math.tanh(0.85 * r))


# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------

def interpret_outcome(
    *,
    close_reason: Optional[str],
    pnl: Optional[float] = None,
    extra: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Interpret a trade close into normalized outcome + learning signals.
    Pure & deterministic.
    """
    reason_key = _norm_reason(close_reason)
    base = _CLOSE_REASON_MAP.get(reason_key) if reason_key else None

    # Finality-only (hard invariant)
    if not _is_final(reason_key, extra):
        return {
            "result": "invalid",
            "win": None,
            "intent_resolved": False,
            "exit_quality": "unknown",
            "confidence": 0.0,
            "tags": ["non_final_ignored"],
            "reason": "non_final",
            "version": _VERSION,
            "final": False,
            "structural_fail": False,
            "reward": 0.0,
            "penalty": 0.0,
            "confidence_signal": 0.0,
            "memory_weight": 0.0,
        }

    # Unknown reason: infer label from pnl (but keep low confidence)
    if base is None:
        if pnl is not None:
            if pnl > 0:
                result, win = "win", True
            elif pnl < 0:
                result, win = "loss", False
            else:
                result, win = "scratch", None

            base = {
                "result": result,
                "win": win,
                "exit_quality": "unknown",
                "confidence": 0.30,
                "tags": ["unknown_reason", "pnl_inferred"],
            }
            reason_key = f"unmapped_close_reason:{reason_key or 'none'}"
        else:
            # no reason, no pnl => no learning
            return {
                "result": "invalid",
                "win": None,
                "intent_resolved": False,
                "exit_quality": "unknown",
                "confidence": 0.0,
                "tags": ["invalid"],
                "reason": f"unknown_reason:{reason_key or 'none'}",
                "version": _VERSION,
                "final": True,
                "structural_fail": False,
                "reward": 0.0,
                "penalty": 0.0,
                "confidence_signal": 0.0,
                "memory_weight": 0.0,
            }

    tags: List[str] = list(base.get("tags") or [])
    exit_quality = str(base.get("exit_quality") or "unknown")
    base_conf = float(base.get("confidence") or 0.0)

    # intent_resolved means "this was a real win/loss signal", not scratch/abort
    intent_resolved = base.get("result") in ("win", "loss")

    # Structural failure overrides everything (hard invariant)
    structural_fail = _detect_structural_fail(extra, tags)
    if structural_fail and "structural_fail" not in tags:
        tags.append("structural_fail")

    # Compute bounded reward
    reward = 0.0
    if intent_resolved:
        r = _infer_r_multiple(pnl, extra)
        if r is not None:
            # Optional regret modeling if fields exist (never required)
            if extra:
                mfe = _as_float(extra.get("max_favorable_r"))
                mae = _as_float(extra.get("max_adverse_r"))

                # missed upside (win that could have been better)
                if mfe is not None and math.isfinite(mfe):
                    missed = max(0.0, float(mfe) - float(r))
                    r -= 0.30 * math.tanh(0.5 * missed)

                # excess adverse excursion (drawdown severity)
                if mae is not None and math.isfinite(mae):
                    excess = max(0.0, abs(float(mae)) - abs(float(r)))
                    r -= 0.20 * math.tanh(0.5 * excess)

            reward = _bounded_reward_from_r(r)

    # Time adjustment (never boosts)
    exposure_sec = _as_float(extra.get("exposure_sec")) if extra else None
    reward *= _time_factor(exposure_sec)

    # Structural failure override (after time factor)
    if structural_fail:
        reward = -1.0

    # Clamp + companions
    reward = float(max(-1.0, min(1.0, reward)))
    penalty = float(max(0.0, -reward))

    # Confidence signal: base confidence, down-weight aborts/scratch/unknown
    conf_sig = base_conf
    if base.get("result") == "scratch":
        conf_sig *= 0.60
    if base.get("result") == "abort":
        conf_sig *= 0.40
    if "unknown_reason" in tags:
        conf_sig *= 0.55
    if structural_fail:
        conf_sig = max(conf_sig, 0.80)  # high certainty this was bad behavior
    conf_sig = float(max(0.0, min(1.0, conf_sig)))

    # Memory weight: quality * confidence * |reward|
    mem_w = _quality_weight(exit_quality) * conf_sig * abs(reward)
    mem_w = float(max(0.0, min(1.0, mem_w)))

    return {
        # legacy fields (keep stable for existing readers)
        "result": base["result"],
        "win": base["win"],
        "intent_resolved": intent_resolved,
        "exit_quality": exit_quality,
        "confidence": float(base_conf),
        "tags": tags,
        "reason": reason_key,
        "version": _VERSION,

        # learning fields
        "final": True,
        "structural_fail": structural_fail,
        "reward": reward,
        "penalty": penalty,
        "confidence_signal": conf_sig,
        "memory_weight": mem_w,
    }
