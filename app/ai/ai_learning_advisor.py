#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Learning Advisor v1 (Phase 6)

Converts Phase 6 learning stats into a deterministic, explainable advisory contract.

Hard rules:
- READ-ONLY over Phase 6 derived learning.sqlite (and Phase 5 indirectly)
- NO execution hooks
- Deterministic: same inputs -> same advisory bytes
- Conservative: insufficient data => NEUTRAL

Recommendations:
- INSUFFICIENT_DATA => NEUTRAL (confidence=0)
- drift_flag => AVOID (unless insufficient, then still NEUTRAL)
- winsor_mean_r >= favor_threshold and sufficient => FAVOR
- winsor_mean_r <= avoid_threshold and sufficient => AVOID
- else => NEUTRAL
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class AdvisoryRow:
    schema_version: int
    built_ts_ms: int

    policy_hash: str
    memory_fingerprint: str
    symbol: str
    timeframe: str
    setup_type: str

    sample_size: int
    confidence_state: str
    confidence: float

    expected_r: Optional[float]
    winsor_mean_r: Optional[float]
    median_r: Optional[float]
    mad_r: Optional[float]
    win_rate: Optional[float]

    drift_flag: bool
    drift_reason: str

    recommendation: str
    reasons: List[str]


def _fmt(x: Any) -> str:
    if x is None:
        return "n/a"
    try:
        return f"{float(x):.4f}".rstrip("0").rstrip(".")
    except Exception:
        return str(x)


def recommend(
    *,
    n: int,
    confidence_state: str,
    confidence: float,
    winsor_mean_r: Optional[float],
    expected_r: Optional[float],
    win_rate: Optional[float],
    drift_flag: bool,
    drift_reason: str,
    favor_threshold: float,
    avoid_threshold: float,
) -> Tuple[str, List[str]]:
    """
    Deterministic, explainable recommendation.
    """
    reasons: List[str] = []

    # Always record key metrics (even if insufficient)
    reasons.append(f"n={n}")
    reasons.append(f"confidence_state={confidence_state}")
    reasons.append(f"confidence={_fmt(confidence)}")
    if winsor_mean_r is not None:
        reasons.append(f"winsor_mean_r={_fmt(winsor_mean_r)}")
    if expected_r is not None:
        reasons.append(f"mean_r={_fmt(expected_r)}")
    if win_rate is not None:
        reasons.append(f"win_rate={_fmt(win_rate)}")

    # Hard gate: insufficient data => NEUTRAL no matter what
    if confidence_state == "INSUFFICIENT_DATA" or n <= 0:
        reasons.append("gate=INSUFFICIENT_DATA=>NEUTRAL")
        return ("NEUTRAL", reasons)

    # Drift penalty (conservative)
    if drift_flag:
        if drift_reason:
            reasons.append(f"drift={drift_reason}")
        else:
            reasons.append("drift=flagged")
        reasons.append("rule=DRIFT=>AVOID")
        return ("AVOID", reasons)

    # If robust metric missing, refuse to pretend
    if winsor_mean_r is None:
        reasons.append("gate=winsor_mean_r_missing=>NEUTRAL")
        return ("NEUTRAL", reasons)

    # Threshold rules
    if winsor_mean_r >= favor_threshold:
        reasons.append(f"rule=winsor_mean_r>={_fmt(favor_threshold)}=>FAVOR")
        return ("FAVOR", reasons)

    if winsor_mean_r <= avoid_threshold:
        reasons.append(f"rule=winsor_mean_r<={_fmt(avoid_threshold)}=>AVOID")
        return ("AVOID", reasons)

    reasons.append("rule=between_thresholds=>NEUTRAL")
    return ("NEUTRAL", reasons)


def row_to_dict(r: AdvisoryRow) -> Dict[str, Any]:
    return {
        "schema_version": r.schema_version,
        "built_ts_ms": r.built_ts_ms,
        "policy_hash": r.policy_hash,
        "memory_fingerprint": r.memory_fingerprint,
        "symbol": r.symbol,
        "timeframe": r.timeframe,
        "setup_type": r.setup_type,
        "sample_size": r.sample_size,
        "confidence_state": r.confidence_state,
        "confidence": r.confidence,
        "expected_R": r.expected_r,
        "winsor_mean_R": r.winsor_mean_r,
        "median_R": r.median_r,
        "mad_R": r.mad_r,
        "win_rate": r.win_rate,
        "drift_flag": bool(r.drift_flag),
        "drift_reason": r.drift_reason,
        "recommendation": r.recommendation,
        "reasons": list(r.reasons),
    }


from app.ai.ai_live_state_adapter import LiveAIState
from app.ai.ai_evolution_engine import run_evolution_cycle

def run_evolution_if_ready():
    state = LiveAIState()
    decisions = run_evolution_cycle(state)
    return decisions



from app.ai.capital_flow_engine import compute_capital_flows
from app.ai.capital_flow_emitter import emit_capital_flows

def run_capital_flow(evolution_decisions):
    from app.ai.ai_policy_stats import load_policy_stats
    stats = load_policy_stats()
    flows = compute_capital_flows(evolution_decisions, stats)
    emit_capital_flows(flows)



from app.ai.capital_flow_engine import compute_capital_flows
from app.ai.capital_flow_emitter import emit_capital_flows

def run_capital_flow(evolution_decisions):
    from app.ai.ai_policy_stats import load_policy_stats
    stats = load_policy_stats()
    flows = compute_capital_flows(evolution_decisions, stats)
    emit_capital_flows(flows)

