#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Regime Scanner AI (Pipeline 0, shadow-mode)

Purpose:
- Classify market regime from lightweight indicators (adx, atr_pct, vol_z).
- Produce stable tags for downstream:
    - regime_tag: trend | range | high_vol | other
    - trend_state: TREND | RANGE | TRANSITION
    - volatility_state: LOW | NORMAL | EXPANDING
    - confidence: 0..1 (heuristic v1)
    - allowed_strategy_tags: list[str] (optional gating if strategies define tags)

This is NOT an execution gate by default. It's telemetry + optional soft filtering.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class RegimeResult:
    regime_tag: str
    trend_state: str
    volatility_state: str
    confidence: float
    tags: List[str]
    allowed_strategy_tags: List[str]
    features: Dict[str, Any]


_DEFAULT_POLICY: Dict[str, Any] = {
    # Trend strength
    "trend_adx": 1.25,  # your "ADX proxy" is small; tune later
    # Volatility thresholds
    "atr_low_pct": 0.35,
    "atr_high_pct": 1.25,
    # Volume shock (z-score)
    "vol_z_high": 1.8,
    # Allowed strategy tags by regime (only used if strategy has `tags`)
    "allowed_strategy_tags": {
        "trend": ["trend", "breakout", "momentum"],
        "range": ["range", "mean_reversion", "reversion"],
        "high_vol": ["breakout", "momentum", "scalp"],
        "other": [],
    },
}


def _clamp(x: float, lo: float, hi: float) -> float:
    try:
        if x < lo:
            return lo
        if x > hi:
            return hi
        return x
    except Exception:
        return lo


def classify_from_indicators(
    regime_ind: Dict[str, Any],
    *,
    policy: Optional[Dict[str, Any]] = None,
) -> RegimeResult:
    """
    Input:
      regime_ind = {"adx": float, "atr_pct": float, "vol_z": float}

    Output: RegimeResult with tags + stable features.
    """
    p = dict(_DEFAULT_POLICY)
    if isinstance(policy, dict):
        # shallow merge only (keep it simple and safe)
        for k, v in policy.items():
            p[k] = v

    adx = float(regime_ind.get("adx", 0.0) or 0.0)
    atr_pct = float(regime_ind.get("atr_pct", 0.0) or 0.0)
    vol_z = float(regime_ind.get("vol_z", 0.0) or 0.0)

    trend_adx = float(p.get("trend_adx", 1.25) or 1.25)
    atr_low = float(p.get("atr_low_pct", 0.35) or 0.35)
    atr_high = float(p.get("atr_high_pct", 1.25) or 1.25)
    vol_z_high = float(p.get("vol_z_high", 1.8) or 1.8)

    # Volatility state
    if atr_pct <= atr_low:
        vol_state = "LOW"
    elif atr_pct >= atr_high:
        vol_state = "EXPANDING"
    else:
        vol_state = "NORMAL"

    # Trend state
    if adx >= trend_adx:
        trend_state = "TREND"
    else:
        trend_state = "RANGE"

    # Regime tag (single label)
    if vol_state == "EXPANDING" or abs(vol_z) >= vol_z_high:
        regime_tag = "high_vol"
    else:
        regime_tag = "trend" if trend_state == "TREND" else "range"

    # Confidence heuristic
    # - trend confidence rises with adx
    # - high_vol confidence rises with atr_pct / |vol_z|
    # - range confidence rises when adx is low and vol normal/low
    conf = 0.5
    if regime_tag == "trend":
        conf = 0.45 + _clamp((adx / max(trend_adx, 1e-9)) * 0.35, 0.0, 0.35)
    elif regime_tag == "range":
        conf = 0.45 + _clamp((1.0 - (adx / max(trend_adx, 1e-9))) * 0.30, 0.0, 0.30)
        if vol_state == "LOW":
            conf += 0.05
    else:  # high_vol
        conf = 0.50
        conf += _clamp((atr_pct / max(atr_high, 1e-9)) * 0.25, 0.0, 0.25)
        conf += _clamp((abs(vol_z) / max(vol_z_high, 1e-9)) * 0.15, 0.0, 0.15)

    conf = _clamp(conf, 0.10, 0.95)

    tags: List[str] = [
        f"regime:{regime_tag}",
        f"trend_state:{trend_state.lower()}",
        f"vol:{vol_state.lower()}",
    ]

    allowed_map = p.get("allowed_strategy_tags", {}) if isinstance(p.get("allowed_strategy_tags"), dict) else {}
    allowed = allowed_map.get(regime_tag, [])
    if not isinstance(allowed, list):
        allowed = []

    features = {
        "adx": adx,
        "atr_pct": atr_pct,
        "vol_z": vol_z,
        "trend_adx": trend_adx,
        "atr_low_pct": atr_low,
        "atr_high_pct": atr_high,
        "vol_z_high": vol_z_high,
    }

    return RegimeResult(
        regime_tag=regime_tag,
        trend_state=trend_state,
        volatility_state=vol_state,
        confidence=float(conf),
        tags=tags,
        allowed_strategy_tags=[str(x) for x in allowed if str(x).strip()],
        features=features,
    )
