#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Flashback — AI Gatekeeper (Phase 4)

Purpose:
- Evaluate memory stats and decide if we can trade
- Deterministic + explainable
"""

from __future__ import annotations

from typing import Any, Dict, Tuple


def evaluate_memory_gates(
    memory: Dict[str, Any],
    *,
    min_n_effective: int = 2,
    min_r_mean: float = 0.10,
    max_loss_rate: float = 0.60,
    min_abs_r_sum: float = 0.0,
) -> Tuple[bool, Dict[str, Any]]:
    stats = memory.get("stats") if isinstance(memory.get("stats"), dict) else {}
    n = int(stats.get("n") or 0)
    wins = int(stats.get("wins") or 0)
    losses = int(stats.get("losses") or 0)
    r_mean = stats.get("r_mean")
    r_sum = stats.get("r_sum")

    r_mean_f = float(r_mean) if r_mean is not None else 0.0
    r_sum_f = float(r_sum) if r_sum is not None else 0.0

    loss_rate = (float(losses) / float(n)) if n > 0 else 1.0

    observed = {
        "n": n,
        "wins": wins,
        "losses": losses,
        "loss_rate": loss_rate,
        "r_mean": r_mean_f,
        "r_sum": r_sum_f,
    }

    if n < min_n_effective:
        return False, {"reason": "insufficient_sample", "observed": observed}
    if loss_rate > max_loss_rate:
        return False, {"reason": "loss_rate_too_high", "observed": observed}
    if r_mean_f < min_r_mean:
        return False, {"reason": "r_mean_too_low", "observed": observed}
    if abs(r_sum_f) < float(min_abs_r_sum):
        return False, {"reason": "r_sum_too_small", "observed": observed}

    return True, {"reason": "passed", "observed": observed}
