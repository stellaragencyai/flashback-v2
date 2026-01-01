#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Paper Report (promotion checker v1.1)

Purpose
-------
Read AI outcome logs from:

    state/ai_events/outcomes.jsonl

and compute, per strategy:

    - trade_count
    - win_count
    - winrate
    - avg_r (average R multiple)
    - expectancy_r (mean R)
    - cum_R series + max_drawdown_pct (in R space)
    - READY_FOR_CANARY vs NOT_READY based on config/strategies.yaml
      promotion_rules.

Usage
-----
From project root, human-readable table:

    python -m app.tools.ai_paper_report

JSON output for automation / promotion scripts:

    python -m app.tools.ai_paper_report --json

You should already have:
    - executor_v2 logging setup_context + features
    - PaperBroker emitting outcome_record events
    - ai_events_spine merging setups/outcomes into enriched records
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml  # type: ignore

try:
    from app.core.config import settings  # type: ignore
    ROOT: Path = settings.ROOT  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[1]


AI_EVENTS_DIR = ROOT / "state" / "ai_events"
OUTCOMES_PATH = AI_EVENTS_DIR / "outcomes.jsonl"
STRATEGIES_PATH = ROOT / "config" / "strategies.yaml"


def _load_strategies() -> Dict[str, Dict[str, Any]]:
    """
    Load strategies.yaml and return a mapping:

        strategy_name -> { raw_yaml_block + promotion_rules }
    """
    if not STRATEGIES_PATH.exists():
        raise FileNotFoundError(f"strategies.yaml not found at {STRATEGIES_PATH}")

    data = yaml.safe_load(STRATEGIES_PATH.read_text(encoding="utf-8")) or {}
    subs = data.get("subaccounts") or []

    out: Dict[str, Dict[str, Any]] = {}
    for sub in subs:
        if not isinstance(sub, dict):
            continue
        name = str(sub.get("name") or sub.get("account_label") or "UNKNOWN")
        promo = sub.get("promotion_rules") or {}
        out[name] = {
            "raw": sub,
            "promotion_rules": {
                "enabled": bool(promo.get("enabled", False)),
                "min_trades": int(promo.get("min_trades", 0)),
                "min_winrate": float(promo.get("min_winrate", 0.0)),
                "min_avg_r": float(promo.get("min_avg_r", 0.0)),
                "min_expectancy_r": float(promo.get("min_expectancy_r", 0.0)),
                "max_drawdown_pct": float(promo.get("max_drawdown_pct", 100.0)),
            },
        }
    return out


def _extract_r_and_win(
    outcome: Dict[str, Any]
) -> Tuple[Optional[float], Optional[bool], str]:
    """
    Extract R-multiple and win flag from an outcome event.

    Handles both:
        - enriched outcomes (event_type == "outcome_enriched")
        - raw outcome_record events (payload.[r_multiple, win])
    """
    etype = outcome.get("event_type")
    if etype == "outcome_enriched":
        stats = outcome.get("stats") or {}
        r = stats.get("r_multiple")
        win = stats.get("win")
        reason = str(stats.get("exit_reason") or outcome.get("exit_reason") or "")
        try:
            r_f = float(r) if r is not None else None
        except Exception:
            r_f = None
        return r_f, bool(win) if win is not None else None, reason

    # fallback: legacy outcome_record
    payload = outcome.get("payload") or {}
    r = payload.get("r_multiple")
    win = payload.get("win")
    reason = str(payload.get("exit_reason") or "")
    try:
        r_f = float(r) if r is not None else None
    except Exception:
        r_f = None
    return r_f, bool(win) if win is not None else None, reason


def _compute_drawdown_stats(r_series: List[float]) -> Tuple[float, float]:
    """
    Compute:
        - max_drawdown_R : worst peak-to-trough decline in cumulative R
        - max_drawdown_pct : same but as percentage of peak equity in R space

    Equity in R space is just cumulative sum of R over time.
    """
    if not r_series:
        return 0.0, 0.0

    equity = 0.0
    peak = 0.0
    max_dd = 0.0

    for r in r_series:
        equity += r
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd

    if peak <= 0:
        return max_dd, 0.0

    dd_pct = (max_dd / peak) * 100.0
    return max_dd, dd_pct


def _safe_mean(vals: List[float]) -> float:
    if not vals:
        return 0.0
    return sum(vals) / float(len(vals))


def _load_outcomes() -> List[Dict[str, Any]]:
    """
    Load all outcome events from outcomes.jsonl.
    """
    if not OUTCOMES_PATH.exists():
        return []

    out: List[Dict[str, Any]] = []
    with OUTCOMES_PATH.open("rb") as f:
        for raw in f:
            try:
                line = raw.decode("utf-8").strip()
            except Exception:
                continue
            if not line:
                continue
            try:
                evt = json.loads(line)
            except Exception:
                continue
            if not isinstance(evt, dict):
                continue
            etype = evt.get("event_type")
            if etype not in ("outcome_enriched", "outcome_record"):
                continue
            out.append(evt)
    return out


def build_strategy_stats(outcomes: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Aggregate stats per strategy_name (as recorded in outcome["strategy"]).
    """
    stats: Dict[str, Dict[str, Any]] = {}

    for evt in outcomes:
        strat = str(evt.get("strategy") or "UNKNOWN")
        r, win, _reason = _extract_r_and_win(evt)
        if strat not in stats:
            stats[strat] = {
                "trade_count": 0,
                "win_count": 0,
                "r_values": [],       # type: List[float]
                "r_nonzero": [],      # type: List[float]
                "r_series": [],       # for DD calc
            }

        s = stats[strat]
        s["trade_count"] += 1
        if win:
            s["win_count"] += 1
        if r is not None and not math.isnan(r):
            s["r_values"].append(r)
            if r != 0.0:
                s["r_nonzero"].append(r)
            s["r_series"].append(r)

    # finalize aggregates
    for strat, s in stats.items():
        n = s["trade_count"]
        wins = s["win_count"]
        r_vals = s["r_values"]
        winrate = (wins / n) if n > 0 else 0.0
        avg_r = _safe_mean(r_vals)
        expectancy_r = avg_r  # in R-space, expectancy is just mean(R)

        max_dd_R, max_dd_pct = _compute_drawdown_stats(s["r_series"])

        s["winrate"] = winrate
        s["avg_r"] = avg_r
        s["expectancy_r"] = expectancy_r
        s["max_drawdown_R"] = max_dd_R
        s["max_drawdown_pct"] = max_dd_pct

    return stats


def evaluate_promotions(
    stats: Dict[str, Dict[str, Any]],
    strat_cfg: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """
    Compare per-strategy stats vs promotion_rules and attach:

        "promotion_status": "READY_FOR_CANARY" | "NOT_READY" | "NO_RULES"
        "promotion_reason": short explanation
    """
    result: Dict[str, Dict[str, Any]] = {}

    for strat_name, s in stats.items():
        cfg = strat_cfg.get(strat_name)
        if not cfg:
            # strategy in logs but not in config, just copy stats
            result[strat_name] = {
                **s,
                "promotion_status": "NO_RULES",
                "promotion_reason": "strategy not found in strategies.yaml",
            }
            continue

        rules = cfg.get("promotion_rules") or {}
        enabled = bool(rules.get("enabled", False))
        if not enabled:
            result[strat_name] = {
                **s,
                "promotion_status": "NO_RULES",
                "promotion_reason": "promotion_rules.disabled",
            }
            continue

        min_trades = int(rules.get("min_trades", 0))
        min_winrate = float(rules.get("min_winrate", 0.0))
        min_avg_r = float(rules.get("min_avg_r", 0.0))
        min_expectancy_r = float(rules.get("min_expectancy_r", 0.0))
        max_dd_pct = float(rules.get("max_drawdown_pct", 100.0))

        trade_count = s.get("trade_count", 0)
        winrate = s.get("winrate", 0.0)
        avg_r = s.get("avg_r", 0.0)
        expectancy_r = s.get("expectancy_r", 0.0)
        dd_pct = s.get("max_drawdown_pct", 0.0)

        reasons: List[str] = []

        if trade_count < min_trades:
            reasons.append(f"trades {trade_count} < min {min_trades}")
        if winrate < min_winrate:
            reasons.append(f"winrate {winrate:.3f} < min {min_winrate:.3f}")
        if avg_r < min_avg_r:
            reasons.append(f"avg_r {avg_r:.3f} < min {min_avg_r:.3f}")
        if expectancy_r < min_expectancy_r:
            reasons.append(
                f"expectancy_r {expectancy_r:.3f} < min {min_expectancy_r:.3f}"
            )
        if dd_pct > max_dd_pct:
            reasons.append(
                f"max_dd_pct {dd_pct:.1f}% > allowed {max_dd_pct:.1f}%"
            )

        if reasons:
            status = "NOT_READY"
            reason = "; ".join(reasons)
        else:
            status = "READY_FOR_CANARY"
            reason = "all promotion_rules satisfied"

        result[strat_name] = {
            **s,
            "promotion_status": status,
            "promotion_reason": reason,
        }

    # Also ensure strategies that exist in config but have no trades are included
    for strat_name, cfg in strat_cfg.items():
        if strat_name in result:
            continue
        rules = cfg.get("promotion_rules") or {}
        enabled = bool(rules.get("enabled", False))
        if not enabled:
            status = "NO_RULES"
            reason = "promotion_rules.disabled or missing"
        else:
            status = "NOT_READY"
            reason = "no trades logged yet"
        result[strat_name] = {
            "trade_count": 0,
            "win_count": 0,
            "winrate": 0.0,
            "avg_r": 0.0,
            "expectancy_r": 0.0,
            "max_drawdown_R": 0.0,
            "max_drawdown_pct": 0.0,
            "promotion_status": status,
            "promotion_reason": reason,
        }

    return result


def _fmt_pct(x: float) -> str:
    return f"{x*100:.1f}%"


def _fmt_r(x: float) -> str:
    return f"{x:+.3f}"


def print_report(
    evaluated: Dict[str, Dict[str, Any]],
) -> None:
    """
    Pretty-print a compact promotion report per strategy.
    """
    print("\n=== AI Paper Report — Strategy Promotion View ===\n")

    header = (
        f"{'Strategy':<20}  "
        f"{'Trades':>7}  "
        f"{'Win%':>7}  "
        f"{'Avg R':>8}  "
        f"{'Exp R':>8}  "
        f"{'MaxDD%':>8}  "
        f"{'Status':>16}"
    )
    print(header)
    print("-" * len(header))

    # Sort by status then by name
    def sort_key(item: Tuple[str, Dict[str, Any]]) -> Tuple[int, str]:
        name, s = item
        status = s.get("promotion_status", "NO_RULES")
        order = {"READY_FOR_CANARY": 0, "NOT_READY": 1, "NO_RULES": 2}.get(status, 3)
        return (order, name)

    for strat_name, s in sorted(evaluated.items(), key=sort_key):
        n = s.get("trade_count", 0)
        winrate = float(s.get("winrate", 0.0))
        avg_r = float(s.get("avg_r", 0.0))
        exp_r = float(s.get("expectancy_r", 0.0))
        dd_pct = float(s.get("max_drawdown_pct", 0.0))
        status = s.get("promotion_status", "NO_RULES")

        print(
            f"{strat_name:<20}  "
            f"{n:7d}  "
            f"{winrate*100:6.1f}%  "
            f"{avg_r:8.3f}  "
            f"{exp_r:8.3f}  "
            f"{dd_pct:8.1f}%  "
            f"{status:>16}"
        )

    print("\nLegend:")
    print("  READY_FOR_CANARY  = Meets all promotion_rules → candidate to flip to LIVE_CANARY")
    print("  NOT_READY         = Has trades but fails one or more promotion_rules")
    print("  NO_RULES          = promotion_rules.disabled or missing in strategies.yaml\n")


# ---------------------------------------------------------------------------
# Public library entrypoint for other bots / tools
# ---------------------------------------------------------------------------

def evaluate_strategies() -> Dict[str, Dict[str, Any]]:
    """
    High-level helper for other modules:

        - Loads strategies.yaml
        - Loads outcomes.jsonl
        - Builds per-strategy stats
        - Evaluates promotion readiness

    Returns:
        Dict[strategy_name, Dict[str, Any]]
    """
    strat_cfg_raw = _load_strategies()

    # Flatten to strategy_name -> merged config dict
    strat_cfg: Dict[str, Dict[str, Any]] = {}
    for name, blob in strat_cfg_raw.items():
        merged = dict(blob.get("raw") or {})
        merged["promotion_rules"] = blob.get("promotion_rules") or {}
        strat_cfg[name] = merged

    outcomes = _load_outcomes()
    stats = build_strategy_stats(outcomes)
    evaluated = evaluate_promotions(stats, strat_cfg)
    return evaluated


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Flashback — AI Paper Report (strategy promotion view)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output machine-readable JSON instead of a human table.",
    )

    args = parser.parse_args(argv)

    print(f"Root: {ROOT}")
    print(f"Reading outcomes from: {OUTCOMES_PATH}")
    print(f"Reading strategies from: {STRATEGIES_PATH}")

    try:
        evaluated = evaluate_strategies()
    except FileNotFoundError as e:
        print(f"\nERROR: {e}\n")
        return

    if not evaluated:
        print(
            "\nNo strategies or outcome events found. "
            "Ensure LEARN_DRY + PaperBroker + ai_events_spine are running.\n"
        )
        return

    if args.json:
        print(json.dumps(evaluated, indent=2, sort_keys=True))
    else:
        print_report(evaluated)


if __name__ == "__main__":
    main()
