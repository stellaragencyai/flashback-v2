#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Metrics Aggregator (strategies snapshot v1)

Role
----
Scan AI setup/outcome event logs and build a clean per-strategy snapshot:

    state/ai_metrics/strategies_snapshot.json

For each strategy (e.g., Sub2_Breakout on flashback02), we compute:

    - sample size: n_trades, win_rate, avg_R, median_R, max_R, min_R
    - risk: max_drawdown_R (cumulative R curve), avg_risk_usd
    - AI gate: avg_score, pass_rate, win_rate_when_allowed
    - metadata: account_label, strategy_name, ai_profile, mode
    - score: 0–100 rating + tier (S/A/B/C/D)

This snapshot is the single source of truth for:

    - Telegram /ai_status style summaries
    - Future web dashboard tables
"""

from __future__ import annotations

import json
import statistics
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from app.core.config import settings  # type: ignore
    ROOT: Path = settings.ROOT  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[2]


AI_EVENTS_DIR = ROOT / "state" / "ai_events"
SETUPS_PATH = AI_EVENTS_DIR / "setups.jsonl"
OUTCOMES_PATH = AI_EVENTS_DIR / "outcomes.jsonl"

METRICS_DIR = ROOT / "state" / "ai_metrics"
METRICS_DIR.mkdir(parents=True, exist_ok=True)
SNAPSHOT_PATH = METRICS_DIR / "strategies_snapshot.json"

# -----------------------------------------------------------------------------
# Logging (fall back gracefully)
# -----------------------------------------------------------------------------

try:
    from app.core.logger import get_logger  # type: ignore
except Exception:
    try:
        from app.core.log import get_logger  # type: ignore
    except Exception:  # pragma: no cover
        import logging
        import sys

        def get_logger(name: str) -> "logging.Logger":  # type: ignore
            logger_ = logging.getLogger(name)
            if not logger_.handlers:
                handler = logging.StreamHandler(sys.stdout)
                fmt = logging.Formatter(
                    "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
                )
                handler.setFormatter(fmt)
                logger_.addHandler(handler)
            logger_.setLevel(logging.INFO)
            return logger_

log = get_logger("ai_metrics_aggregator")


# -----------------------------------------------------------------------------
# Data structures
# -----------------------------------------------------------------------------

@dataclass
class StrategyMetrics:
    account_label: str
    strategy_name: str
    ai_profile: Optional[str]

    # sample stats
    n_trades: int = 0
    n_wins: int = 0
    sum_R: float = 0.0
    sum_risk_usd: float = 0.0
    r_values: List[float] = None  # type: ignore

    # AI gate stats
    ai_scores: List[float] = None  # type: ignore
    ai_allowed_count: int = 0
    ai_allowed_wins: int = 0

    # equity curve in R for max drawdown
    equity_curve_R: List[float] = None  # type: ignore

    # derived
    win_rate: float = 0.0
    avg_R: float = 0.0
    median_R: Optional[float] = None
    max_R: Optional[float] = None
    min_R: Optional[float] = None
    max_dd_R: float = 0.0
    avg_risk_usd: float = 0.0

    ai_avg_score: Optional[float] = None
    ai_pass_rate: float = 0.0
    ai_win_rate_when_allowed: Optional[float] = None

    score: int = 0
    tier: str = "D"

    mode: Optional[str] = None  # PAPER / LIVE_CANARY / LIVE_FULL etc.

    def __post_init__(self) -> None:
        if self.r_values is None:
            self.r_values = []
        if self.ai_scores is None:
            self.ai_scores = []
        if self.equity_curve_R is None:
            self.equity_curve_R = []

    def add_trade(
        self,
        r_multiple: Optional[float],
        pnl_usd: Optional[float],
        risk_usd: Optional[float],
        win: Optional[bool],
        ai_score: Optional[float],
        ai_allowed: Optional[bool],
    ) -> None:
        # Only count trades that actually have an R
        if r_multiple is None:
            return

        self.n_trades += 1
        self.sum_R += r_multiple
        self.r_values.append(r_multiple)

        if win is True:
            self.n_wins += 1

        if risk_usd is not None:
            self.sum_risk_usd += float(risk_usd)

        # AI gate stats
        if ai_score is not None:
            self.ai_scores.append(ai_score)

        if ai_allowed is True:
            self.ai_allowed_count += 1
            if win is True:
                self.ai_allowed_wins += 1

        # Equity curve in R for max drawdown
        prev = self.equity_curve_R[-1] if self.equity_curve_R else 0.0
        self.equity_curve_R.append(prev + r_multiple)

    def finalize(self) -> None:
        if self.n_trades == 0:
            return

        self.win_rate = self.n_wins / self.n_trades if self.n_trades > 0 else 0.0
        self.avg_R = self.sum_R / self.n_trades if self.n_trades > 0 else 0.0

        if self.r_values:
            try:
                self.median_R = statistics.median(self.r_values)
            except Exception:
                self.median_R = None
            self.max_R = max(self.r_values)
            self.min_R = min(self.r_values)

        if self.sum_risk_usd > 0 and self.n_trades > 0:
            self.avg_risk_usd = self.sum_risk_usd / self.n_trades

        # AI stats
        if self.ai_scores:
            self.ai_avg_score = sum(self.ai_scores) / len(self.ai_scores)
        if self.n_trades > 0:
            self.ai_pass_rate = self.ai_allowed_count / self.n_trades
        if self.ai_allowed_count > 0:
            self.ai_win_rate_when_allowed = (
                self.ai_allowed_wins / self.ai_allowed_count
            )

        # max drawdown in R
        self.max_dd_R = _compute_max_drawdown(self.equity_curve_R)

        # final score + tier
        self.score, self.tier = _score_strategy(self)


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _compute_max_drawdown(curve: List[float]) -> float:
    """
    Compute max drawdown of a cumulative R curve.
    Returns a negative number (e.g., -3.4) or 0.0 if no drawdown.
    """
    if not curve:
        return 0.0
    peak = curve[0]
    max_dd = 0.0
    for x in curve:
        if x > peak:
            peak = x
        dd = x - peak
        if dd < max_dd:
            max_dd = dd
    return float(max_dd)


def _score_strategy(m: StrategyMetrics) -> Tuple[int, str]:
    """
    Turn raw metrics into a single 0–100 rating + tier.

    This is intentionally simple and interpretable.
    """
    if m.n_trades == 0:
        return 0, "D"

    # Base from expectancy in R
    #   > 0.3 R avg → strong
    #   0.1–0.3 R   → decent
    #   < 0.0 R     → trash
    base = 50.0 + (m.avg_R * 60.0)  # rough scaling

    # Sample size bonus (diminishing)
    #  0–50 trades: +0 → +5
    #  50–200:      +5 → +15
    #  200+:        +15 → +20
    n = m.n_trades
    if n <= 50:
        sample_bonus = (n / 50.0) * 5.0
    elif n <= 200:
        sample_bonus = 5.0 + ((n - 50) / 150.0) * 10.0
    else:
        sample_bonus = 15.0 + min((n - 200) / 300.0, 1.0) * 5.0
    base += sample_bonus

    # AI gate bonus: if AI is selective and accurate, reward it
    if m.ai_avg_score is not None:
        base += (m.ai_avg_score - 0.5) * 20.0  # scores around 0.5 neutral
    if m.ai_pass_rate > 0 and m.ai_win_rate_when_allowed is not None:
        # if allowed trades win much more than base win_rate, reward
        diff = m.ai_win_rate_when_allowed - m.win_rate
        base += diff * 40.0

    # Drawdown penalty
    # A big negative max_dd_R pulls score down
    if m.max_dd_R < 0:
        base += m.max_dd_R * 10.0  # e.g. -3R dd → -30 pts

    # Clip to [0, 100]
    score = int(max(0.0, min(100.0, base)))

    # Tier boundaries
    if score >= 85:
        tier = "S"
    elif score >= 70:
        tier = "A"
    elif score >= 55:
        tier = "B"
    elif score >= 40:
        tier = "C"
    else:
        tier = "D"

    return score, tier


# -----------------------------------------------------------------------------
# IO helpers
# -----------------------------------------------------------------------------

def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("rb") as f:
        for raw in f:
            try:
                line = raw.decode("utf-8").strip()
            except Exception:
                continue
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows


# -----------------------------------------------------------------------------
# Core aggregation
# -----------------------------------------------------------------------------

def build_snapshot() -> Dict[str, Any]:
    """
    Build the full snapshot dict:

        {
          "version": 1,
          "strategies": [
             { ...metrics for Sub1... },
             { ...metrics for Sub2... },
             ...
          ]
        }
    """
    log.info("Loading AI setup/outcome events...")
    setups = _read_jsonl(SETUPS_PATH)
    outcomes = _read_jsonl(OUTCOMES_PATH)

    log.info("Loaded %d setups, %d outcomes", len(setups), len(outcomes))

    setups_by_id: Dict[str, Dict[str, Any]] = {}
    for row in setups:
        tid = str(row.get("trade_id") or row.get("id") or "")
        if not tid:
            continue
        setups_by_id[tid] = row

    metrics_map: Dict[Tuple[str, str], StrategyMetrics] = {}

    for out in outcomes:
        trade_id = str(out.get("trade_id") or "")
        if not trade_id:
            continue

        setup = setups_by_id.get(trade_id, {})

        # Strategy + account_label resolution
        strategy_name = (
            str(out.get("strategy"))
            or str(setup.get("strategy") or "")
            or str(setup.get("strategy_name") or "")
            or "unknown_strategy"
        )
        account_label = (
            str(out.get("account_label"))
            or str(setup.get("account_label") or "")
            or "unknown"
        )

        features = setup.get("features") or {}
        extra = setup.get("extra") or {}
        ai_profile = (
            setup.get("ai_profile")
            or features.get("ai_profile")
            or extra.get("ai_profile")
        )

        r_mult = out.get("r_multiple")
        pnl_usd = out.get("pnl_usd")
        win = out.get("win")

        # risk_usd lives in features (from executor / paper_broker)
        risk_usd = None
        if isinstance(features, dict):
            risk_usd = features.get("risk_usd")

        # AI score + allow (from features, if present)
        ai_score = None
        ai_allowed = None
        if isinstance(features, dict):
            ai_score = features.get("ai_score")
            ai_allowed = features.get("ai_allowed")
        # backwards compatible: some setups may store under different keys
        if ai_allowed is None and isinstance(features, dict):
            ai_allowed = features.get("ai_gate_allow")

        key = (account_label, strategy_name)
        sm = metrics_map.get(key)
        if sm is None:
            sm = StrategyMetrics(
                account_label=account_label,
                strategy_name=strategy_name,
                ai_profile=str(ai_profile) if ai_profile is not None else None,
            )
            metrics_map[key] = sm

        # mode (once, from extra)
        if sm.mode is None:
            mode = extra.get("mode") if isinstance(extra, dict) else None
            if mode:
                sm.mode = str(mode)

        try:
            sm.add_trade(
                r_multiple=float(r_mult) if r_mult is not None else None,
                pnl_usd=float(pnl_usd) if pnl_usd is not None else None,
                risk_usd=float(risk_usd) if risk_usd is not None else None,
                win=bool(win) if win is not None else None,
                ai_score=float(ai_score) if ai_score is not None else None,
                ai_allowed=bool(ai_allowed) if ai_allowed is not None else None,
            )
        except Exception as e:
            log.warning(
                "Failed to add trade for %s/%s (trade_id=%s): %r",
                account_label,
                strategy_name,
                trade_id,
                e,
            )

    strategies_payload: List[Dict[str, Any]] = []

    for (account_label, strategy_name), sm in metrics_map.items():
        sm.finalize()
        payload = asdict(sm)

        # r_values and equity_curve_R are noisy; omit from snapshot
        payload.pop("r_values", None)
        payload.pop("equity_curve_R", None)
        payload.pop("ai_scores", None)

        strategies_payload.append(payload)

    # sort: S-tier first, then by score desc
    strategies_payload.sort(
        key=lambda x: (x.get("tier") not in ("S", "A"), -int(x.get("score", 0)))
    )

    snapshot = {
        "version": 1,
        "strategies": strategies_payload,
    }

    return snapshot


def save_snapshot(snapshot: Dict[str, Any]) -> None:
    try:
        SNAPSHOT_PATH.write_text(
            json.dumps(snapshot, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        log.info("Saved AI strategy snapshot to %s", SNAPSHOT_PATH)
    except Exception as e:
        log.error("Failed to save snapshot to %s: %r", SNAPSHOT_PATH, e)


def main() -> None:
    snapshot = build_snapshot()
    save_snapshot(snapshot)


if __name__ == "__main__":
    main()
