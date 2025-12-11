#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Policy Stats (v1.0)

Purpose
-------
Analyze the AI policy decisions recorded in:

    state/ai_policy_log.jsonl

and produce a compact but insightful summary:

    - How many decisions total
    - Allow vs block counts and percentages
    - Breakdown by reason (score_below_min, mode_not_enabled, etc.)
    - Per-strategy stats (how often AI blocks each strategy)
    - Per-mode stats (PAPER / LIVE_CANARY / LIVE_FULL)

This is READ-ONLY and does NOT change any policy.

Inputs
------
    state/ai_policy_log.jsonl
    Each line is a decision payload emitted by executor_ai_gate.ai_gate_decide:

      {
        "allow": bool,
        "reason": "ok" | "score_below_min" | ...,
        "strategy_name": "Sub1_Trend",
        "symbol": "BTCUSDT",
        "account_label": "main",
        "mode": "PAPER",
        "trade_id": "...",
        "score": 0.73,
        "min_score": 0.0,
        "min_score_live": 0.6,
        "min_score_canary": 0.5,
        "policy_flags": {...},
        "ts_ms": 1733500100123
      }

Outputs
-------
Logs a human-readable summary and writes a JSON snapshot to:

    state/ai_policy_stats.json

This will be used later by policy_tune.py to suggest threshold changes.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List

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
        return logging.getLogger("ai_policy_stats")


ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])
STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

POLICY_LOG_PATH: Path = STATE_DIR / "ai_policy_log.jsonl"
STATS_OUTPUT_PATH: Path = STATE_DIR / "ai_policy_stats.json"

log = get_logger("ai_policy_stats")


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        log.warning("[ai_policy_stats] %s does not exist; no decisions yet.", path)
        return []
    rows: List[Dict[str, Any]] = []
    with path.open("rb") as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            try:
                row = orjson.loads(raw)
            except Exception:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows


def _to_float(x: Any):
    try:
        return float(x)
    except Exception:
        return None


def build_stats(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(rows)
    if total == 0:
        return {
            "total_decisions": 0,
            "allow_count": 0,
            "block_count": 0,
            "allow_pct": 0.0,
            "block_pct": 0.0,
            "by_reason": {},
            "by_reason_pct": {},
            "by_strategy": [],
            "by_mode": [],
            "score_histogram": {},
        }

    allow_count = 0
    block_count = 0
    by_reason: Counter[str] = Counter()
    by_strategy: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "strategy_name": "",
        "decisions": 0,
        "allowed": 0,
        "blocked": 0,
        "allow_pct": 0.0,
        "avg_score": None,
        "score_sum": 0.0,
        "score_cnt": 0,
    })
    by_mode: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "mode": "",
        "decisions": 0,
        "allowed": 0,
        "blocked": 0,
        "allow_pct": 0.0,
    })

    # Simple score histogram buckets: <0.2, 0.2-0.4, 0.4-0.6, 0.6-0.8, >=0.8
    score_buckets = {
        "<0.2": 0,
        "0.2-0.4": 0,
        "0.4-0.6": 0,
        "0.6-0.8": 0,
        ">=0.8": 0,
        "None": 0,
    }

    for d in rows:
        allow = bool(d.get("allow"))
        reason = str(d.get("reason") or "unknown")
        strat = str(d.get("strategy_name") or "unknown")
        mode = str(d.get("mode") or "UNKNOWN").upper()
        score = _to_float(d.get("score"))

        if allow:
            allow_count += 1
        else:
            block_count += 1

        by_reason[reason] += 1

        # per-strategy
        s = by_strategy[strat]
        s["strategy_name"] = strat
        s["decisions"] += 1
        if allow:
            s["allowed"] += 1
        else:
            s["blocked"] += 1
        if score is not None:
            s["score_sum"] += score
            s["score_cnt"] += 1

        # per-mode
        m = by_mode[mode]
        m["mode"] = mode
        m["decisions"] += 1
        if allow:
            m["allowed"] += 1
        else:
            m["blocked"] += 1

        # score bucket
        if score is None:
            score_buckets["None"] += 1
        else:
            if score < 0.2:
                score_buckets["<0.2"] += 1
            elif score < 0.4:
                score_buckets["0.2-0.4"] += 1
            elif score < 0.6:
                score_buckets["0.4-0.6"] += 1
            elif score < 0.8:
                score_buckets["0.6-0.8"] += 1
            else:
                score_buckets[">=0.8"] += 1

    # finalize per-strategy stats
    for s in by_strategy.values():
        dec = s["decisions"] or 1
        s["allow_pct"] = round(s["allowed"] * 100.0 / dec, 2)
        if s["score_cnt"] > 0:
            s["avg_score"] = s["score_sum"] / s["score_cnt"]

    # finalize per-mode stats
    for m in by_mode.values():
        dec = m["decisions"] or 1
        m["allow_pct"] = round(m["allowed"] * 100.0 / dec, 2)

    def _pct_map(counts: Dict[str, int]) -> Dict[str, float]:
        return {k: round(v * 100.0 / total, 2) for k, v in counts.items()}

    reason_pct = _pct_map(dict(by_reason))

    stats: Dict[str, Any] = {
        "total_decisions": total,
        "allow_count": allow_count,
        "block_count": block_count,
        "allow_pct": round(allow_count * 100.0 / total, 2),
        "block_pct": round(block_count * 100.0 / total, 2),
        "by_reason": dict(by_reason),
        "by_reason_pct": reason_pct,
        "by_strategy": sorted(
            by_strategy.values(), key=lambda s: s["decisions"], reverse=True
        ),
        "by_mode": sorted(
            by_mode.values(), key=lambda m: m["decisions"], reverse=True
        ),
        "score_histogram": score_buckets,
    }
    return stats


def main() -> None:
    log.info("[ai_policy_stats] Loading decisions from %s ...", POLICY_LOG_PATH)
    rows = _load_jsonl(POLICY_LOG_PATH)
    log.info("[ai_policy_stats] Loaded %d decisions.", len(rows))

    stats = build_stats(rows)
    STATS_OUTPUT_PATH.write_bytes(orjson.dumps(stats, option=orjson.OPT_INDENT_2))

    # Log a quick human-readable summary
    log.info("=== AI Policy Stats ===")
    log.info("Total decisions: %d", stats["total_decisions"])
    log.info("Allow: %d (%.2f%%)  |  Block: %d (%.2f%%)",
             stats["allow_count"], stats["allow_pct"],
             stats["block_count"], stats["block_pct"])
    log.info("By reason: %r", stats["by_reason"])
    log.info("Score histogram: %r", stats["score_histogram"])
    log.info("Top strategies by decision count:")
    for s in stats["by_strategy"][:10]:
        log.info("  %s: %d decisions, %.2f%% allow, avg_score=%r",
                 s["strategy_name"], s["decisions"], s["allow_pct"], s["avg_score"])

    log.info("[ai_policy_stats] Wrote stats -> %s", STATS_OUTPUT_PATH)
    log.info("[ai_policy_stats] Done.")


if __name__ == "__main__":
    main()
