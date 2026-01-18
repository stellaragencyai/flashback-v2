# ai_decision_scorer.py
# READ-ONLY AI decision scoring (safe for PAPER / CANARY / LIVE)

import json
import os
from statistics import mean
from typing import Any, Dict, List, Optional

MEMORY_DIR = os.getenv("AI_MEMORY_PATH", "state/ai_memory")
MIN_OUTCOMES = int(os.getenv("AI_SCORING_MIN_OUTCOMES", "1"))


def _load_memory() -> List[Dict[str, Any]]:
    if not os.path.isdir(MEMORY_DIR):
        return []

    rows: List[Dict[str, Any]] = []
    for name in os.listdir(MEMORY_DIR):
        path = os.path.join(MEMORY_DIR, name)
        if not os.path.isfile(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                row = json.loads(f.readline())
                if isinstance(row, dict):
                    rows.append(row)
        except Exception:
            continue
    return rows


def score_decision(
    *,
    features: Dict[str, Any],
    symbol: str,
    strategy: str,
    account_label: str,
    mode: str,
) -> Optional[float]:
    """
    Returns float score in [0,1] or None if insufficient memory
    """
    memory = _load_memory()
    if len(memory) < MIN_OUTCOMES:
        return None

    wins: List[float] = []

    for row in memory:
        if not isinstance(row, dict):
            continue

        # Context filter (CRITICAL)
        if row.get("symbol") != symbol:
            continue
        if row.get("strategy") != strategy:
            continue
        if row.get("account_label") != account_label:
            continue
        if row.get("mode") != mode:
            continue

        outcome = row.get("outcome")
        if not isinstance(outcome, dict):
            continue

        win = outcome.get("win")
        if isinstance(win, bool):
            wins.append(1.0 if win else 0.0)

    if len(wins) < MIN_OUTCOMES:
        return None

    return round(mean(wins), 4)
