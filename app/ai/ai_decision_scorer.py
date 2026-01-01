# ai_decision_scorer.py
# READ-ONLY AI decision scoring (safe for PAPER / CANARY / LIVE)

import json
import os
from statistics import mean

MEMORY_DIR = os.getenv("AI_MEMORY_PATH", "state/ai_memory")
MIN_OUTCOMES = int(os.getenv("AI_SCORING_MIN_OUTCOMES", "1"))


def _load_memory():
    if not os.path.isdir(MEMORY_DIR):
        return []

    rows = []
    for name in os.listdir(MEMORY_DIR):
        path = os.path.join(MEMORY_DIR, name)
        if not os.path.isfile(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                rows.append(json.loads(f.readline()))
        except Exception:
            continue
    return rows


def score_decision(*, features, symbol, strategy, account_label, mode):
    """
    Returns float score in [0,1] or None if insufficient memory
    """
    memory = _load_memory()
    if len(memory) < MIN_OUTCOMES:
        return None

    wins = []
    for row in memory:
        outcome = row.get("outcome", {})
        if "win" in outcome:
            wins.append(1.0 if outcome["win"] else 0.0)

    if len(wins) < MIN_OUTCOMES:
        return None

    return round(mean(wins), 4)
