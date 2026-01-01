from __future__ import annotations
import json
import time
import threading
from pathlib import Path
from typing import Dict, Any

STATE_DIR = Path("state")
STATE_DIR.mkdir(exist_ok=True)

OUTCOMES_FILE = STATE_DIR / "trade_outcomes.jsonl"
METRICS_FILE = STATE_DIR / "trade_outcomes_metrics.json"

WRITE_THROTTLE_MS = 250
_last_write_ms = 0

_seen_hashes: set[str] = set()
_lock = threading.Lock()

_metrics = {
    "written": 0,
    "deduplicated": 0,
    "throttled": 0,
    "schema_invalid": 0,
    "dropped_outcomes": 0,
}


REQUIRED_KEYS = {
    "ts_ms",
    "symbol",
    "side",
    "entry_price",
    "exit_price",
    "pnl",
    "strategy",
    "account",
}


def _hash_outcome(o: Dict[str, Any]) -> str:
    return json.dumps(o, sort_keys=True)


def _validate_schema(o: Dict[str, Any]) -> bool:
    return REQUIRED_KEYS.issubset(o.keys())


def write_trade_outcome(outcome: Dict[str, Any]) -> None:
    global _last_write_ms

    now_ms = int(time.time() * 1000)

    with _lock:
        # Throttle
        if now_ms - _last_write_ms < WRITE_THROTTLE_MS:
            _metrics["throttled"] += 1
            _metrics["dropped_outcomes"] += 1
            _flush_metrics()
            return

        # Schema validation
        if not _validate_schema(outcome):
            _metrics["schema_invalid"] += 1
            _metrics["dropped_outcomes"] += 1
            _flush_metrics()
            return

        h = _hash_outcome(outcome)
        if h in _seen_hashes:
            _metrics["deduplicated"] += 1
            _metrics["dropped_outcomes"] += 1
            _flush_metrics()
            return

        _seen_hashes.add(h)

        with OUTCOMES_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(outcome) + "\n")

        _last_write_ms = now_ms
        _metrics["written"] += 1
        _flush_metrics()


def _flush_metrics():
    METRICS_FILE.write_text(json.dumps(_metrics, indent=2))
