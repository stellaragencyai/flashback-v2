from __future__ import annotations

import os
import time

try:
    from app.core.ai_decision_logger import append_decision
except Exception as e:
    raise SystemExit(f"FATAL: ai_decision_logger unavailable: {e!r}")

def _now_ms() -> int:
    return int(time.time() * 1000)

def main() -> None:
    label = (os.getenv("ACCOUNT_LABEL", "main") or "main").strip()
    ts_ms = _now_ms()

    # Minimal-but-valid pilot decision row.
    # Purpose: deterministic ops tick to refresh ai_decisions.jsonl + prove writer is alive.
    payload = {
        "schema_version": 1,
        "ts_ms": ts_ms,
        "ts": ts_ms,  # legacy compatibility
        "event_type": "pilot_decision",
        "meta": {"source": "ai_decision_tick", "stage": "ops"},
        "decision": "COLD_START",
        "tier_used": "NONE",
        "memory": None,
        "gates": {"reason": "ops_tick"},
        "proposed_action": None,
        "allow": False,
        "size_multiplier": 0.0,
        "account_label": label,
        "trade_id": "",
        "symbol": "",
        "timeframe": "5m",
    }

    append_decision(payload)
    print(f"OK: wrote decision tick label={label} ts_ms={ts_ms}")

if __name__ == "__main__":
    main()
