from __future__ import annotations
from typing import Dict, Any
from time import time

# ==========================================================
# DASHBOARD TABLE SCHEMA — VERSION 1 (LOCKED)
# ==========================================================
# This schema defines EXACTLY ONE ROW per Flashback subaccount.
# All dashboard rendering, metrics, and ML overlays depend on this.
# Do NOT mutate fields in-place. Extend only via v2+.
# ==========================================================


def empty_account_row(account_label: str) -> Dict[str, Any]:
    now_ms = int(time() * 1000)

    return {
        # -------------------------------
        # Identity
        # -------------------------------
        "account_label": account_label,
        "strategy_name": None,
        "strategy_version": None,

        # -------------------------------
        # Lifecycle / Status
        # -------------------------------
        "enabled": False,
        "online": False,
        "phase": "unknown",  # running | stopped | degraded | booting
        "last_heartbeat_ms": None,

        # -------------------------------
        # Trade Activity
        # -------------------------------
        "open_trade": False,
        "last_trade_ts_ms": None,
        "total_trades": 0,
        "profitable_trades": 0,
        "losing_trades": 0,

        # -------------------------------
        # Performance Metrics
        # -------------------------------
        "avg_return_pct": None,
        "cumulative_return_pct": None,
        "win_rate_pct": None,

        # -------------------------------
        # AI / ML Signals
        # -------------------------------
        "confidence_score": None,      # 0.0 – 1.0
        "n_buckets": None,
        "ml_ready": False,
        "regime": None,

        # -------------------------------
        # Risk / Health
        # -------------------------------
        "risk_state": "unknown",       # ok | warning | blocked
        "error_count": 0,
        "last_error": None,

        # -------------------------------
        # Metadata
        # -------------------------------
        "last_updated_ms": now_ms,
        "schema_version": 1,
    }
