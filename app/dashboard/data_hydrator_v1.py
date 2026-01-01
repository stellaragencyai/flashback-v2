import json
import time
from pathlib import Path
from typing import Dict, Any, List

STATE_DIR = Path("state")
ORCH_STATE = STATE_DIR / "orchestrator_state.json"
OPS_SNAPSHOT = STATE_DIR / "ops_snapshot.json"

SCHEMA_VERSION = 1


def _safe_read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _now_ms() -> int:
    return int(time.time() * 1000)


def hydrate_dashboard_rows() -> List[Dict[str, Any]]:
    """
    Canonical v1 dashboard row hydrator.
    READ-ONLY. SAFE FOR LIVE.
    """
    orch = _safe_read_json(ORCH_STATE)
    ops = _safe_read_json(OPS_SNAPSHOT)

    subaccounts = orch.get("subaccounts", {})
    ops_accounts = ops.get("accounts", {})

    rows: List[Dict[str, Any]] = []

    for account_id, acct in subaccounts.items():
        ops_acct = ops_accounts.get(account_id, {})

        trades = ops_acct.get("trades", {})
        perf = ops_acct.get("performance", {})
        ai = ops_acct.get("ai", {})
        risk = ops_acct.get("risk", {})

        total_trades = int(trades.get("total", 0))
        wins = int(trades.get("wins", 0))
        losses = int(trades.get("losses", 0))

        win_rate = (wins / total_trades * 100.0) if total_trades > 0 else 0.0

        row = {
            # Identity
            "account_label": acct.get("label", account_id),
            "strategy_name": acct.get("strategy", {}).get("name", "unknown"),
            "strategy_version": acct.get("strategy", {}).get("version", "unknown"),

            # Lifecycle
            "enabled": bool(acct.get("enabled", False)),
            "online": bool(acct.get("online", False)),
            "phase": acct.get("phase", "unknown"),
            "heartbeat": acct.get("last_heartbeat_ms", 0),

            # Trade activity
            "open_trade": bool(trades.get("open_trade", False)),
            "total_trades": total_trades,
            "win_count": wins,
            "loss_count": losses,

            # Performance
            "avg_return_pct": float(perf.get("avg_return_pct", 0.0)),
            "cumulative_return_pct": float(perf.get("cumulative_return_pct", 0.0)),
            "win_rate_pct": round(win_rate, 2),

            # AI / ML
            "confidence_score": float(ai.get("confidence", 0.0)),
            "n_buckets": int(ai.get("buckets", 0)),
            "regime": ai.get("regime", "unknown"),
            "ml_ready": bool(ai.get("ml_ready", False)),

            # Risk
            "risk_state": risk.get("state", "unknown"),
            "error_count": int(risk.get("error_count", 0)),
            "last_error": risk.get("last_error", None),

            # Metadata
            "last_updated_ms": _now_ms(),
            "schema_version": SCHEMA_VERSION,
        }

        rows.append(row)

    return rows
