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


# ---------------------------------------------------------------------------
# Outcomes-backed trade stats (canonical truth)
# ---------------------------------------------------------------------------

OUTCOMES_V1 = STATE_DIR / "ai_events" / "outcomes.v1.jsonl"

def _iter_jsonl(path: Path):
    if not path.exists():
        return
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = (line or "").strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if isinstance(obj, dict):
                    yield obj
    except Exception:
        return

def _load_outcomes_stats() -> Dict[str, Dict[str, Any]]:
    """
    Returns dict keyed by account_label:
      {
        "total": int,
        "wins": int,
        "losses": int,
        "win_rate_pct": float,
        "pnl_total_usd": float,
        "pnl_avg_usd": float,
        "last_outcome_ts_ms": int
      }
    """
    stats: Dict[str, Dict[str, Any]] = {}

    for row in _iter_jsonl(OUTCOMES_V1):
        if row.get("schema_version") != "outcome.v1":
            continue

        acct = row.get("account_label")
        if not isinstance(acct, str) or not acct.strip():
            continue
        acct = acct.strip()

        try:
            pnl_f = float(row.get("pnl_usd") or 0.0)
        except Exception:
            pnl_f = 0.0

        ts = row.get("ts_ms") or row.get("closed_ts_ms") or 0
        try:
            ts_i = int(ts)
        except Exception:
            ts_i = 0

        s = stats.get(acct)
        if s is None:
            s = {
                "total": 0,
                "wins": 0,
                "losses": 0,
                "pnl_total_usd": 0.0,
                "last_outcome_ts_ms": 0,
            }
            stats[acct] = s

        s["total"] += 1
        if pnl_f > 0:
            s["wins"] += 1
        elif pnl_f < 0:
            s["losses"] += 1

        s["pnl_total_usd"] += pnl_f
        if ts_i > int(s.get("last_outcome_ts_ms", 0) or 0):
            s["last_outcome_ts_ms"] = ts_i

    for acct, s in stats.items():
        t = int(s.get("total", 0) or 0)
        w = int(s.get("wins", 0) or 0)
        pnl_total = float(s.get("pnl_total_usd", 0.0) or 0.0)
        s["win_rate_pct"] = round((w / t * 100.0), 2) if t > 0 else 0.0
        s["pnl_avg_usd"] = (pnl_total / t) if t > 0 else 0.0

    return stats


def hydrate_dashboard_rows() -> List[Dict[str, Any]]:
    """
    Canonical v1 dashboard row hydrator.
    READ-ONLY. SAFE FOR LIVE.
    """
    orch = _safe_read_json(ORCH_STATE)
    ops = _safe_read_json(OPS_SNAPSHOT)

    subaccounts = orch.get("subaccounts", {})
    ops_accounts = ops.get("accounts", {})
    outcomes_stats = _load_outcomes_stats()

    rows: List[Dict[str, Any]] = []

    for account_id, acct in subaccounts.items():
        ops_acct = ops_accounts.get(account_id, {})

        trades = ops_acct.get("trades", {})
        perf = ops_acct.get("performance", {})
        ai = ops_acct.get("ai", {})
        risk = ops_acct.get("risk", {})

        # Prefer canonical outcomes-derived stats when available
        o = outcomes_stats.get(account_id) or outcomes_stats.get(acct.get("label", account_id)) or {}
        o_total = int(o.get("total", 0) or 0)
        o_wins = int(o.get("wins", 0) or 0)
        o_losses = int(o.get("losses", 0) or 0)
        o_win_rate = float(o.get("win_rate_pct", 0.0) or 0.0)
        o_pnl_total = float(o.get("pnl_total_usd", 0.0) or 0.0)
        o_pnl_avg = float(o.get("pnl_avg_usd", 0.0) or 0.0)
        o_last_ts = int(o.get("last_outcome_ts_ms", 0) or 0)
        total_trades = o_total if o_total > 0 else int(trades.get("total", 0))
        wins = o_wins if o_total > 0 else int(trades.get("wins", 0))
        losses = o_losses if o_total > 0 else int(trades.get("losses", 0))

        win_rate = o_win_rate if o_total > 0 else ((wins / total_trades * 100.0) if total_trades > 0 else 0.0)
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

            # Truthful USD performance (from outcomes.v1)
            "pnl_total_usd": float(o_pnl_total),
            "pnl_avg_usd": float(o_pnl_avg),
            "last_outcome_ts_ms": int(o_last_ts),

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
