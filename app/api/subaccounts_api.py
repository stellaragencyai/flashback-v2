import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter

router = APIRouter()

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]  # ...\Flashback
STATE_ROOT = REPO_ROOT / "state"
STATE_APP = REPO_ROOT / "app" / "state"

OUTCOMES_V1 = STATE_ROOT / "ai_events" / "outcomes.v1.jsonl"

SUBACCOUNTS_STATE_CANDIDATES = [
    STATE_APP / "subaccounts_state.json",
    STATE_ROOT / "subaccounts_state.json",
]
GOV_STATE_CANDIDATES = [
    STATE_APP / "governance_state.json",
    STATE_ROOT / "governance_state.json",
]

# API build marker for debugging deploy mismatches
API_BUILD = "subaccounts_api.v2.tiered_online_2026-01-02"

# Online TTL (align with dashboard hydrator truth: 60s default)
ONLINE_TTL_SEC = float(os.getenv("ONLINE_TTL_SEC", "60"))

# If AI stack is enabled, these workers are considered CRITICAL.
# If any is dead, tier should go RED.
CRITICAL_WORKERS = {
    "executor_v2",
    "trade_outcomes",
    "ws_switchboard",
    "risk_daemon",
    "tp_sl_manager",
}

# Noncritical workers: warning tier if dead (YELLOW), not full RED.
NONCRITICAL_WORKERS = {
    "ai_pilot",
    "ai_action_router",
    "paper_price_feeder",
    "ai_journal",
}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    except Exception:
        return {}


def _pick_first_existing(candidates: List[Path]) -> Optional[Path]:
    for p in candidates:
        try:
            if p.exists():
                return p
        except Exception:
            continue
    return None


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


def _parse_window_to_start_ms(window: str, now_ms: int) -> Optional[int]:
    w = (window or "all").strip().lower()
    if w in ("all", "full", "max", ""):
        return None
    if w == "7d":
        return now_ms - 7 * 86400 * 1000
    if w == "30d":
        return now_ms - 30 * 86400 * 1000
    if w == "1y":
        return now_ms - 365 * 86400 * 1000
    return None


def _load_outcomes_stats(window: str) -> Dict[str, Dict[str, Any]]:
    now = _now_ms()
    start_ms = _parse_window_to_start_ms(window, now)

    stats: Dict[str, Dict[str, Any]] = {}

    for row in _iter_jsonl(OUTCOMES_V1):
        if row.get("schema_version") != "outcome.v1":
            continue

        acct = row.get("account_label")
        if not isinstance(acct, str) or not acct.strip():
            continue
        acct = acct.strip()

        ts = row.get("closed_ts_ms") or row.get("ts_ms") or 0
        try:
            ts_i = int(ts)
        except Exception:
            ts_i = 0

        if start_ms is not None and ts_i < start_ms:
            continue

        try:
            pnl = float(row.get("pnl_usd") or 0.0)
        except Exception:
            pnl = 0.0

        s = stats.get(acct)
        if s is None:
            s = {"total": 0, "wins": 0, "losses": 0, "pnl_usd_total": 0.0, "last_ts_ms": 0}
            stats[acct] = s

        s["total"] += 1
        if pnl > 0:
            s["wins"] += 1
        elif pnl < 0:
            s["losses"] += 1

        s["pnl_usd_total"] += pnl
        if ts_i > int(s.get("last_ts_ms", 0) or 0):
            s["last_ts_ms"] = ts_i

    for acct, s in stats.items():
        t = int(s.get("total", 0) or 0)
        w = int(s.get("wins", 0) or 0)
        pnl_total = float(s.get("pnl_usd_total", 0.0) or 0.0)
        last_ts = int(s.get("last_ts_ms", 0) or 0)

        win_rate = (w / t) if t > 0 else 0.0  # 0..1
        s["win_rate"] = win_rate
        s["win_rate_pct"] = round(win_rate * 100.0, 2)
        s["pnl_usd_avg"] = (pnl_total / t) if t > 0 else 0.0
        s["outcomes_last_ts_iso"] = (
            time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(last_ts / 1000.0))
            if last_ts > 0
            else None
        )
        s["outcomes_freshness_sec"] = round((now - last_ts) / 1000.0, 3) if last_ts > 0 else None

    return stats


def _derive_dead_workers(sa: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    Return {critical: [...], noncritical: [...]} using:
      - sa['workers_dead'] list (preferred)
      - sa['workers'] map (fallback, if present)
    """
    dead = sa.get("workers_dead") or []
    if not isinstance(dead, list):
        dead = []

    dead_norm = []
    for x in dead:
        try:
            dead_norm.append(str(x).strip())
        except Exception:
            continue

    # If workers_dead is empty but workers map exists, infer dead from alive==False
    if not dead_norm:
        workers = sa.get("workers") or {}
        if isinstance(workers, dict):
            for name, meta in workers.items():
                if not isinstance(name, str):
                    continue
                if not isinstance(meta, dict):
                    continue
                alive = meta.get("alive")
                if alive is False:
                    dead_norm.append(name.strip())

    crit = [w for w in dead_norm if w in CRITICAL_WORKERS]
    noncrit = [w for w in dead_norm if (w not in CRITICAL_WORKERS and w in NONCRITICAL_WORKERS)]
    return {"critical": sorted(list(set(crit))), "noncritical": sorted(list(set(noncrit)))}


def _tier_logic(enable_ai_stack: bool, enabled: bool, supervisor_ok: Any, supervisor_age_sec: Any, dead_map: Dict[str, List[str]]) -> Dict[str, Any]:
    """
    Canonical status tier logic:
      - Manual accounts: GREEN / MANUAL_MODE when enabled.
      - AI accounts:
          RED: supervisor_ok != True
          YELLOW: supervisor stale (>TTL) OR noncritical dead
          RED: any critical dead
          GREEN: ok + fresh + no critical dead (+ optionally no dead at all)
    """
    out = {
        "online_ttl_sec": ONLINE_TTL_SEC,
        "status_tier": "gray",
        "status_reason": "UNKNOWN",
        "is_online": False,
        "critical_workers_dead": dead_map.get("critical", []),
        "noncritical_workers_dead": dead_map.get("noncritical", []),
    }

    if not enable_ai_stack:
        if enabled:
            out["status_tier"] = "green"
            out["status_reason"] = "MANUAL_MODE"
            out["is_online"] = True
        else:
            out["status_tier"] = "gray"
            out["status_reason"] = "DISABLED"
            out["is_online"] = False
        return out

    # AI stack enabled
    try:
        ok = bool(supervisor_ok)
    except Exception:
        ok = False

    try:
        age = float(supervisor_age_sec)
    except Exception:
        age = 1e18

    if ok is not True:
        out["status_tier"] = "red"
        out["status_reason"] = "SUPERVISOR_DOWN"
        out["is_online"] = False
        return out

    # Critical dead is a hard fail even if supervisor is ok/fresh
    if out["critical_workers_dead"]:
        out["status_tier"] = "red"
        out["status_reason"] = "CRITICAL_WORKER_DEAD"
        out["is_online"] = False
        return out

    # Stale supervisor = yellow (degraded)
    if age > ONLINE_TTL_SEC:
        out["status_tier"] = "yellow"
        out["status_reason"] = "SUPERVISOR_STALE"
        out["is_online"] = False
        return out

    # Noncritical dead = yellow
    if out["noncritical_workers_dead"]:
        out["status_tier"] = "yellow"
        out["status_reason"] = "NONCRITICAL_WORKER_DEAD"
        out["is_online"] = False
        return out

    out["status_tier"] = "green"
    out["status_reason"] = "OK"
    out["is_online"] = True
    return out


@router.get("/subaccounts")
def get_subaccounts(window: str = "all"):
    state_path = _pick_first_existing(SUBACCOUNTS_STATE_CANDIDATES)
    if not state_path:
        return []

    state = _safe_read_json(state_path)
    subaccounts = state.get("subaccounts", [])
    if not isinstance(subaccounts, list):
        return []

    governance: Dict[str, Any] = {}
    gov_path = _pick_first_existing(GOV_STATE_CANDIDATES)
    if gov_path:
        governance = _safe_read_json(gov_path).get("districts", {}) or {}
        if not isinstance(governance, dict):
            governance = {}

    outcomes = _load_outcomes_stats(window)

    out_rows: List[Dict[str, Any]] = []

    for sa in subaccounts:
        if not isinstance(sa, dict):
            continue

        # Governance merge (best-effort)
        try:
            gid = sa.get("subaccount_uid")
            if gid and gid in governance and isinstance(governance[gid], dict):
                sa.update(governance[gid])
        except Exception:
            pass

        # Normalize uid/account_label for consistent joins
        uid = sa.get("account_label") or sa.get("subaccount_uid") or sa.get("subaccount_name")
        uid = str(uid) if uid is not None else "unknown"
        sa["account_label"] = uid
        sa["subaccount_uid"] = sa.get("subaccount_uid") or uid

        o = outcomes.get(uid, {})
        t = int(o.get("total", 0) or 0)

        # Overwrite placeholders with outcomes truth (windowed)
        sa["total_trades"] = t
        sa["win_rate"] = float(o.get("win_rate", 0.0) or 0.0)  # 0..1
        sa["win_rate_pct"] = float(o.get("win_rate_pct", round(sa["win_rate"] * 100.0, 2)) or 0.0)
        sa["pnl_usd_total"] = float(o.get("pnl_usd_total", 0.0) or 0.0) if t > 0 else 0.0
        sa["pnl_usd_avg"] = float(o.get("pnl_usd_avg", 0.0) or 0.0) if t > 0 else 0.0
        sa["outcomes_last_ts_iso"] = o.get("outcomes_last_ts_iso", None)
        sa["outcomes_freshness_sec"] = o.get("outcomes_freshness_sec", None)

        # Tiered online truth
        enable_ai_stack = bool(sa.get("enable_ai_stack", False))
        enabled = bool(sa.get("enabled", True))
        dead_map = _derive_dead_workers(sa)

        tier = _tier_logic(
            enable_ai_stack=enable_ai_stack,
            enabled=enabled,
            supervisor_ok=sa.get("supervisor_ok", False),
            supervisor_age_sec=sa.get("supervisor_age_sec", None),
            dead_map=dead_map,
        )

        sa["online_ttl_sec"] = tier["online_ttl_sec"]
        sa["status_tier"] = tier["status_tier"]
        sa["status_reason"] = tier["status_reason"]
        sa["critical_workers_dead"] = tier["critical_workers_dead"]
        sa["noncritical_workers_dead"] = tier["noncritical_workers_dead"]

        sa["is_online"] = bool(tier["is_online"])
        sa["status"] = "online" if sa["is_online"] else "offline"

        sa["window"] = (window or "all").lower()
        sa["api_build"] = API_BUILD

        out_rows.append(sa)

    return out_rows
