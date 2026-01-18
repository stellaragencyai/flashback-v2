import json
import os
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ------------------------------------------------------------
# Pathing: ALWAYS anchor from repo root (C:\Flashback)
# ------------------------------------------------------------
_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]  # ...\Flashback
STATE_ROOT = REPO_ROOT / "state"
STATE_APP = REPO_ROOT / "app" / "state"  # legacy / existing in your repo

# Global outcomes file (legacy) + lane outcomes files (truth)
OUTCOMES_V1_GLOBAL = STATE_ROOT / "ai_events" / "outcomes.v1.jsonl"

# Orchestrator truth candidates
ORCH_STATE_CANDIDATES = [
    STATE_ROOT / "orchestrator_state.json",
    REPO_ROOT / "app" / "ops" / "orchestrator_state.json",
]

# Watchdog is not full state, but it's a useful signal
ORCH_WATCHDOG_CANDIDATES = [
    STATE_ROOT / "orchestrator_watchdog.json",
    REPO_ROOT / "app" / "ops" / "orchestrator_watchdog.json",
]

# Some of your state lives under app/state (legacy)
SUBACCOUNTS_STATE_CANDIDATES = [
    STATE_APP / "subaccounts_state.json",
    STATE_ROOT / "subaccounts_state.json",
]
GOV_STATE_CANDIDATES = [
    STATE_APP / "governance_state.json",
    STATE_ROOT / "governance_state.json",
]

SCHEMA_VERSION = 4

# TTL used for ONLINE determination (telemetry freshness)
ONLINE_TTL_SEC = float(os.getenv("SUBACCOUNTS_ONLINE_TTL_SEC", "60"))

# Dashboard bus health thresholds
BUS_GREEN_SEC = float(os.getenv("DASH_BUS_GREEN_SEC", "15"))
BUS_YELLOW_SEC = float(os.getenv("DASH_BUS_YELLOW_SEC", "60"))

# These are "critical" for live trading integrity. If dead while AI stack is enabled => RED.
CRITICAL_WORKERS = {
    "ws_switchboard",
    "trade_outcomes",
    "tp_sl_manager",
    "risk_daemon",
}
# "noncritical" can degrade UX/learning but isn't immediate execution-kill
NONCRITICAL_WORKERS = {
    "ai_pilot",
    "ai_action_router",
    "paper_price_feeder",
    "ai_journal",
}

EXPECTED_ACCOUNTS = ["main"] + [f"flashback{n:02d}" for n in range(1, 11)]


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
    """
    Returns start_ms (inclusive) or None for 'all'.
    """
    w = (window or "all").strip().lower()
    if w in ("all", "full", "max", ""):
        return None
    if w == "7d":
        return now_ms - 7 * 86400 * 1000
    if w == "30d":
        return now_ms - 30 * 86400 * 1000
    if w == "1y":
        return now_ms - 365 * 86400 * 1000
    # Unknown window -> treat as all (fail-soft)
    return None


# ---------------------------------------------------------------------
# Heartbeat telemetry (written by ws_switchboard)
# ---------------------------------------------------------------------
def _heartbeat_path_for_label(label: str) -> Path:
    lab = str(label or "").strip()
    return STATE_ROOT / f"ws_switchboard_heartbeat_{lab}.txt"


def _heartbeat_age_sec(label: str, now_ms: int) -> Tuple[bool, Optional[float], Optional[int], str]:
    """
    Returns: (exists, age_sec, mtime_ms, path_str)
    """
    p = _heartbeat_path_for_label(label)
    if not p.exists():
        return (False, None, None, str(p))
    try:
        st = p.stat()
        mtime_ms = int(st.st_mtime * 1000)
        age = round((now_ms - mtime_ms) / 1000.0, 3)
        if age < 0:
            age = 0.0
        return (True, age, mtime_ms, str(p))
    except Exception:
        return (True, None, None, str(p))


# ---------------------------------------------------------------------
# Bus telemetry (per-account JSON files written by ws_switchboard)
# ---------------------------------------------------------------------
def _bus_paths_for_label(label: str) -> Dict[str, Path]:
    lab = str(label or "").strip()
    return {
        "trades": STATE_ROOT / f"trades_bus_{lab}.json",
        "orderbook": STATE_ROOT / f"orderbook_bus_{lab}.json",
        "positions": STATE_ROOT / f"positions_bus_{lab}.json",
    }


def _read_bus_file(path: Path) -> Dict[str, Any]:
    return _safe_read_json(path)


def _extract_bus_updated_ms(bus_obj: Dict[str, Any]) -> Optional[int]:
    try:
        v = bus_obj.get("updated_ms", None)
        if v is None:
            return None
        vi = int(v)
        return vi if vi > 0 else None
    except Exception:
        return None


def _extract_symbol_keys(bus_obj: Dict[str, Any]) -> List[str]:
    syms = bus_obj.get("symbols", {})
    if isinstance(syms, dict):
        out = []
        for k in syms.keys():
            ks = str(k).strip()
            if ks:
                out.append(ks)
        return out
    return []


def _positions_label_present(bus_obj: Dict[str, Any], label: str) -> Optional[bool]:
    """
    positions bus may be:
      - version 2 w/ {"labels": {"flashback06": {...}}}
      - or a simple object keyed differently depending on patch lineage
    We only do a best-effort presence check.
    """
    lab = str(label or "").strip()
    if not lab:
        return None

    labels = bus_obj.get("labels")
    if isinstance(labels, dict):
        return lab in labels

    return (lab in bus_obj) if isinstance(bus_obj, dict) else None


def _compute_bus_health(trades_age: Optional[float], ob_age: Optional[float], pos_age: Optional[float]) -> Tuple[str, str]:
    """
    Returns (tier, reason)
      - GRAY: no bus data
      - GREEN: all present and <= BUS_GREEN_SEC
      - YELLOW: present but stale (<= BUS_YELLOW_SEC)
      - RED: missing or very stale
    """
    ages = [a for a in (trades_age, ob_age, pos_age) if isinstance(a, (int, float))]
    if not ages:
        return ("GRAY", "NO_BUS_DATA")

    if trades_age is None or ob_age is None or pos_age is None:
        return ("RED", "MISSING_ONE_OR_MORE_BUSES")

    worst = max(ages) if ages else None
    if worst is None:
        return ("GRAY", "NO_AGES")

    if worst <= BUS_GREEN_SEC:
        return ("GREEN", f"FRESH<= {BUS_GREEN_SEC}s")
    if worst <= BUS_YELLOW_SEC:
        return ("YELLOW", f"STALE<= {BUS_YELLOW_SEC}s")
    return ("RED", f"VERY_STALE> {BUS_YELLOW_SEC}s")


def _load_bus_telemetry(label: str, now_ms: int) -> Dict[str, Any]:
    """
    Loads per-account bus telemetry from state/*_bus_<label>.json.
    Fail-soft: never throws, returns fields as None when missing.
    """
    lab = str(label or "").strip()
    paths = _bus_paths_for_label(lab)

    trades_obj = _read_bus_file(paths["trades"])
    ob_obj = _read_bus_file(paths["orderbook"])
    pos_obj = _read_bus_file(paths["positions"])

    t_upd = _extract_bus_updated_ms(trades_obj) if trades_obj else None
    o_upd = _extract_bus_updated_ms(ob_obj) if ob_obj else None
    p_upd = _extract_bus_updated_ms(pos_obj) if pos_obj else None

    def _age(upd: Optional[int]) -> Optional[float]:
        if not upd:
            return None
        try:
            return round((now_ms - int(upd)) / 1000.0, 3)
        except Exception:
            return None

    t_age = _age(t_upd)
    o_age = _age(o_upd)
    p_age = _age(p_upd)

    t_syms = _extract_symbol_keys(trades_obj) if trades_obj else []
    o_syms = _extract_symbol_keys(ob_obj) if ob_obj else []

    tier, reason = _compute_bus_health(t_age, o_age, p_age)

    return {
        "trades_bus_path": str(paths["trades"]),
        "orderbook_bus_path": str(paths["orderbook"]),
        "positions_bus_path": str(paths["positions"]),
        "trades_bus_exists": paths["trades"].exists(),
        "orderbook_bus_exists": paths["orderbook"].exists(),
        "positions_bus_exists": paths["positions"].exists(),

        "trades_bus_updated_ms": t_upd,
        "orderbook_bus_updated_ms": o_upd,
        "positions_bus_updated_ms": p_upd,

        "trades_bus_age_sec": t_age,
        "orderbook_bus_age_sec": o_age,
        "positions_bus_age_sec": p_age,

        "trades_bus_symbol_count": len(t_syms),
        "orderbook_bus_symbol_count": len(o_syms),
        "trades_bus_symbols": t_syms[:50],
        "orderbook_bus_symbols": o_syms[:50],
        "positions_bus_label_present": _positions_label_present(pos_obj, lab) if pos_obj else None,
        "bus_health_tier": tier,
        "bus_health_reason": reason,
    }


def _telemetry_is_fresh(bus: Dict[str, Any], hb_age: Optional[float]) -> Tuple[bool, str]:
    """
    Telemetry considered fresh if:
      - heartbeat exists and hb_age <= ONLINE_TTL_SEC
        OR
      - any bus age exists and <= ONLINE_TTL_SEC
    """
    if isinstance(hb_age, (int, float)) and hb_age <= ONLINE_TTL_SEC:
        return (True, f"HEARTBEAT_FRESH<= {ONLINE_TTL_SEC}s")

    ages = []
    for k in ("trades_bus_age_sec", "orderbook_bus_age_sec", "positions_bus_age_sec"):
        v = bus.get(k)
        if isinstance(v, (int, float)):
            ages.append(v)

    if ages:
        best = min(ages)
        if best <= ONLINE_TTL_SEC:
            return (True, f"BUS_FRESH<= {ONLINE_TTL_SEC}s")

    return (False, "NO_FRESH_TELEMETRY")


def _load_outcomes_paths() -> List[Path]:
    """
    Truth: outcomes may live in multiple lanes under state/ai_events*.
    Collect all outcomes.v1.jsonl files under state.
    """
    out: List[Path] = []
    try:
        for p in STATE_ROOT.glob("ai_events*/outcomes.v1.jsonl"):
            if p.is_file():
                out.append(p)
    except Exception:
        pass

    if OUTCOMES_V1_GLOBAL.exists() and OUTCOMES_V1_GLOBAL not in out:
        out.append(OUTCOMES_V1_GLOBAL)

    return sorted(out, key=lambda x: str(x).lower())


def _iter_all_outcomes_rows():
    for path in _load_outcomes_paths():
        yield from _iter_jsonl(path)


def _load_outcomes_stats(window: str) -> Dict[str, Dict[str, Any]]:
    now = _now_ms()
    start_ms = _parse_window_to_start_ms(window, now)

    stats: Dict[str, Dict[str, Any]] = {}

    for row in _iter_all_outcomes_rows():
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

        try:
            fees = float(row.get("fees_usd") or 0.0)
        except Exception:
            fees = 0.0

        s = stats.get(acct)
        if s is None:
            s = {
                "total": 0,
                "wins": 0,
                "losses": 0,
                "pnl_usd_total": 0.0,
                "fees_usd_total": 0.0,
                "last_ts_ms": 0,
                "modes": Counter(),
            }
            stats[acct] = s

        s["total"] += 1
        if pnl > 0:
            s["wins"] += 1
        elif pnl < 0:
            s["losses"] += 1

        s["pnl_usd_total"] += pnl
        s["fees_usd_total"] += fees
        if ts_i > int(s.get("last_ts_ms", 0) or 0):
            s["last_ts_ms"] = ts_i

        mode = str(row.get("mode") or "").strip().upper() or "UNKNOWN"
        s["modes"][mode] += 1

    for acct, s in stats.items():
        t = int(s.get("total", 0) or 0)
        w = int(s.get("wins", 0) or 0)
        pnl_total = float(s.get("pnl_usd_total", 0.0) or 0.0)
        last_ts = int(s.get("last_ts_ms", 0) or 0)

        win_rate = (w / t) if t > 0 else 0.0
        s["win_rate"] = win_rate
        s["win_rate_pct"] = round(win_rate * 100.0, 2)
        s["pnl_usd_avg"] = (pnl_total / t) if t > 0 else 0.0
        s["outcomes_last_ts_iso"] = (
            time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(last_ts / 1000.0))
            if last_ts > 0
            else None
        )
        s["outcomes_freshness_sec"] = round((now - last_ts) / 1000.0, 3) if last_ts > 0 else None
        s["ltm_trades_24h"] = 0
        try:
            s["top_outcome_modes"] = s["modes"].most_common(3)
        except Exception:
            s["top_outcome_modes"] = []

    ltm_start = now - 24 * 3600 * 1000
    for row in _iter_all_outcomes_rows():
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
        if ts_i < ltm_start:
            continue

        if acct in stats:
            stats[acct]["ltm_trades_24h"] = int(stats[acct].get("ltm_trades_24h", 0) or 0) + 1
        else:
            stats[acct] = {
                "total": 0,
                "wins": 0,
                "losses": 0,
                "pnl_usd_total": 0.0,
                "fees_usd_total": 0.0,
                "last_ts_ms": 0,
                "win_rate": 0.0,
                "win_rate_pct": 0.0,
                "pnl_usd_avg": 0.0,
                "outcomes_last_ts_iso": None,
                "outcomes_freshness_sec": None,
                "ltm_trades_24h": 1,
                "modes": Counter(),
                "top_outcome_modes": [],
            }

    return stats


def _load_orchestrator_state() -> Tuple[Dict[str, Any], Optional[str]]:
    for cand in ORCH_STATE_CANDIDATES:
        try:
            if cand.exists():
                return _safe_read_json(cand), str(cand)
        except Exception:
            continue
    return {}, None


def _load_orchestrator_watchdog() -> Tuple[Dict[str, Any], Optional[str]]:
    for cand in ORCH_WATCHDOG_CANDIDATES:
        try:
            if cand.exists():
                return _safe_read_json(cand), str(cand)
        except Exception:
            continue
    return {}, None


def _load_subaccounts_state_legacy() -> List[Dict[str, Any]]:
    state_path = _pick_first_existing(SUBACCOUNTS_STATE_CANDIDATES)
    if not state_path:
        return []
    state = _safe_read_json(state_path)
    subs = state.get("subaccounts", [])
    return subs if isinstance(subs, list) else []


def _merge_governance(subaccounts: List[Dict[str, Any]]) -> None:
    gov_path = _pick_first_existing(GOV_STATE_CANDIDATES)
    if not gov_path:
        return
    gov = _safe_read_json(gov_path).get("districts", {})
    if not isinstance(gov, dict):
        return
    for sa in subaccounts:
        try:
            gid = sa.get("subaccount_uid") or sa.get("account_label")
            if gid and gid in gov and isinstance(gov[gid], dict):
                sa.update(gov[gid])
        except Exception:
            continue


def _dead_worker_sets(workers_dead: Any) -> Tuple[List[str], List[str], List[str]]:
    dead_list: List[str] = []
    if isinstance(workers_dead, list):
        dead_list = [str(x) for x in workers_dead if str(x).strip()]
    critical = [w for w in dead_list if w in CRITICAL_WORKERS]
    noncritical = [w for w in dead_list if w in NONCRITICAL_WORKERS and w not in critical]
    return dead_list, critical, noncritical


def _status_tier_and_reason_from_orch(
    enabled: Optional[bool],
    should_run: Optional[bool],
    status: str,
    reason: Optional[str],
    alive: bool,
) -> Tuple[str, str]:
    """
    enabled/should_run can be None when orchestrator state is missing.
    """
    # Explicitly disabled is still disabled
    if enabled is False:
        return ("gray", "DISABLED")

    st = (status or "").strip().upper() or "UNKNOWN"
    rs = (reason or "").strip()

    # If orchestrator is missing, we shouldn't pretend "disabled"
    if st in ("MISSING", "UNKNOWN") and (enabled is None or should_run is None):
        return ("yellow", rs or "ORCH_STATE_UNKNOWN")

    if st == "RUNNING" and alive:
        return ("green", "OK")

    if st == "RUNNING" and not alive:
        return ("red", "PROC_DEAD")

    if st == "SKIPPED":
        if should_run:
            return ("yellow", f"SKIPPED:{rs or 'UNKNOWN'}")
        return ("gray", f"SKIPPED:{rs or 'UNKNOWN'}")

    if (should_run is True) and not alive:
        return ("yellow", f"NOT_RUNNING:{st or 'UNKNOWN'}:{rs or 'NO_REASON'}")

    return ("gray", st or "UNKNOWN")


def _build_base_rows_from_orchestrator() -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    orch, orch_path = _load_orchestrator_state()
    wd, wd_path = _load_orchestrator_watchdog()

    subs_map = orch.get("subaccounts") if isinstance(orch.get("subaccounts"), dict) else {}
    procs_map = orch.get("procs") if isinstance(orch.get("procs"), dict) else {}

    meta = {
        "orch_present": bool(orch),
        "orch_path": orch_path,
        "orch_mode": (orch.get("mode") or "UNKNOWN"),
        "orch_only_labels": orch.get("only_labels") or [],
        "orch_started": orch.get("started") or [],
        "orch_skipped": orch.get("skipped") or [],
        "orch_ts_ms": orch.get("ts_ms") or 0,

        "watchdog_present": bool(wd),
        "watchdog_path": wd_path,
        "watchdog_ts_ms": wd.get("ts_ms") if isinstance(wd.get("ts_ms"), (int, float, str)) else None,
        "watchdog_labels": wd.get("labels") if isinstance(wd.get("labels"), list) else [],
        "watchdog_checked": wd.get("checked") if isinstance(wd.get("checked"), dict) else {},
    }

    out: List[Dict[str, Any]] = []

    for uid in EXPECTED_ACCOUNTS:
        base: Dict[str, Any] = {
            "subaccount_uid": uid,
            "subaccount_name": uid,
            "account_label": uid,
        }

        if isinstance(subs_map, dict) and uid in subs_map and isinstance(subs_map[uid], dict):
            sa = subs_map[uid]
            base.update(sa)
        else:
            # IMPORTANT CHANGE:
            # Do NOT force enabled/should_run to False when orch state is missing.
            # Unknown is unknown. Telemetry will decide online truth later.
            base.update({
                "label": uid,
                "enabled": None,              # was False
                "enable_ai_stack": False,
                "automation_mode": "UNKNOWN",
                "should_run": None,           # was False
                "online": False,
                "status": "MISSING",
                "reason": "NOT_IN_ORCHESTRATOR_STATE",
                "strategy": {"name": "unknown", "version": "unknown"},
            })

        proc = procs_map.get(uid) if isinstance(procs_map, dict) else None
        if isinstance(proc, dict):
            base["pid"] = proc.get("pid")
            base["alive"] = bool(proc.get("alive"))
            base["stdout_log"] = proc.get("stdout_log")
            base["stderr_log"] = proc.get("stderr_log")
            base["proc_started_ts_ms"] = proc.get("started_ts_ms")
        else:
            base.setdefault("pid", None)
            base.setdefault("alive", False)
            base.setdefault("stdout_log", None)
            base.setdefault("stderr_log", None)
            base.setdefault("proc_started_ts_ms", None)

        out.append(base)

    return out, meta


def hydrate_dashboard_rows(window: str = "all") -> List[Dict[str, Any]]:
    now = _now_ms()

    base_rows, orch_meta = _build_base_rows_from_orchestrator()
    _merge_governance(base_rows)
    outcomes = _load_outcomes_stats(window)

    rows: List[Dict[str, Any]] = []

    for sa in base_rows:
        uid = sa.get("subaccount_uid") or sa.get("label") or sa.get("subaccount_name") or "unknown"
        uid = str(uid)

        o = outcomes.get(uid, {})

        # enabled/should_run may be None when orch state is missing
        enabled_raw = sa.get("enabled", None)
        should_run_raw = sa.get("should_run", None)

        enabled: Optional[bool] = enabled_raw if isinstance(enabled_raw, bool) else None
        should_run: Optional[bool] = should_run_raw if isinstance(should_run_raw, bool) else None

        enable_ai_stack = bool(sa.get("enable_ai_stack", False))
        status = sa.get("status") or "UNKNOWN"
        reason = sa.get("reason")

        alive = bool(sa.get("alive", False))

        # Bus telemetry
        if uid != "main":
            bus = _load_bus_telemetry(uid, now)
        else:
            bus = {
                "trades_bus_path": str(STATE_ROOT / "trades_bus.json"),
                "orderbook_bus_path": str(STATE_ROOT / "orderbook_bus.json"),
                "positions_bus_path": str(STATE_ROOT / "positions_bus.json"),
                "trades_bus_exists": (STATE_ROOT / "trades_bus.json").exists(),
                "orderbook_bus_exists": (STATE_ROOT / "orderbook_bus.json").exists(),
                "positions_bus_exists": (STATE_ROOT / "positions_bus.json").exists(),
                "trades_bus_updated_ms": None,
                "orderbook_bus_updated_ms": None,
                "positions_bus_updated_ms": None,
                "trades_bus_age_sec": None,
                "orderbook_bus_age_sec": None,
                "positions_bus_age_sec": None,
                "trades_bus_symbol_count": 0,
                "orderbook_bus_symbol_count": 0,
                "trades_bus_symbols": [],
                "orderbook_bus_symbols": [],
                "positions_bus_label_present": None,
                "bus_health_tier": "GRAY",
                "bus_health_reason": "MAIN_USES_GLOBAL_BUSES",
            }

        # Heartbeat telemetry
        hb_exists, hb_age, hb_mtime_ms, hb_path = (
            _heartbeat_age_sec(uid, now) if uid != "main" else (False, None, None, str(_heartbeat_path_for_label(uid)))
        )

        telem_fresh, telem_reason = _telemetry_is_fresh(bus, hb_age)

        # Online truth rules:
        # - If explicitly disabled or explicitly should_run=False => offline
        # - Otherwise (including orch-unknown), online if process alive OR telemetry fresh
        if enabled is False:
            is_online = False
            online_gate_reason = "EXPLICIT_DISABLED"
        elif should_run is False:
            is_online = False
            online_gate_reason = "EXPLICIT_SHOULD_NOT_RUN"
        else:
            is_online = bool(alive) or bool(telem_fresh)
            online_gate_reason = "ALIVE_OR_TELEMETRY" if is_online else "NO_ALIVE_NO_TELEM"

        status_tier, status_reason = _status_tier_and_reason_from_orch(
            enabled=enabled,
            should_run=should_run,
            status=str(status),
            reason=str(reason) if reason is not None else None,
            alive=alive,
        )

        # If telemetry says online but proc isn't alive, call it out explicitly
        if is_online and not alive and uid != "main":
            status_tier = "yellow"
            status_reason = f"TELEMETRY_ONLINE_BUT_PROC_DEAD:{telem_reason}"

        workers_running = int(sa.get("workers_running", 0) or 0)
        workers_dead_raw = sa.get("workers_dead") or []
        workers_dead_all, critical_dead, noncritical_dead = _dead_worker_sets(workers_dead_raw)

        strat = sa.get("strategy") if isinstance(sa.get("strategy"), dict) else {}
        strategy_name = (
            sa.get("current_strategy")
            or sa.get("strategy_name")
            or strat.get("name")
            or "unknown"
        )

        total_trades = int(o.get("total", 0) or 0)
        win_rate = float(o.get("win_rate", 0.0) or 0.0)  # 0..1
        pnl_total = float(o.get("pnl_usd_total", 0.0) or 0.0)

        orch_mode = str(orch_meta.get("orch_mode") or "UNKNOWN").strip().upper()
        top_outcome_modes = o.get("top_outcome_modes") or []
        outcome_mode_primary = top_outcome_modes[0][0] if (isinstance(top_outcome_modes, list) and top_outcome_modes) else None
        mode_mismatch = bool(outcome_mode_primary) and (orch_mode != "UNKNOWN") and (str(outcome_mode_primary).upper() != orch_mode)

        # Normalize enabled/should_run to bool fields for front-end compatibility
        enabled_bool = bool(enabled) if enabled is not None else False
        should_run_bool = bool(should_run) if should_run is not None else False

        row = {
            "account": uid,
            "account_label": uid,
            "subaccount_uid": uid,
            "subaccount_name": uid,

            "strategy": strategy_name,
            "strategy_version": strat.get("version") if isinstance(strat, dict) else None,
            "automation_mode": sa.get("automation_mode") or "unknown",
            "funds_source": _funds_source(sa.get("automation_mode") or "unknown"),
            "balance_display_mode": ("SHOW" if _funds_source(sa.get("automation_mode") or "unknown") == "REAL" else "HIDE"),
            "role": sa.get("role") or "unknown",
            "risk_pct": float(sa.get("risk_pct", 0.0) or 0.0),
            "symbols": sa.get("symbols") or [],
            "timeframes": sa.get("timeframes") or [],
            "setup_types": sa.get("setup_types") or [],

            "orch_mode": orch_mode,
            "orch_only_labels": orch_meta.get("orch_only_labels") or [],
            "orch_status": str(status),
            "orch_reason": reason,

            # NEW: raw truth (can be None)
            "enabled_raw": enabled,
            "should_run_raw": should_run,

            # Legacy bools for UI compatibility
            "should_run": should_run_bool,
            "enabled": enabled_bool,
            "enable_ai_stack": enable_ai_stack,

            "pid": sa.get("pid"),
            "alive": alive,
            "stdout_log": sa.get("stdout_log"),
            "stderr_log": sa.get("stderr_log"),
            "proc_started_ts_ms": sa.get("proc_started_ts_ms"),

            "online_ttl_sec": ONLINE_TTL_SEC,
            "is_online": is_online,
            "online_gate_reason": online_gate_reason,
            "status": "online" if is_online else "offline",
            "status_tier": status_tier,
            "status_reason": status_reason,

            "heartbeat_path": hb_path,
            "heartbeat_exists": hb_exists,
            "heartbeat_age_sec": hb_age,
            "heartbeat_mtime_ms": hb_mtime_ms,
            "telemetry_fresh": telem_fresh,
            "telemetry_reason": telem_reason,

            "workers_running": workers_running,
            "workers_enabled": sa.get("workers_enabled") or [],
            "workers_dead": workers_dead_all,
            "critical_workers_dead": critical_dead,
            "noncritical_workers_dead": noncritical_dead,
            "workers": sa.get("workers") or {},

            "balance": float(sa.get("balance", 0.0) or 0.0),
            "n_bucket": int(sa.get("n_bucket", 0) or 0),
            "autonomy_ready": bool(sa.get("autonomy_ready", False)),
            "telegram_enabled": bool(sa.get("telegram_enabled", False)),

            "total_trades": total_trades,
            "win_rate": win_rate,
            "win_rate_pct": float(o.get("win_rate_pct", round(win_rate * 100.0, 2)) if total_trades else 0.0),
            "pnl_usd_total": pnl_total,
            "pnl_usd_avg": float(o.get("pnl_usd_avg", 0.0) or 0.0),
            "outcomes_last_ts_iso": o.get("outcomes_last_ts_iso", None),
            "outcomes_freshness_sec": o.get("outcomes_freshness_sec", None),

            "top_outcome_modes": top_outcome_modes,
            "outcome_mode_primary": outcome_mode_primary,
            "mode_mismatch": mode_mismatch,

            "ltm_trades_24h": int(o.get("ltm_trades_24h", 0) or 0),

            **bus,

            "window": (window or "all").lower(),
            "updated_ms": now,
            "schema_version": SCHEMA_VERSION,
        }

        rows.append(row)

    order = {a: i for i, a in enumerate(EXPECTED_ACCOUNTS)}
    rows.sort(key=lambda r: order.get(str(r.get("account_label") or r.get("account") or ""), 999))
    return rows


def hydrate_dashboard_meta(window: str = "all") -> Dict[str, Any]:
    orch, orch_path = _load_orchestrator_state()
    wd, wd_path = _load_orchestrator_watchdog()

    orch_mode = str(orch.get("mode") or "UNKNOWN").strip().upper()
    only_labels = orch.get("only_labels") or []
    started = orch.get("started") or []
    skipped = orch.get("skipped") or []

    now = _now_ms()
    last_ts = 0
    total_rows = 0

    for row in _iter_all_outcomes_rows():
        if row.get("schema_version") != "outcome.v1":
            continue
        total_rows += 1
        ts = row.get("closed_ts_ms") or row.get("ts_ms") or 0
        try:
            ts_i = int(ts)
        except Exception:
            ts_i = 0
        if ts_i > last_ts:
            last_ts = ts_i

    freshness_sec = round((now - last_ts) / 1000.0, 3) if last_ts > 0 else None
    last_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(last_ts / 1000.0)) if last_ts > 0 else None

    # Watchdog metadata (not full orch state)
    wd_ts = None
    try:
        if wd and wd.get("ts_ms") is not None:
            wd_ts = int(wd.get("ts_ms"))
    except Exception:
        wd_ts = None

    return {
        "schema_version": SCHEMA_VERSION,
        "updated_ms": now,

        "orch_present": bool(orch),
        "orch_path": orch_path,
        "orch_mode": orch_mode,
        "orch_only_labels": only_labels,
        "orch_started": started,
        "orch_skipped": skipped,

        "watchdog_present": bool(wd),
        "watchdog_path": wd_path,
        "watchdog_ts_ms": wd_ts,
        "watchdog_labels": wd.get("labels") if isinstance(wd.get("labels"), list) else [],

        "expected_accounts": EXPECTED_ACCOUNTS,

        # Keep legacy field for compatibility, but it is no longer "the truth"
        "outcomes_path": str(OUTCOMES_V1_GLOBAL),
        "outcomes_exists": OUTCOMES_V1_GLOBAL.exists(),
        "outcomes_last_ts_iso": last_iso,
        "outcomes_freshness_sec": freshness_sec,

        # New: aggregated outcomes truth
        "outcomes_agg_files": [str(p) for p in _load_outcomes_paths()],
        "outcomes_agg_total_rows": total_rows,

        "window": (window or "all").lower(),
        "bus_green_sec": BUS_GREEN_SEC,
        "bus_yellow_sec": BUS_YELLOW_SEC,
        "online_ttl_sec": ONLINE_TTL_SEC,
    }


def _funds_source(automation_mode: str) -> str:
    m = (automation_mode or "").strip().upper()
    if "LIVE" in m:
        return "REAL"
    if any(x in m for x in ("DRY", "PAPER", "SIM", "LEARN")):
        return "SIM"
    return "SIM"

if __name__ == "__main__":
    import argparse
    import json

    ap = argparse.ArgumentParser(description="Flashback dashboard hydrator CLI")
    ap.add_argument("--window", default="all", help="window to hydrate (default: all)")
    ap.add_argument("--meta-only", action="store_true", help="print meta only")
    args = ap.parse_args()

    meta = hydrate_dashboard_meta(window=args.window)
    if args.meta_only:
        print(json.dumps(meta, indent=2))
    else:
        rows = hydrate_dashboard_rows(window=args.window)
        print(json.dumps(meta, indent=2))
        print("---")
        print(json.dumps(rows, indent=2))

