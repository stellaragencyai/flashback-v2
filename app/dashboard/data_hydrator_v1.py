import json
import os
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from app.core.paper_account_state import load_paper_account_snapshot, paper_ledger_path

try:
    import yaml  # type: ignore
except Exception as exc:  # pragma: no cover
    yaml = None  # type: ignore
    YAML_IMPORT_ERROR = str(exc)
else:
    YAML_IMPORT_ERROR = None

# ------------------------------------------------------------
# Pathing: ALWAYS anchor from repo root (C:\Flashback)
# ------------------------------------------------------------
_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]  # ...\Flashback
STATE_ROOT = REPO_ROOT / "state"
STATE_APP = REPO_ROOT / "app" / "state"  # legacy / existing in your repo
CONFIG_ROOT = REPO_ROOT / "config"

SUBACCOUNTS_CONFIG_PATH = CONFIG_ROOT / "subaccounts.yaml"
FLEET_MANIFEST_PATH = CONFIG_ROOT / "fleet_manifest.yaml"
OPS_SNAPSHOT_CANDIDATES = [
    STATE_ROOT / "ops_snapshot.json",
]

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
FLEET_META_STATE_CANDIDATES = [
    STATE_APP / "fleet_meta_state.json",
    STATE_ROOT / "fleet_meta_state.json",
]
APPROVAL_QUEUE_CANDIDATES = [
    STATE_APP / "live_promotion_queue.json",
    STATE_ROOT / "live_promotion_queue.json",
]
REBUILD_PLAN_CANDIDATES = [
    STATE_APP / "fleet_rebuild_plan.json",
    STATE_ROOT / "fleet_rebuild_plan.json",
]
MAIN_UTILS_STATUS_CANDIDATES = [
    STATE_ROOT / "main_account" / "supervisor_status.json",
]
MAIN_UTILS_DEFAULT_WORKERS = {
    "guardian": {"enabled": True, "alive": False, "module": "app.trading.main_account.guardian"},
    "auto_tp_sl": {"enabled": True, "alive": False, "module": "app.trading.main_account.auto_tp_sl"},
    "profit_drip": {"enabled": True, "alive": False, "module": "app.trading.main_account.profit_drip"},
    "campaign_tier_notifier": {"enabled": True, "alive": False, "module": "app.trading.main_account.campaign_tier_notifier"},
}

SCHEMA_VERSION = 5

# TTL used for ONLINE determination (telemetry freshness)
ONLINE_TTL_SEC = float(os.getenv("SUBACCOUNTS_ONLINE_TTL_SEC", "60"))
SUPER_AI_LABEL = str(os.getenv("DASH_SUPER_AI_LABEL", "flashback06") or "flashback06").strip() or "flashback06"

# Dashboard bus health thresholds
BUS_GREEN_SEC = float(os.getenv("DASH_BUS_GREEN_SEC", "15"))
BUS_YELLOW_SEC = float(os.getenv("DASH_BUS_YELLOW_SEC", "60"))
MAIN_LIVE_BALANCE_CACHE_TTL_SEC = float(os.getenv("DASH_MAIN_BALANCE_CACHE_TTL_SEC", "30"))

# These are "critical" for live trading integrity. If dead while AI stack is enabled => RED.
CRITICAL_WORKERS = {
    "ws_switchboard",
    "trade_outcomes",
    "ai_events_spine",
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
_MAIN_LIVE_BALANCE_CACHE: Dict[str, Any] = {
    "updated_ms": 0,
    "balance": None,
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


def _safe_read_yaml(path: Path) -> Dict[str, Any]:
    if yaml is None or not path.exists():
        return {}
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8", errors="ignore"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _unwrap_statebus_struct(maybe_struct: Any) -> Dict[str, Any]:
    if not isinstance(maybe_struct, dict):
        return {}
    data = maybe_struct.get("data")
    if isinstance(data, dict):
        out: Dict[str, Any] = {}
        for key, value in data.items():
            if isinstance(value, dict) and "_value" in value:
                out[key] = value.get("_value")
            else:
                out[key] = value
        return out
    return dict(maybe_struct)


def _is_meaningful_text(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    return text.lower() not in {"unknown", "none", "null", "n/a"}


def _pick_text(*values: Any, default: str = "unknown") -> str:
    for value in values:
        if _is_meaningful_text(value):
            return str(value).strip()
    return default


def _pick_first_existing(candidates: List[Path]) -> Optional[Path]:
    for p in candidates:
        try:
            if p.exists():
                return p
        except Exception:
            continue
    return None


def _group_info(label: str, role: str = "") -> Tuple[str, str, int]:
    lab = str(label or "").strip().lower()
    role_norm = str(role or "").strip().lower()
    if role_norm.startswith("sniper"):
        return ("snipers", "Snipers", 3)
    if role_norm.startswith("htf"):
        return ("htf_core", "HTF Trend Core", 1)
    if role_norm.startswith("alt_"):
        return ("extractor_25x", "25x Altcoin Extractors", 2)
    if role_norm == "manual_discretionary":
        return ("manual", "Manual / Off", 4)
    if role_norm == "main":
        return ("main", "Main", 5)
    if lab in {"flashback01", "flashback02", "flashback03"}:
        return ("htf_core", "HTF Trend Core", 1)
    if lab in {"flashback04", "flashback05", "flashback06"}:
        return ("extractor_25x", "25x Altcoin Extractors", 2)
    if lab in {"flashback07", "flashback08", "flashback09"}:
        return ("snipers", "Snipers", 3)
    if lab == "flashback10":
        return ("manual", "Manual / Off", 4)
    if lab == "main":
        return ("main", "Main", 5)
    return ("other", "Other", 99)


def _load_subaccounts_config_map() -> Dict[str, Dict[str, Any]]:
    cfg = _safe_read_yaml(SUBACCOUNTS_CONFIG_PATH)
    out: Dict[str, Dict[str, Any]] = {}
    for label in EXPECTED_ACCOUNTS:
        row = cfg.get(label)
        if isinstance(row, dict):
            item = dict(row)
            item["account_label"] = label
            out[label] = item
    return out


def _load_strategy_cutover_map() -> Dict[str, int]:
    cfg = _safe_read_yaml(FLEET_MANIFEST_PATH)
    fleet = cfg.get("fleet") if isinstance(cfg, dict) else None
    out: Dict[str, int] = {}
    if not isinstance(fleet, list):
        return out
    for row in fleet:
        if not isinstance(row, dict):
            continue
        label = str(row.get("account_label") or "").strip()
        if not label:
            continue
        try:
            cutover_ts = int(row.get("strategy_cutover_ts_ms") or 0)
        except Exception:
            cutover_ts = 0
        if cutover_ts > 0:
            out[label] = cutover_ts
    return out


def _is_paper_mode(automation_mode: object) -> bool:
    m = str(automation_mode or "").strip().upper()
    return any(x in m for x in ("DRY", "PAPER", "SIM", "LEARN"))


def _load_fleet_manifest_map() -> Dict[str, Dict[str, Any]]:
    cfg = _safe_read_yaml(FLEET_MANIFEST_PATH)
    fleet = cfg.get("fleet")
    out: Dict[str, Dict[str, Any]] = {}
    if not isinstance(fleet, list):
        return out
    for row in fleet:
        if not isinstance(row, dict):
            continue
        label = str(row.get("account_label") or "").strip()
        if not label:
            continue
        out[label] = dict(row)
    return out


def _load_ops_snapshot() -> Tuple[Dict[str, Any], Optional[str]]:
    for cand in OPS_SNAPSHOT_CANDIDATES:
        try:
            if cand.exists():
                return _safe_read_json(cand), str(cand)
        except Exception:
            continue
    return {}, None


def _balance_topic_path(account_label: str) -> Path:
    return STATE_ROOT / f"balances_{str(account_label or '').strip().lower()}.json"


def _extract_balance_value(raw: Dict[str, Any]) -> Optional[float]:
    data = _unwrap_statebus_struct(raw)
    if not data:
        return None

    for key in ("USDT", "usdt"):
        asset = data.get(key)
        if isinstance(asset, dict):
            for field in ("equity_usdt", "totalEquity", "equity", "walletBalance", "wallet_balance", "availableBalance"):
                try:
                    if asset.get(field) not in (None, "", "null"):
                        return float(asset.get(field))
                except Exception:
                    continue

    for field in ("equity_usdt", "totalEquity", "equity", "walletBalance", "wallet_balance", "availableBalance"):
        try:
            if data.get(field) not in (None, "", "null"):
                return float(data.get(field))
        except Exception:
            continue

    return None


def _load_live_balance_snapshots(labels: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
    wanted = [str(v).strip() for v in (labels or EXPECTED_ACCOUNTS) if str(v).strip()]
    out: Dict[str, Dict[str, Any]] = {}

    for label in wanted:
        path = _balance_topic_path(label)
        raw = _safe_read_json(path)
        balance = _extract_balance_value(raw)
        if balance is None:
            continue

        updated_ts_ms = None
        try:
            if path.exists():
                updated_ts_ms = int(path.stat().st_mtime * 1000)
        except Exception:
            updated_ts_ms = None

        out[label] = {
            "balance": float(balance),
            "path": str(path),
            "updated_ts_ms": updated_ts_ms,
            "source": "state_bus_balance",
        }

    return out


def _load_main_live_balance_fallback(now_ms: int) -> Dict[str, Any]:
    cached_ms = int(_MAIN_LIVE_BALANCE_CACHE.get("updated_ms") or 0)
    if cached_ms > 0 and (now_ms - cached_ms) <= int(MAIN_LIVE_BALANCE_CACHE_TTL_SEC * 1000):
        balance = _MAIN_LIVE_BALANCE_CACHE.get("balance")
        return {
            "balance": balance,
            "path": "flashback_common.get_equity_usdt",
            "updated_ts_ms": cached_ms,
            "source": "live_api_fallback",
        } if balance is not None else {}

    balance = None
    try:
        from app.core.flashback_common import get_equity_usdt  # type: ignore

        balance = float(get_equity_usdt())
    except Exception:
        balance = None

    _MAIN_LIVE_BALANCE_CACHE["updated_ms"] = now_ms
    _MAIN_LIVE_BALANCE_CACHE["balance"] = balance

    return {
        "balance": balance,
        "path": "flashback_common.get_equity_usdt",
        "updated_ts_ms": now_ms,
        "source": "live_api_fallback",
    } if balance is not None else {}


def _load_live_runtime_by_label(now_ms: int) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    snap, snap_path = _load_ops_snapshot()
    components = snap.get("components") if isinstance(snap.get("components"), dict) else {}

    out: Dict[str, Dict[str, Any]] = {}
    for component_row in components.values():
        if not isinstance(component_row, dict):
            continue
        label = str(component_row.get("account_label") or "").strip()
        component_name = str(component_row.get("component") or "").strip()
        details = component_row.get("details") if isinstance(component_row.get("details"), dict) else {}
        if not label or not component_name:
            continue

        row = out.setdefault(label, {
            "label": label,
            "pid": None,
            "alive": False,
            "status": "MISSING",
            "reason": "OPS_SNAPSHOT_MISSING",
            "supervisor_ok": False,
            "supervisor_age_sec": None,
            "workers_running": 0,
            "workers_enabled": [],
            "workers_dead": [],
            "workers": {},
        })

        if component_name == "supervisor_ai_stack":
            ts_ms = component_row.get("ts_ms")
            try:
                ts_i = int(ts_ms)
            except Exception:
                ts_i = 0
            age_sec = round((now_ms - ts_i) / 1000.0, 3) if ts_i > 0 else None

            running = details.get("running") if isinstance(details.get("running"), list) else []
            enabled = details.get("enabled") if isinstance(details.get("enabled"), list) else []
            dead = details.get("dead") if isinstance(details.get("dead"), list) else []
            sup_ok = bool(component_row.get("ok"))

            row.update({
                "alive": sup_ok or bool(running),
                "status": "RUNNING" if sup_ok else "DEGRADED",
                "reason": None if sup_ok else "SUPERVISOR_NOT_OK",
                "supervisor_ok": sup_ok,
                "supervisor_age_sec": age_sec,
                "workers_running": len(running),
                "workers_enabled": [str(x) for x in enabled if str(x).strip()],
                "workers_dead": [str(x) for x in dead if str(x).strip()],
            })
            continue

        if component_name.startswith("worker_"):
            worker_name = component_name[len("worker_"):]
            worker_info = {
                "enabled": details.get("enabled"),
                "alive": details.get("alive"),
                "pid": details.get("pid"),
                "restart_count": details.get("restart_count"),
                "last_exitcode": details.get("last_exitcode"),
                "last_reason": details.get("last_reason"),
            }
            row["workers"][worker_name] = worker_info
            if worker_info.get("alive"):
                row["alive"] = True

    meta = {
        "ops_snapshot_present": bool(snap),
        "ops_snapshot_path": snap_path,
        "ops_snapshot_updated_ms": snap.get("updated_ms") if isinstance(snap, dict) else None,
    }
    return out, meta


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


def _lane_file_candidates(base_dir: str, name: str) -> List[Path]:
    """
    Support both lane layouts:
    - state/ai_events/<label>/<name>
    - state/ai_events_<label>/<name>
    - state/ai_events/<name> (main / legacy)
    """
    out: List[Path] = []
    seen: set[str] = set()
    patterns = [
        f"{base_dir}/{name}",
        f"{base_dir}/*/{name}",
        f"{base_dir}_*/{name}",
    ]
    for pattern in patterns:
        try:
            matches = STATE_ROOT.glob(pattern)
        except Exception:
            matches = []
        for path in matches:
            try:
                if not path.is_file():
                    continue
            except Exception:
                continue
            key = str(path).lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(path)
    return sorted(out, key=lambda p: str(p).lower())


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


def _utc_day_bucket(ts_ms: Any) -> Optional[str]:
    try:
        ts_i = int(ts_ms or 0)
    except Exception:
        return None
    if ts_i <= 0:
        return None
    try:
        return time.strftime("%Y-%m-%d", time.gmtime(ts_i / 1000.0))
    except Exception:
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
    out = _lane_file_candidates("ai_events", "outcomes.v1.jsonl")

    if OUTCOMES_V1_GLOBAL.exists() and OUTCOMES_V1_GLOBAL not in out:
        out.append(OUTCOMES_V1_GLOBAL)

    return sorted(out, key=lambda x: str(x).lower())


def _iter_all_outcomes_rows():
    for path in _load_outcomes_paths():
        yield from _iter_jsonl(path)


def _outcome_identity(row: Dict[str, Any]) -> Optional[str]:
    try:
        outcome_id = str(row.get("outcome_id") or "").strip()
        if outcome_id:
            return f"oid:{outcome_id}"

        trade_id = str(row.get("trade_id") or "").strip()
        ts = row.get("closed_ts_ms") or row.get("ts_ms") or row.get("ts") or ""
        reason = str(row.get("close_reason") or row.get("exit_reason") or "").strip()
        if trade_id:
            return f"tid:{trade_id}|ts:{ts}|reason:{reason}"
    except Exception:
        return None
    return None


def _load_paper_trade_stats(labels: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
    wanted = [str(v).strip() for v in (labels or EXPECTED_ACCOUNTS) if str(v).strip()]
    cutover_by_label = _load_strategy_cutover_map()
    out: Dict[str, Dict[str, Any]] = {}

    for label in wanted:
        path = paper_ledger_path(label, state_root=STATE_ROOT)
        raw = _safe_read_json(path)
        if not raw:
            continue

        closed = raw.get("closed_trades") if isinstance(raw.get("closed_trades"), list) else []
        cutover_ts = cutover_by_label.get(label)
        wins = 0
        losses = 0
        total = 0
        pnl_total = 0.0
        last_ts_ms = 0
        hold_sec_total = 0.0
        hold_count = 0
        symbols: Counter[str] = Counter()
        setups: Counter[str] = Counter()

        for trade in closed:
            if not isinstance(trade, dict):
                continue

            try:
                closed_ms = int(trade.get("closed_ms") or 0)
            except Exception:
                closed_ms = 0
            if cutover_ts and closed_ms and closed_ms < cutover_ts:
                continue

            try:
                pnl = float(trade.get("pnl_usd") or 0.0)
            except Exception:
                pnl = 0.0

            total += 1
            pnl_total += pnl
            if pnl > 0:
                wins += 1
            elif pnl < 0:
                losses += 1

            if closed_ms > last_ts_ms:
                last_ts_ms = closed_ms

            sym = str(trade.get("symbol") or "").strip().upper()
            if sym:
                symbols[sym] += 1

            setup = str(trade.get("setup_type") or "").strip().lower()
            if setup:
                setups[setup] += 1

            try:
                opened_ms = int(trade.get("opened_ms") or 0)
            except Exception:
                opened_ms = 0
            if opened_ms > 0 and closed_ms > opened_ms:
                hold_sec_total += max(0.0, (closed_ms - opened_ms) / 1000.0)
                hold_count += 1

        starting_equity = float(raw.get("starting_equity") or 1000.0)
        equity = float(raw.get("equity") or starting_equity)
        equity_delta = equity - starting_equity
        win_rate = (wins / total) if total > 0 else 0.0
        pnl_gap = pnl_total - equity_delta

        out[label] = {
            "total": total,
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "win_rate_pct": round(win_rate * 100.0, 2),
            "pnl_usd_total": round(pnl_total, 8),
            "pnl_usd_avg": round((pnl_total / total), 8) if total else 0.0,
            "last_ts_ms": last_ts_ms,
            "avg_time_to_outcome_sec": round((hold_sec_total / hold_count), 2) if hold_count > 0 else None,
            "avg_time_to_outcome_min": round((hold_sec_total / hold_count) / 60.0, 2) if hold_count > 0 else None,
            "symbol_mix": symbols.most_common(4),
            "setup_mix": setups.most_common(4),
            "starting_equity": starting_equity,
            "equity": equity,
            "equity_delta_usd": round(equity_delta, 8),
            "ledger_pnl_gap_usd": round(pnl_gap, 8),
            "ledger_pnl_ok": abs(pnl_gap) <= 0.01,
            "ledger_path": str(path),
        }

    return out


def _load_outcomes_stats(window: str) -> Dict[str, Dict[str, Any]]:
    now = _now_ms()
    start_ms = _parse_window_to_start_ms(window, now)
    cutover_by_label = _load_strategy_cutover_map()

    stats: Dict[str, Dict[str, Any]] = {}
    seen: set[str] = set()

    for row in _iter_all_outcomes_rows():
        if row.get("schema_version") != "outcome.v1":
            continue

        identity = _outcome_identity(row)
        if identity:
            if identity in seen:
                continue
            seen.add(identity)

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
        cutover_ts = cutover_by_label.get(acct)
        if cutover_ts and ts_i < cutover_ts:
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
                "symbols": Counter(),
                "setups": Counter(),
                "fill_days": set(),
                "outcome_days": set(),
                "hold_sec_total": 0.0,
                "hold_count": 0,
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

        symbol = str(row.get("symbol") or "").strip().upper()
        if symbol:
            s["symbols"][symbol] += 1

        setup = str(row.get("setup_type") or "").strip().lower()
        if setup:
            s["setups"][setup] += 1

        fill_day = _utc_day_bucket(row.get("opened_ts_ms") or row.get("ts_ms"))
        if fill_day:
            s["fill_days"].add(fill_day)

        outcome_day = _utc_day_bucket(ts_i)
        if outcome_day:
            s["outcome_days"].add(outcome_day)

        try:
            opened_ts = int(row.get("opened_ts_ms") or 0)
        except Exception:
            opened_ts = 0
        if opened_ts > 0 and ts_i > opened_ts:
            s["hold_sec_total"] += max(0.0, (ts_i - opened_ts) / 1000.0)
            s["hold_count"] += 1

    for acct, s in stats.items():
        t = int(s.get("total", 0) or 0)
        w = int(s.get("wins", 0) or 0)
        pnl_total = float(s.get("pnl_usd_total", 0.0) or 0.0)
        last_ts = int(s.get("last_ts_ms", 0) or 0)
        fill_day_count = len(s.get("fill_days") or [])
        outcome_day_count = len(s.get("outcome_days") or [])
        hold_count = int(s.get("hold_count", 0) or 0)
        hold_sec_total = float(s.get("hold_sec_total", 0.0) or 0.0)

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
        s["fills_per_day"] = round((t / fill_day_count), 2) if fill_day_count > 0 else 0.0
        s["outcomes_per_day"] = round((t / outcome_day_count), 2) if outcome_day_count > 0 else 0.0
        s["active_fill_days"] = fill_day_count
        s["active_outcome_days"] = outcome_day_count
        s["avg_time_to_outcome_sec"] = round((hold_sec_total / hold_count), 2) if hold_count > 0 else None
        s["avg_time_to_outcome_min"] = round((hold_sec_total / hold_count) / 60.0, 2) if hold_count > 0 else None
        try:
            s["top_outcome_modes"] = s["modes"].most_common(3)
        except Exception:
            s["top_outcome_modes"] = []
        try:
            s["symbol_mix"] = s["symbols"].most_common(4)
        except Exception:
            s["symbol_mix"] = []
        try:
            s["setup_mix"] = s["setups"].most_common(4)
        except Exception:
            s["setup_mix"] = []

    ltm_start = now - 24 * 3600 * 1000
    seen_24h: set[str] = set()
    for row in _iter_all_outcomes_rows():
        if row.get("schema_version") != "outcome.v1":
            continue

        identity = _outcome_identity(row)
        if identity:
            if identity in seen_24h:
                continue
            seen_24h.add(identity)

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
        cutover_ts = cutover_by_label.get(acct)
        if cutover_ts and ts_i < cutover_ts:
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
                "fills_per_day": 0.0,
                "outcomes_per_day": 0.0,
                "active_fill_days": 0,
                "active_outcome_days": 0,
                "avg_time_to_outcome_sec": None,
                "avg_time_to_outcome_min": None,
                "symbol_mix": [],
                "setup_mix": [],
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


def _load_fleet_meta_payload() -> Dict[str, Any]:
    path = _pick_first_existing(FLEET_META_STATE_CANDIDATES)
    return _safe_read_json(path) if path else {}


def _load_approval_queue_payload() -> Dict[str, Any]:
    path = _pick_first_existing(APPROVAL_QUEUE_CANDIDATES)
    return _safe_read_json(path) if path else {}


def _load_rebuild_plan_payload() -> Dict[str, Any]:
    path = _pick_first_existing(REBUILD_PLAN_CANDIDATES)
    return _safe_read_json(path) if path else {}


def _load_main_utils_status() -> Dict[str, Any]:
    path = _pick_first_existing(MAIN_UTILS_STATUS_CANDIDATES)
    status = _safe_read_json(path) if path else {}
    workers = status.get("workers") if isinstance(status.get("workers"), dict) else {}
    merged_workers: Dict[str, Any] = {
        name: dict(meta) for name, meta in MAIN_UTILS_DEFAULT_WORKERS.items()
    }
    for name, meta in workers.items():
        if isinstance(meta, dict):
            base = merged_workers.get(str(name), {})
            merged_workers[str(name)] = {**base, **meta}
    enabled_workers = sum(1 for meta in merged_workers.values() if bool((meta or {}).get("enabled", False)))
    running_workers = sum(1 for meta in merged_workers.values() if bool((meta or {}).get("alive", False)))
    return {
        **status,
        "enabled_workers": int(status.get("enabled_workers", enabled_workers) or enabled_workers),
        "running_workers": int(status.get("running_workers", running_workers) or running_workers),
        "all_running": bool(status.get("all_running", enabled_workers > 0 and enabled_workers == running_workers)),
        "workers": merged_workers,
    }


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
    now = _now_ms()

    subs_map = orch.get("subaccounts") if isinstance(orch.get("subaccounts"), dict) else {}
    procs_map = orch.get("procs") if isinstance(orch.get("procs"), dict) else {}
    config_map = _load_subaccounts_config_map()
    fleet_map = _load_fleet_manifest_map()
    runtime_map, runtime_meta = _load_live_runtime_by_label(now)

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

        **runtime_meta,
    }

    out: List[Dict[str, Any]] = []
    paper_snapshots = {uid: load_paper_account_snapshot(uid, state_root=STATE_ROOT) for uid in EXPECTED_ACCOUNTS}
    live_balance_snapshots = _load_live_balance_snapshots(EXPECTED_ACCOUNTS)
    if "main" not in live_balance_snapshots:
        main_balance = _load_main_live_balance_fallback(now)
        if main_balance:
            live_balance_snapshots["main"] = main_balance

    for uid in EXPECTED_ACCOUNTS:
        config_row = config_map.get(uid, {})
        fleet_row = fleet_map.get(uid, {})
        runtime_row = runtime_map.get(uid, {})
        paper = paper_snapshots.get(uid, {})
        live_balance = live_balance_snapshots.get(uid, {})
        runtime_balance_present = isinstance(runtime_row, dict) and runtime_row.get("balance") not in (None, "", "null")
        role = (
            fleet_row.get("role")
            or config_row.get("role")
            or runtime_row.get("role")
            or "unknown"
        )
        group_key, group_name, group_order = _group_info(uid, str(role))

        enabled = config_row.get("enabled")
        enable_ai_stack = bool(config_row.get("enable_ai_stack", False))
        should_run = bool(enabled) and enable_ai_stack if isinstance(enabled, bool) else None

        strategy_name = (
            fleet_row.get("strategy_name")
            or config_row.get("strategy_name")
            or "unknown"
        )
        role = fleet_row.get("role") or config_row.get("role") or "unknown"
        automation_mode = fleet_row.get("automation_mode") or config_row.get("automation_mode") or "UNKNOWN"

        base: Dict[str, Any] = {
            "subaccount_uid": uid,
            "subaccount_name": uid,
            "account_label": uid,
            "label": uid,
            "enabled": enabled if isinstance(enabled, bool) else None,
            "enable_ai_stack": enable_ai_stack,
            "automation_mode": automation_mode,
            "should_run": should_run,
            "online": False,
            "status": "SKIPPED" if should_run is False else "MISSING",
            "reason": "DISABLED_IN_CONFIG" if should_run is False else "NOT_IN_RUNTIME_STATE",
            "strategy": {"name": strategy_name, "version": None},
            "current_strategy": strategy_name,
            "strategy_name": strategy_name,
            "role": role,
            "risk_pct": fleet_row.get("risk_pct", 0.0),
            "symbols": fleet_row.get("symbols") or [],
            "timeframes": fleet_row.get("timeframes") or [],
            "setup_types": fleet_row.get("setup_types") or [],
            "promotion_rules": fleet_row.get("promotion_rules") or {},
            "pinned_symbol": config_row.get("pinned_symbol"),
            "group_key": group_key,
            "group_name": group_name,
            "group_order": group_order,
            "telegram_enabled": bool(config_row.get("telegram_channel")),
            "n_bucket": 0,
            "autonomy_ready": False,
            "balance": float(live_balance.get("balance", 0.0) or 0.0),
            "live_balance": live_balance.get("balance"),
            "live_balance_path": live_balance.get("path"),
            "live_balance_updated_ts_ms": live_balance.get("updated_ts_ms"),
            "live_balance_source": live_balance.get("source"),
            "runtime_balance_present": runtime_balance_present,
            "runtime_balance": float(runtime_row.get("balance", 0.0) or 0.0) if runtime_balance_present else None,
            "paper_balance": float(paper.get("equity", 1000.0) or 1000.0),
            "paper_starting_equity": float(paper.get("starting_equity", 1000.0) or 1000.0),
            "paper_open_positions": int(paper.get("open_positions_count", 0) or 0),
            "paper_closed_trades": int(paper.get("closed_trades_count", 0) or 0),
            "paper_ledger_path": paper.get("path"),
            "paper_ledger_exists": bool(paper.get("exists", False)),
            "paper_ledger_updated_ts_ms": paper.get("updated_ts_ms"),
            "supervisor_ok": False,
            "supervisor_age_sec": None,
            "workers_running": 0,
            "workers_enabled": [],
            "workers_dead": [],
            "workers": {},
            "pid": None,
            "alive": False,
            "stdout_log": str(STATE_ROOT / "orchestrator_logs" / f"{uid}.stdout.log"),
            "stderr_log": str(STATE_ROOT / "orchestrator_logs" / f"{uid}.stderr.log"),
            "proc_started_ts_ms": None,
        }

        if isinstance(subs_map, dict) and uid in subs_map and isinstance(subs_map[uid], dict):
            base.update(subs_map[uid])

        proc = procs_map.get(uid) if isinstance(procs_map, dict) else None
        if isinstance(proc, dict):
            base["pid"] = proc.get("pid")
            base["alive"] = bool(proc.get("alive"))
            base["stdout_log"] = proc.get("stdout_log") or base.get("stdout_log")
            base["stderr_log"] = proc.get("stderr_log") or base.get("stderr_log")
            base["proc_started_ts_ms"] = proc.get("started_ts_ms")

        if runtime_row:
            base.update(runtime_row)
            if live_balance.get("balance") is not None:
                base["balance"] = float(live_balance.get("balance", 0.0) or 0.0)
                base["live_balance"] = live_balance.get("balance")
                base["live_balance_path"] = live_balance.get("path")
                base["live_balance_updated_ts_ms"] = live_balance.get("updated_ts_ms")
                base["live_balance_source"] = live_balance.get("source")
            if runtime_balance_present:
                base["runtime_balance_present"] = True
                base["runtime_balance"] = float(runtime_row.get("balance", 0.0) or 0.0)

        # Re-apply canonical config truth after runtime/orchestrator merges so
        # stale runtime state cannot degrade the dashboard into UNKNOWN labels.
        enabled_final = enabled if isinstance(enabled, bool) else base.get("enabled")
        enable_ai_stack_final = bool(config_row.get("enable_ai_stack", base.get("enable_ai_stack", False)))
        strategy_final = _pick_text(
            base.get("current_strategy"),
            base.get("strategy_name"),
            (base.get("strategy") or {}).get("name") if isinstance(base.get("strategy"), dict) else None,
            fleet_row.get("strategy_name"),
            config_row.get("strategy_name"),
        )
        role_final = _pick_text(
            base.get("role"),
            fleet_row.get("role"),
            config_row.get("role"),
        )
        automation_mode_final = _pick_text(
            base.get("automation_mode"),
            fleet_row.get("automation_mode"),
            config_row.get("automation_mode"),
            default="OFF",
        )
        should_run_final = (
            bool(enabled_final) and enable_ai_stack_final
            if isinstance(enabled_final, bool)
            else base.get("should_run")
        )

        base["enabled"] = enabled_final if isinstance(enabled_final, bool) else None
        base["enable_ai_stack"] = enable_ai_stack_final
        base["should_run"] = should_run_final if isinstance(should_run_final, bool) else None
        base["automation_mode"] = automation_mode_final
        base["role"] = role_final
        base["strategy"] = {"name": strategy_final, "version": (base.get("strategy") or {}).get("version") if isinstance(base.get("strategy"), dict) else None}
        base["current_strategy"] = strategy_final
        base["strategy_name"] = strategy_final
        if not base.get("symbols"):
            base["symbols"] = fleet_row.get("symbols") or []
        if not base.get("timeframes"):
            base["timeframes"] = fleet_row.get("timeframes") or []
        if not base.get("setup_types"):
            base["setup_types"] = fleet_row.get("setup_types") or []
        if not base.get("promotion_rules"):
            base["promotion_rules"] = fleet_row.get("promotion_rules") or {}
        if not base.get("pinned_symbol"):
            base["pinned_symbol"] = config_row.get("pinned_symbol")
        base["telegram_enabled"] = bool(config_row.get("telegram_channel"))
        if not base.get("risk_pct"):
            base["risk_pct"] = float(fleet_row.get("risk_pct", 0.0) or 0.0)
        base["paper_trades_entered"] = int(paper.get("trades_entered_count", 0) or 0)

        if not base.get("workers_enabled") and isinstance(base.get("workers"), dict):
            base["workers_enabled"] = [
                name for name, info in base["workers"].items()
                if isinstance(info, dict) and info.get("enabled")
            ]
        if not base.get("workers_dead") and isinstance(base.get("workers"), dict):
            base["workers_dead"] = [
                name for name, info in base["workers"].items()
                if isinstance(info, dict) and info.get("enabled") and not info.get("alive")
            ]
        if not base.get("workers_running") and isinstance(base.get("workers"), dict):
            base["workers_running"] = sum(
                1 for info in base["workers"].values()
                if isinstance(info, dict) and info.get("alive")
            )

        out.append(base)

    return out, meta


def _funds_source_for_account(account_label: str, automation_mode: str) -> str:
    if str(account_label or "").strip().lower() == "main":
        return "REAL"
    return _funds_source(automation_mode)


def hydrate_dashboard_rows(window: str = "all") -> List[Dict[str, Any]]:
    now = _now_ms()

    base_rows, orch_meta = _build_base_rows_from_orchestrator()
    _merge_governance(base_rows)
    outcomes = _load_outcomes_stats(window)
    paper_trade_stats = _load_paper_trade_stats([
        str(
            sa.get("subaccount_uid")
            or sa.get("label")
            or sa.get("subaccount_name")
            or ""
        ).strip()
        for sa in base_rows
        if isinstance(sa, dict)
    ])
    main_utils = _load_main_utils_status()

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

        analytics_total_trades = int(o.get("total", 0) or 0)
        analytics_wins = int(o.get("wins", 0) or 0)
        analytics_losses = int(o.get("losses", 0) or 0)
        analytics_win_rate = float(o.get("win_rate", 0.0) or 0.0)  # 0..1
        analytics_win_rate_pct = float(
            o.get("win_rate_pct", round(analytics_win_rate * 100.0, 2)) if analytics_total_trades else 0.0
        )
        analytics_pnl_total = float(o.get("pnl_usd_total", 0.0) or 0.0)
        analytics_pnl_avg = float(o.get("pnl_usd_avg", 0.0) or 0.0)
        promotion_rules = sa.get("promotion_rules") if isinstance(sa.get("promotion_rules"), dict) else {}
        min_outcomes_to_live = int(promotion_rules.get("min_trades", 0) or 0)

        orch_mode = str(orch_meta.get("orch_mode") or "UNKNOWN").strip().upper()
        top_outcome_modes = o.get("top_outcome_modes") or []
        outcome_mode_primary = top_outcome_modes[0][0] if (isinstance(top_outcome_modes, list) and top_outcome_modes) else None
        mode_mismatch = bool(outcome_mode_primary) and (orch_mode != "UNKNOWN") and (str(outcome_mode_primary).upper() != orch_mode)

        # Normalize enabled/should_run to bool fields for front-end compatibility
        enabled_bool = bool(enabled) if enabled is not None else False
        should_run_bool = bool(should_run) if should_run is not None else False

        automation_mode = sa.get("automation_mode") or "unknown"
        funds_source = _funds_source_for_account(uid, str(automation_mode))
        paper_mode = _is_paper_mode(automation_mode)
        live_mode = funds_source == "REAL"
        manual_mode = not enable_ai_stack
        balance_display_mode = "SHOW" if (funds_source == "REAL" or paper_mode) else "HIDE"
        paper_balance = float(sa.get("paper_balance", 1000.0) or 1000.0) if paper_mode else 0.0
        live_balance = sa.get("live_balance")
        runtime_balance = sa.get("runtime_balance")
        balance_value = paper_balance if paper_mode else float(
            live_balance if live_balance is not None else (runtime_balance if runtime_balance is not None else sa.get("balance", 0.0) or 0.0)
        )
        paper_starting_equity = float(sa.get("paper_starting_equity", 1000.0) or 1000.0) if paper_mode else None
        ledger_open_positions = int(sa.get("paper_open_positions", 0) or 0) if paper_mode else 0
        ledger_closed_trades = int(sa.get("paper_closed_trades", 0) or 0) if paper_mode else 0
        ledger_trades_entered = int(sa.get("paper_trades_entered", 0) or 0) if paper_mode else analytics_total_trades
        paper_truth = paper_trade_stats.get(uid, {}) if paper_mode else {}

        total_trades = analytics_total_trades
        wins = analytics_wins
        losses = analytics_losses
        win_rate = analytics_win_rate
        win_rate_pct = analytics_win_rate_pct
        pnl_total = analytics_pnl_total
        pnl_avg = analytics_pnl_avg
        equity_delta_usd = None
        reconciliation_gap_usd = None
        reconciliation_count_gap = None
        reconciliation_ok = True
        reconciliation_summary = "analytics-only"
        balance_source = "paper_ledger" if paper_mode else "none"

        if paper_mode:
            equity_delta_usd = round(balance_value - float(paper_starting_equity or 0.0), 8)
            pnl_total = float(equity_delta_usd)
            if paper_truth:
                total_trades = int(paper_truth.get("total", 0) or 0)
                wins = int(paper_truth.get("wins", 0) or 0)
                losses = int(paper_truth.get("losses", 0) or 0)
                win_rate = float(paper_truth.get("win_rate", 0.0) or 0.0)
                win_rate_pct = float(
                    paper_truth.get("win_rate_pct", round(win_rate * 100.0, 2)) if total_trades else 0.0
                )
            else:
                win_rate_pct = round(win_rate * 100.0, 2) if total_trades else 0.0
            pnl_avg = (pnl_total / total_trades) if total_trades > 0 else 0.0
            reconciliation_gap_usd = round(analytics_pnl_total - pnl_total, 8)
            reconciliation_count_gap = analytics_total_trades - total_trades
            reconciliation_ok = abs(float(reconciliation_gap_usd)) <= 1.0 and int(reconciliation_count_gap) == 0
            reconciliation_summary = (
                "ok"
                if reconciliation_ok
                else f"gap ${float(reconciliation_gap_usd):.2f} | trades {int(reconciliation_count_gap):+d}"
            )
        elif live_mode:
            if live_balance is not None:
                balance_source = str(sa.get("live_balance_source") or "live_balance")
            elif bool(sa.get("runtime_balance_present")):
                balance_source = "runtime_balance"

        trustworthy_outcomes = total_trades if paper_mode else analytics_total_trades
        outcomes_remaining = max(0, min_outcomes_to_live - trustworthy_outcomes) if min_outcomes_to_live > 0 else 0

        row = {
            "account": uid,
            "account_label": uid,
            "subaccount_uid": uid,
            "subaccount_name": uid,

            "strategy": strategy_name,
            "strategy_name": strategy_name,
            "current_strategy": strategy_name,
            "strategy_version": strat.get("version") if isinstance(strat, dict) else None,
            "automation_mode": automation_mode,
            "funds_source": funds_source,
            "is_live_mode": live_mode,
            "is_paper_mode": paper_mode,
            "is_manual_mode": manual_mode,
            "is_ai_lane": enable_ai_stack,
            "balance_display_mode": balance_display_mode,
            "role": sa.get("role") or "unknown",
            "group_key": sa.get("group_key") or "other",
            "group_name": sa.get("group_name") or "Other",
            "group_order": int(sa.get("group_order", 99) or 99),
            "risk_pct": float(sa.get("risk_pct", 0.0) or 0.0),
            "symbols": sa.get("symbols") or [],
            "timeframes": sa.get("timeframes") or [],
            "setup_types": sa.get("setup_types") or [],
            "pinned_symbol": sa.get("pinned_symbol"),
            "promotion_rules": promotion_rules,
            "n_outcomes": trustworthy_outcomes,
            "n_outcomes_source": "paper_ledger" if paper_mode else "outcomes_v1",
            "min_outcomes_to_live": min_outcomes_to_live,
            "outcomes_remaining_to_live": outcomes_remaining,
            "ready_for_live_by_n": bool(min_outcomes_to_live > 0 and trustworthy_outcomes >= min_outcomes_to_live),

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
            "supervisor_ok": sa.get("supervisor_ok"),
            "supervisor_age_sec": sa.get("supervisor_age_sec"),

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

            "balance": balance_value,
            "live_balance": live_balance,
            "live_balance_path": sa.get("live_balance_path"),
            "live_balance_updated_ts_ms": sa.get("live_balance_updated_ts_ms"),
            "live_balance_source": sa.get("live_balance_source"),
            "paper_balance": paper_balance if paper_mode else None,
            "ledger_balance": paper_balance if paper_mode else None,
            "paper_starting_equity": paper_starting_equity,
            "paper_open_positions": ledger_open_positions,
            "paper_closed_trades": ledger_closed_trades,
            "paper_trades_entered": ledger_trades_entered,
            "ledger_open_positions": ledger_open_positions,
            "ledger_closed_trades": ledger_closed_trades,
            "ledger_trades_entered": ledger_trades_entered,
            "ledger_metrics_source": "paper_ledger" if paper_mode else None,
            "paper_ledger_path": sa.get("paper_ledger_path"),
            "paper_ledger_exists": bool(sa.get("paper_ledger_exists", False)) if paper_mode else False,
            "paper_ledger_updated_ts_ms": sa.get("paper_ledger_updated_ts_ms"),
            "n_bucket": int(sa.get("n_bucket", 0) or 0),
            "autonomy_ready": bool(sa.get("autonomy_ready", False)),
            "governance_decision": sa.get("governance_decision"),
            "governance_summary": sa.get("governance_summary"),
            "governance_reasons": sa.get("governance_reasons") or [],
            "governance_confidence": float(sa.get("governance_confidence", 0.0) or 0.0),
            "promotion_ready": bool(sa.get("promotion_ready", False)),
            "early_promote_candidate": bool(sa.get("early_promote_candidate", False)),
            "early_cull_candidate": bool(sa.get("early_cull_candidate", False)),
            "sample_progress": float(sa.get("sample_progress", 0.0) or 0.0),
            "win_rate_lower": sa.get("win_rate_lower"),
            "win_rate_upper": sa.get("win_rate_upper"),
            "portfolio_uniqueness_score": float(sa.get("portfolio_uniqueness_score", 0.0) or 0.0),
            "portfolio_contribution_score": float(sa.get("portfolio_contribution_score", 0.0) or 0.0),
            "insight_yield_score": float(sa.get("insight_yield_score", 0.0) or 0.0),
            "graduation_stage": sa.get("graduation_stage"),
            "commercial_action": sa.get("commercial_action"),
            "family_peer_count": int(sa.get("family_peer_count", 0) or 0),
            "kill_fast_candidate": bool(sa.get("kill_fast_candidate", False)),
            "shadow_live_ready": bool(sa.get("shadow_live_ready", False)),
            "capital_action": sa.get("capital_action"),
            "recommended_risk_mult": float(sa.get("recommended_risk_mult", 0.0) or 0.0),
            "recommended_risk_pct": float(sa.get("recommended_risk_pct", 0.0) or 0.0),
            "executive_rank_score": float(sa.get("executive_rank_score", 0.0) or 0.0),
            "correlation_score": float(sa.get("correlation_score", 0.0) or 0.0),
            "correlation_penalty": float(sa.get("correlation_penalty", 0.0) or 0.0),
            "correlated_peers": sa.get("correlated_peers") or [],
            "correlation_cluster": sa.get("correlation_cluster"),
            "regime_capital_weight": float(sa.get("regime_capital_weight", 1.0) or 1.0),
            "half_life_score": float(sa.get("half_life_score", 0.0) or 0.0),
            "approval_required": bool(sa.get("approval_required", False)),
            "approval_priority": sa.get("approval_priority"),
            "approval_mode": sa.get("approval_mode"),
            "approval_suppressed": bool(sa.get("approval_suppressed", False)),
            "approval_signature": sa.get("approval_signature"),
            "operator_decision": sa.get("operator_decision"),
            "meta_brain_summary": sa.get("meta_brain_summary"),
            "allocator_summary": sa.get("allocator_summary"),
            "telegram_enabled": bool(sa.get("telegram_enabled", False)),
            "main_utils_enabled": 0,
            "main_utils_running": 0,
            "main_utils_all_running": False,
            "main_utils_workers": {},

            "total_trades": total_trades,
            "wins": wins,
            "losses": losses,
            "outcome_total_trades": analytics_total_trades,
            "outcome_trade_count": analytics_total_trades,
            "outcome_wins": analytics_wins,
            "outcome_losses": analytics_losses,
            "outcome_n": analytics_total_trades,
            "outcome_metrics_source": "outcomes_v1",
            "win_rate": win_rate,
            "win_rate_pct": win_rate_pct,
            "pnl_usd_total": pnl_total,
            "pnl_usd_avg": pnl_avg,
            "analytics_total_trades": analytics_total_trades,
            "analytics_wins": analytics_wins,
            "analytics_losses": analytics_losses,
            "analytics_win_rate": analytics_win_rate,
            "analytics_win_rate_pct": analytics_win_rate_pct,
            "analytics_pnl_usd_total": analytics_pnl_total,
            "analytics_pnl_usd_avg": analytics_pnl_avg,
            "balance_source": balance_source,
            "equity_delta_usd": equity_delta_usd,
            "reconciliation_gap_usd": reconciliation_gap_usd,
            "reconciliation_count_gap": reconciliation_count_gap,
            "reconciliation_ok": reconciliation_ok,
            "reconciliation_summary": reconciliation_summary,
            "outcomes_last_ts_iso": o.get("outcomes_last_ts_iso", None),
            "outcomes_freshness_sec": o.get("outcomes_freshness_sec", None),

            "top_outcome_modes": top_outcome_modes,
            "outcome_mode_primary": outcome_mode_primary,
            "mode_mismatch": mode_mismatch,

            "ltm_trades_24h": int(o.get("ltm_trades_24h", 0) or 0),
            "fills_per_day": float(o.get("fills_per_day", 0.0) or 0.0),
            "outcomes_per_day": float(o.get("outcomes_per_day", 0.0) or 0.0),
            "active_fill_days": int(o.get("active_fill_days", 0) or 0),
            "active_outcome_days": int(o.get("active_outcome_days", 0) or 0),
            "avg_time_to_outcome_sec": o.get("avg_time_to_outcome_sec"),
            "avg_time_to_outcome_min": o.get("avg_time_to_outcome_min"),
            "symbol_mix": o.get("symbol_mix") or [],
            "setup_mix": o.get("setup_mix") or [],

            **bus,

            "window": (window or "all").lower(),
            "updated_ms": now,
            "schema_version": SCHEMA_VERSION,
        }

        if uid == "main":
            workers = main_utils.get("workers") if isinstance(main_utils.get("workers"), dict) else {}
            row["main_utils_enabled"] = int(main_utils.get("enabled_workers", 0) or 0)
            row["main_utils_running"] = int(main_utils.get("running_workers", 0) or 0)
            row["main_utils_all_running"] = bool(main_utils.get("all_running", False))
            row["main_utils_workers"] = workers

        rows.append(row)

    order = {a: i for i, a in enumerate(EXPECTED_ACCOUNTS)}
    rows.sort(key=lambda r: order.get(str(r.get("account_label") or r.get("account") or ""), 999))
    return rows


def hydrate_dashboard_meta(window: str = "all") -> Dict[str, Any]:
    orch, orch_path = _load_orchestrator_state()
    wd, wd_path = _load_orchestrator_watchdog()
    cfg_map = _load_subaccounts_config_map()
    fleet_meta = _load_fleet_meta_payload()
    approval_payload = _load_approval_queue_payload()
    rebuild_payload = _load_rebuild_plan_payload()

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

    present_accounts = [label for label in EXPECTED_ACCOUNTS if label in cfg_map]
    missing_accounts = [label for label in EXPECTED_ACCOUNTS if label not in cfg_map]
    enabled_accounts = [
        label for label in present_accounts
        if bool((cfg_map.get(label) or {}).get("enabled"))
    ]
    disabled_accounts = [label for label in present_accounts if label not in enabled_accounts]
    ai_stack_accounts = [
        label for label in present_accounts
        if bool((cfg_map.get(label) or {}).get("enable_ai_stack"))
    ]
    paper_accounts = [
        label for label in present_accounts
        if _is_paper_mode((cfg_map.get(label) or {}).get("automation_mode"))
    ]
    live_accounts = [
        label for label in present_accounts
        if _funds_source(str((cfg_map.get(label) or {}).get("automation_mode") or "")) == "REAL"
    ]
    portfolio_summary = fleet_meta.get("portfolio_summary") if isinstance(fleet_meta.get("portfolio_summary"), dict) else {}
    approval_queue = approval_payload.get("queue") if isinstance(approval_payload.get("queue"), list) else []
    if not approval_queue and isinstance(fleet_meta.get("approval_queue"), list):
        approval_queue = fleet_meta.get("approval_queue") or []
    rebuild_plan = rebuild_payload if isinstance(rebuild_payload, dict) else {}
    rebuild_targets = rebuild_plan.get("targets") if isinstance(rebuild_plan.get("targets"), list) else []

    return {
        "schema_version": SCHEMA_VERSION,
        "updated_ms": now,
        "yaml_available": yaml is not None,
        "yaml_import_error": YAML_IMPORT_ERROR,
        "config_loader_ok": yaml is not None and bool(cfg_map),

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
        "present_accounts": present_accounts,
        "present_accounts_count": len(present_accounts),
        "missing_accounts": missing_accounts,
        "missing_accounts_count": len(missing_accounts),
        "enabled_accounts": enabled_accounts,
        "enabled_accounts_count": len(enabled_accounts),
        "disabled_accounts": disabled_accounts,
        "disabled_accounts_count": len(disabled_accounts),
        "ai_stack_accounts": ai_stack_accounts,
        "ai_stack_accounts_count": len(ai_stack_accounts),
        "paper_accounts": paper_accounts,
        "paper_accounts_count": len(paper_accounts),
        "live_accounts": live_accounts,
        "live_accounts_count": len(live_accounts),
        "row_count": len(present_accounts),
        "portfolio_summary": portfolio_summary,
        "approval_queue": approval_queue,
        "approval_queue_count": len(approval_queue),
        "rebuild_plan": rebuild_plan,
        "rebuild_targets": rebuild_targets,
        "rebuild_count": len(rebuild_targets),
        "super_ai_label": SUPER_AI_LABEL,

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

