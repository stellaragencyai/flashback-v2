from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Tuple, List, Optional

ROOT = Path(__file__).resolve().parents[2]
STATE = ROOT / "state"
FLEET_SNAPSHOT = STATE / "fleet_snapshot.json"
OUT_PATH = STATE / "fleet_degraded.json"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _env_int(name: str, default: str) -> int:
    try:
        return int(str(os.getenv(name, default)).strip())
    except Exception:
        return int(default)


def _env_csv(name: str) -> Tuple[str, ...]:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return tuple()
    items = [x.strip() for x in raw.split(",") if x.strip()]
    return tuple(sorted(set(items)))


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


@dataclass
class Thresholds:
    # ms (fleet snapshot bus ages are age_ms)
    pos_ms: int
    ob_ms: int
    trades_ms: int
    ops_ms: int

    restarts_n: int
    restarts_window_sec: int


def _thresholds() -> Thresholds:
    return Thresholds(
        pos_ms=_env_int("DEG_POS_STALE_MS", "5000"),
        ob_ms=_env_int("DEG_OB_STALE_MS", "5000"),
        trades_ms=_env_int("DEG_TRADES_STALE_MS", "10000"),
        ops_ms=_env_int("DEG_OPS_STALE_MS", "15000"),
        restarts_n=_env_int("DEG_RESTARTS_N", "3"),
        restarts_window_sec=_env_int("DEG_RESTARTS_WINDOW_SEC", "60"),
    )


def _rank_mode(mode: str) -> int:
    """
    Higher = stricter/live-er.
    OFF < LEARN_DRY < LIVE_CANARY < LIVE
    Unknown treated as LIVE-ish.
    """
    m = (mode or "").strip().upper()
    if m in ("OFF",):
        return 0
    if m in ("LEARN_DRY", "EXEC_DRY_RUN", "PAPER"):
        return 1
    if m in ("LIVE_CANARY",):
        return 2
    if m in ("LIVE",):
        return 3
    return 3


def _choose_fleet_mode_from_snapshot(subs: Dict[str, Any]) -> str:
    """
    Determine fleet mode by inspecting active subs.
    - If no subs are configured to run (should_run true), fleet is OFF.
    - Otherwise choose the strictest mode among should_run subs.
    """
    active_modes: List[str] = []
    for _label, info in subs.items():
        if not isinstance(info, dict):
            continue
        should_run = bool(info.get("should_run"))
        if not should_run:
            continue
        m = str(info.get("automation_mode") or "").strip() or "LIVE"
        active_modes.append(m)

    if not active_modes:
        return "OFF"

    # Strictest mode wins
    best = sorted(active_modes, key=_rank_mode, reverse=True)[0]
    b = best.strip().upper()
    if b in ("EXEC_DRY_RUN", "PAPER"):
        return "LEARN_DRY"
    return b


def evaluate(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    th = _thresholds()
    ts_ms = int(snapshot.get("ts_ms") or 0)
    buses = snapshot.get("buses") or {}
    fleet = snapshot.get("fleet") or {}
    subs = (fleet.get("subs") or {})

    def _age_ms(name: str) -> Optional[int]:
        v = buses.get(name) or {}
        try:
            return int(v.get("age_ms")) if v.get("age_ms") is not None else None
        except Exception:
            return None

    # Observed ages (ms)
    age_pos = _age_ms("positions_bus")
    age_ob = _age_ms("orderbook_bus")
    age_trades = _age_ms("trades_bus")
    age_ops = _age_ms("ops_snapshot")

    # Contract-driven bus classification (authoritative)
    contract_details: Dict[str, Any] = {}
    contract_fail: List[str] = []
    contract_warn: List[str] = []

    try:
        from app.ops.fleet_runtime_contract import Thresholds as CThresholds, validate_runtime_contract  # type: ignore

        # Fleet mode: env override wins; otherwise derived from snapshot subs.
        env_mode = (os.getenv("FLEET_AUTOMATION_MODE", "") or "").strip()
        fleet_mode = env_mode.strip().upper() if env_mode else _choose_fleet_mode_from_snapshot(subs)

        cth = CThresholds(
            max_positions_age=float(th.pos_ms) / 1000.0,
            max_trades_age=float(th.trades_ms) / 1000.0,
            max_orderbook_age=float(th.ob_ms) / 1000.0,
            # These are ignored at fleet layer
            max_ws_hb_age=999999.0,
            max_memory_snapshot_age=999999.0,
            max_decisions_age=999999.0,
        )

        # NOTE: Fleet bus-level evaluation ignores ws heartbeat/memory/decisions because
        # the fleet snapshot doesn't contain those signals at the bus level.
        contract = validate_runtime_contract(
            mode=fleet_mode,
            observed={
                "positions_age": age_pos,
                "trades_age": age_trades,
                "orderbook_age": age_ob,
                "ws_hb_age": None,
                "memory_exists": True,
                "memory_age": 0,
                "memory_parse_ok": True,
                "memory_count": 1,
                "decisions_exists": True,
                "decisions_age": 0,
                "decisions_tail_parse_ok": True,
                "decisions_schema_valid": True,
            },
            thresholds=cth,
            age_unit="ms",
            ignore_ws_heartbeat=True,
            ignore_memory=True,
            ignore_decisions=True,
        )
        contract_details = contract.details
        contract_fail = [c.value for c in contract.faults_fail]
        contract_warn = [c.value for c in contract.faults_warn]
    except Exception as e:
        contract_details = {"error": f"contract_validator_failed:{e!r}"}
        contract_fail = [contract_details["error"]]

    # Per-label degraded (process-level)
    degraded: Dict[str, Any] = {}
    critical_labels = _env_csv("DEG_CRITICAL_LABELS")  # if empty -> any label can degrade fleet, else only these

    for label, info in subs.items():
        if not isinstance(info, dict):
            continue
        should_run = bool(info.get("should_run"))
        alive = bool(info.get("alive"))
        enabled = bool(info.get("enabled"))
        enable_ai_stack = bool(info.get("enable_ai_stack"))
        restart_count = int(info.get("restart_count") or 0)
        automation_mode = str(info.get("automation_mode") or "").strip() or None

        reasons: List[str] = []

        if should_run and not alive:
            reasons.append("should_run_but_dead")

        # Restart loop policy (still crude; Enhancement #2 will replace with timestamps+window)
        if restart_count >= th.restarts_n:
            reasons.append(f"restart_loop restart_count={restart_count} >= {th.restarts_n}")

        if reasons:
            degraded[label] = {
                "reasons": reasons,
                "should_run": should_run,
                "alive": alive,
                "enabled": enabled,
                "enable_ai_stack": enable_ai_stack,
                "restart_count": restart_count,
                "automation_mode": automation_mode,
            }

    # Fleet degraded logic
    fleet_reasons: List[str] = []

    # Contract failures are fleet-level issues, but ONLY meaningful if fleet_mode isn't OFF.
    # If fleet_mode is OFF, bus staleness should not fail the fleet; it should WARN.
    fm = str(contract_details.get("mode") or "").strip().upper()
    if fm == "OFF":
        # Downgrade any contract_fail to warn when fleet is OFF
        if contract_fail:
            fleet_reasons.append(f"contract_warn={contract_fail}")
        if contract_warn:
            fleet_reasons.append(f"contract_warn={contract_warn}")
        contract_fail = []
    else:
        if contract_fail:
            fleet_reasons.append(f"contract_fail={contract_fail}")
        if contract_warn:
            fleet_reasons.append(f"contract_warn={contract_warn}")

    if critical_labels:
        crit_bad = [l for l in degraded.keys() if l in critical_labels]
        if crit_bad:
            fleet_reasons.append(f"critical_labels_degraded={crit_bad}")
    else:
        if degraded:
            fleet_reasons.append(f"labels_degraded={sorted(degraded.keys())}")

    # Fleet ok rules:
    # - If fleet_mode OFF => ok unless labeled degraded rules triggered (process issues)
    # - Else => ok only if no contract_fail and no degraded labels gating fleet
    if fm == "OFF":
        fleet_ok = True
        if critical_labels:
            fleet_ok = not any(r.startswith("critical_labels_degraded=") for r in fleet_reasons)
        else:
            fleet_ok = not any(r.startswith("labels_degraded=") for r in fleet_reasons)
    else:
        fleet_ok = not any(r.startswith("contract_fail=") for r in fleet_reasons)
        if critical_labels:
            fleet_ok = fleet_ok and not any(r.startswith("critical_labels_degraded=") for r in fleet_reasons)
        else:
            fleet_ok = fleet_ok and not any(r.startswith("labels_degraded=") for r in fleet_reasons)

    out = {
        "ts_ms": _now_ms(),
        "snapshot_ts_ms": ts_ms,
        "ok": bool(fleet_ok),
        "fleet_reasons": fleet_reasons,
        "degraded_labels": degraded,
        "contract": contract_details,
        "thresholds": {
            "DEG_POS_STALE_MS": th.pos_ms,
            "DEG_OB_STALE_MS": th.ob_ms,
            "DEG_TRADES_STALE_MS": th.trades_ms,
            "DEG_OPS_STALE_MS": th.ops_ms,
            "DEG_RESTARTS_N": th.restarts_n,
            "DEG_RESTARTS_WINDOW_SEC": th.restarts_window_sec,
            "DEG_CRITICAL_LABELS": list(critical_labels),
        },
    }
    return out


def write_from_snapshot() -> int:
    if not FLEET_SNAPSHOT.exists():
        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        OUT_PATH.write_text(json.dumps({"ok": False, "fleet_reasons": ["missing_fleet_snapshot"]}, indent=2), encoding="utf-8")
        return 2

    snap = _load_json(FLEET_SNAPSHOT)
    out = evaluate(snap)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(out, indent=2), encoding="utf-8")
    return 0


def main() -> int:
    return write_from_snapshot()


if __name__ == "__main__":
    raise SystemExit(main())
