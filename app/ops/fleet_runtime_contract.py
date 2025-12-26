from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional


class ContractStatus(str, Enum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"


class FaultCode(str, Enum):
    BUS_MISSING_POSITIONS = "BUS_MISSING_POSITIONS"
    BUS_STALE_POSITIONS = "BUS_STALE_POSITIONS"

    BUS_MISSING_TRADES = "BUS_MISSING_TRADES"
    BUS_STALE_TRADES = "BUS_STALE_TRADES"

    BUS_MISSING_ORDERBOOK = "BUS_MISSING_ORDERBOOK"
    BUS_STALE_ORDERBOOK = "BUS_STALE_ORDERBOOK"

    WS_HEARTBEAT_MISSING = "WS_HEARTBEAT_MISSING"
    WS_HEARTBEAT_STALE = "WS_HEARTBEAT_STALE"

    MEMORY_SNAPSHOT_MISSING = "MEMORY_SNAPSHOT_MISSING"
    MEMORY_SNAPSHOT_STALE = "MEMORY_SNAPSHOT_STALE"
    MEMORY_SNAPSHOT_PARSE_FAILED = "MEMORY_SNAPSHOT_PARSE_FAILED"
    MEMORY_COUNT_MISSING = "MEMORY_COUNT_MISSING"

    DECISIONS_MISSING = "DECISIONS_MISSING"
    DECISIONS_STALE = "DECISIONS_STALE"
    DECISIONS_TAIL_PARSE_FAILED = "DECISIONS_TAIL_PARSE_FAILED"
    DECISIONS_SCHEMA_INVALID = "DECISIONS_SCHEMA_INVALID"


@dataclass(frozen=True)
class ModeSpec:
    name: str
    require_positions: bool
    require_trades: bool
    require_orderbook: bool
    require_ws_heartbeat: bool
    require_memory: bool
    require_decisions: bool
    is_canary: bool = False


@dataclass(frozen=True)
class Thresholds:
    max_positions_age: float
    max_trades_age: float
    max_orderbook_age: float
    max_ws_hb_age: float
    max_memory_snapshot_age: float
    max_decisions_age: float


@dataclass
class ContractResult:
    status: ContractStatus
    faults_fail: List[FaultCode]
    faults_warn: List[FaultCode]
    details: Dict[str, Any]

    def ok(self) -> bool:
        return self.status == ContractStatus.PASS


def _norm_mode(mode: Optional[str]) -> str:
    m = (mode or "").strip().upper()
    if not m:
        return "UNKNOWN"
    if m in ("EXEC_DRY_RUN", "PAPER"):
        return "LEARN_DRY"
    return m


def get_mode_spec(mode: Optional[str]) -> ModeSpec:
    m = _norm_mode(mode)

    if m == "OFF":
        return ModeSpec(
            name="OFF",
            require_positions=False,
            require_trades=False,
            require_orderbook=False,
            require_ws_heartbeat=False,
            require_memory=False,
            require_decisions=False,
            is_canary=False,
        )

    if m == "LEARN_DRY":
        return ModeSpec(
            name="LEARN_DRY",
            require_positions=True,
            require_trades=True,
            require_orderbook=False,
            require_ws_heartbeat=False,
            require_memory=True,
            require_decisions=True,
            is_canary=False,
        )

    if m == "LIVE_CANARY":
        return ModeSpec(
            name="LIVE_CANARY",
            require_positions=True,
            require_trades=True,
            require_orderbook=True,
            require_ws_heartbeat=True,
            require_memory=True,
            require_decisions=True,
            is_canary=True,
        )

    if m == "LIVE":
        return ModeSpec(
            name="LIVE",
            require_positions=True,
            require_trades=True,
            require_orderbook=True,
            require_ws_heartbeat=True,
            require_memory=True,
            require_decisions=True,
            is_canary=False,
        )

    # Unknown: same as LIVE, but downgrade to WARN to surface misconfig loudly.
    return ModeSpec(
        name=m,
        require_positions=True,
        require_trades=True,
        require_orderbook=True,
        require_ws_heartbeat=True,
        require_memory=True,
        require_decisions=True,
        is_canary=False,
    )


def _age_is_missing(age: Optional[float]) -> bool:
    return age is None


def _is_stale(age: Optional[float], max_age: float) -> bool:
    if age is None:
        return True
    try:
        return float(age) > float(max_age)
    except Exception:
        return True


def validate_runtime_contract(
    mode: Optional[str],
    observed: Dict[str, Any],
    thresholds: Thresholds,
    *,
    age_unit: str = "sec",  # "sec" or "ms"
    force_orderbook_required: Optional[bool] = None,
    force_orderbook_optional: bool = False,
    # NEW: allow callers (fleet_degraded) to ignore checks that do not exist at that layer.
    ignore_ws_heartbeat: bool = False,
    ignore_memory: bool = False,
    ignore_decisions: bool = False,
) -> ContractResult:
    """
    observed keys:
      buses:
        positions_age, trades_age, orderbook_age, ws_hb_age
      memory:
        memory_exists, memory_age, memory_parse_ok, memory_count
      decisions:
        decisions_exists, decisions_age, decisions_tail_parse_ok, decisions_schema_valid
    """
    spec = get_mode_spec(mode)
    unknown_mode = spec.name not in ("OFF", "LEARN_DRY", "LIVE", "LIVE_CANARY")

    def unitify(x: Optional[float]) -> Optional[float]:
        if x is None:
            return None
        try:
            v = float(x)
        except Exception:
            return None
        if age_unit == "ms":
            return v / 1000.0
        return v

    ob_required = spec.require_orderbook
    if force_orderbook_required is not None:
        ob_required = bool(force_orderbook_required)
    if force_orderbook_optional:
        ob_required = False

    pos_age = unitify(observed.get("positions_age"))
    tr_age = unitify(observed.get("trades_age"))
    ob_age = unitify(observed.get("orderbook_age"))
    hb_age = unitify(observed.get("ws_hb_age"))

    mem_exists = bool(observed.get("memory_exists", False))
    mem_age = unitify(observed.get("memory_age"))
    mem_parse_ok = observed.get("memory_parse_ok", None)
    mem_count = observed.get("memory_count", None)

    dec_exists = bool(observed.get("decisions_exists", False))
    dec_age = unitify(observed.get("decisions_age"))
    dec_tail_ok = observed.get("decisions_tail_parse_ok", None)
    dec_schema_ok = observed.get("decisions_schema_valid", None)

    faults_fail: List[FaultCode] = []
    faults_warn: List[FaultCode] = []
    details: Dict[str, Any] = {
        "mode": spec.name,
        "is_canary": spec.is_canary,
        "age_unit": age_unit,
        "unknown_mode": unknown_mode,
        "ignore": {
            "ws_heartbeat": bool(ignore_ws_heartbeat),
            "memory": bool(ignore_memory),
            "decisions": bool(ignore_decisions),
        },
        "requirements": {
            "require_positions": spec.require_positions,
            "require_trades": spec.require_trades,
            "require_orderbook": ob_required,
            "require_ws_heartbeat": spec.require_ws_heartbeat and (not ignore_ws_heartbeat),
            "require_memory": spec.require_memory and (not ignore_memory),
            "require_decisions": spec.require_decisions and (not ignore_decisions),
        },
        "observed": {
            "positions_age_sec": pos_age,
            "trades_age_sec": tr_age,
            "orderbook_age_sec": ob_age,
            "ws_hb_age_sec": hb_age,
            "memory_exists": mem_exists,
            "memory_age_sec": mem_age,
            "memory_parse_ok": mem_parse_ok,
            "memory_count": mem_count,
            "decisions_exists": dec_exists,
            "decisions_age_sec": dec_age,
            "decisions_tail_parse_ok": dec_tail_ok,
            "decisions_schema_valid": dec_schema_ok,
        },
        "thresholds_sec": {
            "max_positions_age": thresholds.max_positions_age,
            "max_trades_age": thresholds.max_trades_age,
            "max_orderbook_age": thresholds.max_orderbook_age,
            "max_ws_hb_age": thresholds.max_ws_hb_age,
            "max_memory_snapshot_age": thresholds.max_memory_snapshot_age,
            "max_decisions_age": thresholds.max_decisions_age,
        },
    }

    def add_fault(code: FaultCode, required: bool) -> None:
        if unknown_mode:
            faults_warn.append(code)
            return
        if required:
            faults_fail.append(code)
        else:
            faults_warn.append(code)

    # WS buses
    if spec.require_positions:
        if _age_is_missing(pos_age):
            add_fault(FaultCode.BUS_MISSING_POSITIONS, required=True)
        elif _is_stale(pos_age, thresholds.max_positions_age):
            add_fault(FaultCode.BUS_STALE_POSITIONS, required=True)
    else:
        if _age_is_missing(pos_age):
            add_fault(FaultCode.BUS_MISSING_POSITIONS, required=False)
        elif _is_stale(pos_age, thresholds.max_positions_age):
            add_fault(FaultCode.BUS_STALE_POSITIONS, required=False)

    if spec.require_trades:
        if _age_is_missing(tr_age):
            add_fault(FaultCode.BUS_MISSING_TRADES, required=True)
        elif _is_stale(tr_age, thresholds.max_trades_age):
            add_fault(FaultCode.BUS_STALE_TRADES, required=True)
    else:
        if _age_is_missing(tr_age):
            add_fault(FaultCode.BUS_MISSING_TRADES, required=False)
        elif _is_stale(tr_age, thresholds.max_trades_age):
            add_fault(FaultCode.BUS_STALE_TRADES, required=False)

    if ob_required:
        if _age_is_missing(ob_age):
            add_fault(FaultCode.BUS_MISSING_ORDERBOOK, required=True)
        elif _is_stale(ob_age, thresholds.max_orderbook_age):
            add_fault(FaultCode.BUS_STALE_ORDERBOOK, required=True)
    else:
        if _age_is_missing(ob_age):
            add_fault(FaultCode.BUS_MISSING_ORDERBOOK, required=False)
        elif _is_stale(ob_age, thresholds.max_orderbook_age):
            add_fault(FaultCode.BUS_STALE_ORDERBOOK, required=False)

    # WS heartbeat
    if not ignore_ws_heartbeat:
        if spec.require_ws_heartbeat:
            if _age_is_missing(hb_age):
                add_fault(FaultCode.WS_HEARTBEAT_MISSING, required=True)
            elif _is_stale(hb_age, thresholds.max_ws_hb_age):
                add_fault(FaultCode.WS_HEARTBEAT_STALE, required=True)
        else:
            if _age_is_missing(hb_age):
                add_fault(FaultCode.WS_HEARTBEAT_MISSING, required=False)
            elif _is_stale(hb_age, thresholds.max_ws_hb_age):
                add_fault(FaultCode.WS_HEARTBEAT_STALE, required=False)

    # Memory snapshot
    if not ignore_memory:
        if spec.require_memory:
            if not mem_exists:
                add_fault(FaultCode.MEMORY_SNAPSHOT_MISSING, required=True)
            else:
                if mem_parse_ok is False:
                    add_fault(FaultCode.MEMORY_SNAPSHOT_PARSE_FAILED, required=True)
                if _is_stale(mem_age, thresholds.max_memory_snapshot_age):
                    add_fault(FaultCode.MEMORY_SNAPSHOT_STALE, required=True)
                if mem_count is None:
                    add_fault(FaultCode.MEMORY_COUNT_MISSING, required=True)
        else:
            if not mem_exists:
                add_fault(FaultCode.MEMORY_SNAPSHOT_MISSING, required=False)
            else:
                if mem_parse_ok is False:
                    add_fault(FaultCode.MEMORY_SNAPSHOT_PARSE_FAILED, required=False)
                if _is_stale(mem_age, thresholds.max_memory_snapshot_age):
                    add_fault(FaultCode.MEMORY_SNAPSHOT_STALE, required=False)
                if mem_count is None:
                    add_fault(FaultCode.MEMORY_COUNT_MISSING, required=False)

    # Decisions
    if not ignore_decisions:
        if spec.require_decisions:
            if not dec_exists:
                add_fault(FaultCode.DECISIONS_MISSING, required=True)
            else:
                if _is_stale(dec_age, thresholds.max_decisions_age):
                    add_fault(FaultCode.DECISIONS_STALE, required=True)
                if dec_tail_ok is False:
                    add_fault(FaultCode.DECISIONS_TAIL_PARSE_FAILED, required=True)
                if dec_schema_ok is False:
                    add_fault(FaultCode.DECISIONS_SCHEMA_INVALID, required=True)
        else:
            if not dec_exists:
                add_fault(FaultCode.DECISIONS_MISSING, required=False)
            else:
                if _is_stale(dec_age, thresholds.max_decisions_age):
                    add_fault(FaultCode.DECISIONS_STALE, required=False)
                if dec_tail_ok is False:
                    add_fault(FaultCode.DECISIONS_TAIL_PARSE_FAILED, required=False)
                if dec_schema_ok is False:
                    add_fault(FaultCode.DECISIONS_SCHEMA_INVALID, required=False)

    if faults_fail:
        status = ContractStatus.FAIL
    elif faults_warn:
        status = ContractStatus.WARN
    else:
        status = ContractStatus.PASS

    return ContractResult(status=status, faults_fail=faults_fail, faults_warn=faults_warn, details=details)
