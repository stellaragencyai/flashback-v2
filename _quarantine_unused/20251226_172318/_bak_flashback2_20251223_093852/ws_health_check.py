#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Flashback — WS Health Check (v2.5 contract-locked)

What it does:
- Observes WS/REST-fed state freshness:
    state/positions_bus.json
    state/orderbook_bus.json
    state/trades_bus.json
    state/ws_switchboard_heartbeat_<ACCOUNT_LABEL>.txt

- Observes log growth risk:
    state/public_trades.jsonl
    state/ws_executions.jsonl

- Observes AI Memory health:
    state/ai_memory/memory_snapshot.json
    state/ai_memory/memory_records.jsonl

- Observes AI Decisions health:
    state/ai_decisions.jsonl (tail parse + schema validate + freshness)

- Writes canonical component status into state/ops_snapshot.json via app.ops.ops_state

Contract-locked behavior:
- Uses app.ops.fleet_runtime_contract.validate_runtime_contract() as the ONLY authority
  for PASS/WARN/FAIL classification by automation_mode.

Orderbook requirement overrides:
    WS_REQUIRE_ORDERBOOK=true/false
    WS_ORDERBOOK_OPTIONAL=true
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Optional, Tuple, Dict, Any

try:
    from app.core.config import settings
    ROOT: Path = settings.ROOT  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[2]


STATE_DIR = ROOT / "state"
POSITIONS = STATE_DIR / "positions_bus.json"
ORDERBOOK = STATE_DIR / "orderbook_bus.json"
TRADES = STATE_DIR / "trades_bus.json"

PUBLIC_TRADES_JSONL = STATE_DIR / "public_trades.jsonl"
WS_EXECUTIONS_JSONL = STATE_DIR / "ws_executions.jsonl"

AI_MEMORY_DIR = STATE_DIR / "ai_memory"
MEMORY_SNAPSHOT = AI_MEMORY_DIR / "memory_snapshot.json"
MEMORY_RECORDS = AI_MEMORY_DIR / "memory_records.jsonl"

AI_DECISIONS_JSONL = STATE_DIR / "ai_decisions.jsonl"

FLEET_MANIFEST = ROOT / "config" / "fleet_manifest.yaml"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _env_bool(name: str, default: str = "false") -> bool:
    raw = os.getenv(name, default)
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")


def _read_json_updated_ms(path: Path) -> Optional[int]:
    try:
        if not path.exists():
            return None
        import json
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return None
        v = data.get("updated_ms")
        return int(v) if v is not None else None
    except Exception:
        return None


def _file_age_sec(path: Path) -> Optional[float]:
    try:
        if not path.exists():
            return None
        mtime = path.stat().st_mtime
        return float(time.time() - mtime)
    except Exception:
        return None


def _age_from_updated_ms(updated_ms: Optional[int]) -> Optional[float]:
    if not updated_ms or updated_ms <= 0:
        return None
    return (_now_ms() - updated_ms) / 1000.0


def _file_size_mb(path: Path) -> Optional[float]:
    try:
        if not path.exists():
            return None
        return float(path.stat().st_size) / (1024.0 * 1024.0)
    except Exception:
        return None


def _safe_replace(src: Path, dst: Path, retries: int = 8, sleep_sec: float = 0.05) -> bool:
    for _ in range(max(1, retries)):
        try:
            os.replace(str(src), str(dst))
            return True
        except Exception:
            time.sleep(sleep_sec)
    return False


def _rotate_file(path: Path, keep: int = 3) -> Tuple[bool, str]:
    try:
        if not path.exists():
            return (False, "missing")

        if keep < 1:
            keep = 1

        for i in range(keep, 0, -1):
            older = path.with_name(f"{path.name}.{i}")
            newer = path.with_name(f"{path.name}.{i+1}")
            if older.exists():
                _safe_replace(older, newer)

        rotated_1 = path.with_name(f"{path.name}.1")
        ok = _safe_replace(path, rotated_1)
        if not ok:
            return (False, "rotate_failed (file lock?)")

        try:
            path.write_text("", encoding="utf-8")
        except Exception:
            with path.open("a", encoding="utf-8"):
                pass

        return (True, f"rotated -> {rotated_1.name}")
    except Exception as e:
        return (False, f"rotate_exception: {e}")


def _write_ops(account_label: str, ok: bool, details: Dict[str, Any]) -> None:
    try:
        from app.ops.ops_state import write_component_status  # type: ignore
        write_component_status(
            component="ws_health_check",
            account_label=account_label,
            ok=ok,
            details=details,
            ts_ms=_now_ms(),
        )
    except Exception:
        return


def _read_memory_snapshot_meta() -> Dict[str, Any]:
    meta: Dict[str, Any] = {
        "exists": MEMORY_SNAPSHOT.exists(),
        "age_sec": _file_age_sec(MEMORY_SNAPSHOT),
        "parse_ok": False,
        "count": None,
        "latest_updated_ts": None,
        "sample_key": None,
    }
    if not MEMORY_SNAPSHOT.exists():
        return meta

    try:
        import json
        data = json.loads(MEMORY_SNAPSHOT.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return meta

        meta["parse_ok"] = True
        meta["count"] = len(data)

        latest = 0
        sample_key = None
        scanned = 0
        for k, rec in data.items():
            if sample_key is None:
                sample_key = k
            if isinstance(rec, dict):
                lifecycle = rec.get("lifecycle") if isinstance(rec.get("lifecycle"), dict) else {}
                u = lifecycle.get("updated_ts") or rec.get("ts") or 0
                try:
                    u_i = int(u)
                except Exception:
                    u_i = 0
                latest = max(latest, u_i)
            scanned += 1
            if scanned >= 500:
                break

        meta["latest_updated_ts"] = latest if latest > 0 else None
        meta["sample_key"] = sample_key
        return meta
    except Exception:
        return meta


def _tail_last_jsonl_line(path: Path, max_bytes: int = 65536) -> Tuple[bool, Optional[str], Optional[Dict[str, Any]]]:
    """
    Read and parse last JSON line from a JSONL file (best effort).
    Returns: (ok, error, obj)
    """
    if not path.exists():
        return (False, "missing", None)
    try:
        size = path.stat().st_size
        if size <= 0:
            return (False, "empty", None)

        read_n = min(int(max_bytes), int(size))
        with path.open("rb") as f:
            f.seek(-read_n, 2)
            chunk = f.read(read_n)

        lines = chunk.splitlines()
        for raw in reversed(lines):
            raw = raw.strip()
            if not raw:
                continue
            try:
                import json
                obj = json.loads(raw.decode("utf-8", errors="ignore"))
                if not isinstance(obj, dict):
                    return (False, "last_line_not_dict", None)
                return (True, None, obj)
            except Exception:
                return (False, "json_parse_failed", None)

        return (False, "no_nonempty_lines", None)
    except Exception as e:
        return (False, f"tail_exception:{e}", None)


def _validate_decision_obj(obj: Dict[str, Any]) -> Tuple[bool, str, Optional[list]]:
    """
    Uses app.core.ai_decision_validate.validate_pilot_decision if available.
    Returns: (ok, reason, errs)
    """
    try:
        from app.core.ai_decision_validate import validate_pilot_decision  # type: ignore
        ok, errs = validate_pilot_decision(obj)
        return (bool(ok), "valid" if ok else "invalid", errs)
    except Exception:
        required = ("schema_version", "ts", "decision", "tier_used", "gates")
        missing = [k for k in required if k not in obj]
        if missing:
            return (False, "missing_fields", missing)
        return (True, "validator_missing_minimal_ok", None)


def _get_mode_from_manifest(account_label: str) -> Optional[str]:
    """
    Best-effort: reads config/fleet_manifest.yaml and returns automation_mode for account_label.
    Returns None if missing/unparseable.
    """
    try:
        if not FLEET_MANIFEST.exists():
            return None
        try:
            import yaml  # type: ignore
        except Exception:
            return None

        d = yaml.safe_load(FLEET_MANIFEST.read_text(encoding="utf-8", errors="ignore")) or {}
        fleet = d.get("fleet") or []
        if not isinstance(fleet, list):
            return None

        for row in fleet:
            if not isinstance(row, dict):
                continue
            lab = str(row.get("account_label") or "").strip()
            if lab != account_label:
                continue
            mode = row.get("automation_mode")
            if mode is None:
                return None
            return str(mode).strip()
        return None
    except Exception:
        return None


def main() -> int:
    account_label = os.getenv("ACCOUNT_LABEL", "main").strip() or "main"
    hb = STATE_DIR / f"ws_switchboard_heartbeat_{account_label}.txt"

    pos_age = _age_from_updated_ms(_read_json_updated_ms(POSITIONS))
    ob_age = _age_from_updated_ms(_read_json_updated_ms(ORDERBOOK))
    tr_age = _age_from_updated_ms(_read_json_updated_ms(TRADES))
    hb_age = _file_age_sec(hb)

    mode = _get_mode_from_manifest(account_label) or os.getenv("AUTOMATION_MODE") or "UNKNOWN"

    # thresholds (seconds)
    max_pos = float(os.getenv("WS_MAX_POS_AGE_SEC", "30"))
    max_ob = float(os.getenv("WS_MAX_OB_AGE_SEC", "10"))
    max_tr = float(os.getenv("WS_MAX_TRADES_AGE_SEC", "10"))
    max_hb = float(os.getenv("WS_MAX_HB_AGE_SEC", "60"))

    # Memory thresholds
    mem_max_age = float(os.getenv("MEM_MAX_SNAPSHOT_AGE_SEC", "3600"))

    # Decisions thresholds
    dec_max_age = float(os.getenv("AI_DECISIONS_MAX_AGE_SEC", "3600"))

    # Memory meta
    mem_meta = _read_memory_snapshot_meta()

    # Decisions meta
    dec_age = _file_age_sec(AI_DECISIONS_JSONL)
    dec_tail_ok, dec_tail_err, dec_last = _tail_last_jsonl_line(AI_DECISIONS_JSONL)
    dec_valid_ok = False
    dec_valid_reason = "missing"
    dec_valid_errs = None
    if dec_tail_ok and isinstance(dec_last, dict):
        dec_valid_ok, dec_valid_reason, dec_valid_errs = _validate_decision_obj(dec_last)

    # Contract validator (authoritative)
    try:
        from app.ops.fleet_runtime_contract import Thresholds, validate_runtime_contract  # type: ignore
        th = Thresholds(
            max_positions_age=max_pos,
            max_trades_age=max_tr,
            max_orderbook_age=max_ob,
            max_ws_hb_age=max_hb,
            max_memory_snapshot_age=mem_max_age,
            max_decisions_age=dec_max_age,
        )

        force_ob_req = None
        if os.getenv("WS_REQUIRE_ORDERBOOK") is not None:
            force_ob_req = _env_bool("WS_REQUIRE_ORDERBOOK", "true")
        force_ob_opt = _env_bool("WS_ORDERBOOK_OPTIONAL", "false")

        contract = validate_runtime_contract(
            mode=mode,
            observed={
                "positions_age": pos_age,
                "trades_age": tr_age,
                "orderbook_age": ob_age,
                "ws_hb_age": hb_age,
                "memory_exists": bool(mem_meta.get("exists")),
                "memory_age": mem_meta.get("age_sec"),
                "memory_parse_ok": bool(mem_meta.get("parse_ok")),
                "memory_count": mem_meta.get("count"),
                "decisions_exists": AI_DECISIONS_JSONL.exists(),
                "decisions_age": dec_age,
                "decisions_tail_parse_ok": bool(dec_tail_ok),
                "decisions_schema_valid": bool(dec_valid_ok),
            },
            thresholds=th,
            age_unit="sec",
            force_orderbook_required=force_ob_req,
            force_orderbook_optional=force_ob_opt,
        )
    except Exception as e:
        # If contract missing, do not silently pass. Scream loudly.
        contract = None
        contract_error = f"contract_validator_failed:{e!r}"

    def fmt_age(x: Optional[float]) -> str:
        return "MISSING" if x is None else f"{x:.2f}s"

    def fmt_mb(x: Optional[float]) -> str:
        return "MISSING" if x is None else f"{x:.2f} MB"

    print("\n=== WS HEALTH CHECK ===")
    print(f"ACCOUNT_LABEL: {account_label}")
    print(f"automation_mode: {mode}")
    print(f"positions_bus.json  age: {fmt_age(pos_age)}")
    print(f"orderbook_bus.json  age: {fmt_age(ob_age)}")
    print(f"trades_bus.json     age: {fmt_age(tr_age)}")
    print(f"heartbeat file      age: {fmt_age(hb_age)}")

    failures = []
    warnings = []

    if contract is None:
        failures.append(contract_error)
        contract_details = {"error": contract_error}
    else:
        contract_details = contract.details
        for c in contract.faults_fail:
            failures.append(str(c.value))
        for c in contract.faults_warn:
            warnings.append(str(c.value))

    # Log growth guardrails
    warn_mb = float(os.getenv("WS_LOG_WARN_MB", "50"))
    cap_mb = float(os.getenv("WS_LOG_CAP_MB", "150"))
    keep_n = int(os.getenv("WS_LOG_ROTATE_KEEP", "3") or "3")

    auto_rotate = _env_bool("WS_LOG_AUTO_ROTATE", "true")
    fail_on_bloat = _env_bool("WS_FAIL_ON_LOG_BLOAT", "false")

    pub_mb = _file_size_mb(PUBLIC_TRADES_JSONL)
    exe_mb = _file_size_mb(WS_EXECUTIONS_JSONL)

    print("\n--- LOG SIZE CHECK ---")
    print(f"public_trades.jsonl  size: {fmt_mb(pub_mb)}")
    print(f"ws_executions.jsonl  size: {fmt_mb(exe_mb)}")
    print(f"policy: warn>{warn_mb:.0f}MB cap>{cap_mb:.0f}MB keep={keep_n} auto_rotate={auto_rotate} fail_on_bloat={fail_on_bloat}")

    bloat_issues = []
    rotations: Dict[str, str] = {}

    def check_one(path: Path, size_mb: Optional[float], label: str) -> None:
        if size_mb is None:
            return
        if size_mb >= warn_mb:
            print(f"WARNING: {label} is large ({size_mb:.2f} MB).")
        if size_mb >= cap_mb:
            if auto_rotate:
                ok, msg = _rotate_file(path, keep=keep_n)
                if ok:
                    print(f"ROTATED: {label} ({size_mb:.2f} MB) {msg}")
                    rotations[label] = msg
                else:
                    print(f"ROTATE FAILED: {label} ({size_mb:.2f} MB) {msg}")
                    bloat_issues.append(f"{label} rotation failed ({msg})")
            else:
                bloat_issues.append(f"{label} exceeds cap ({size_mb:.2f} MB >= {cap_mb:.2f} MB)")

    check_one(PUBLIC_TRADES_JSONL, pub_mb, "public_trades.jsonl")
    check_one(WS_EXECUTIONS_JSONL, exe_mb, "ws_executions.jsonl")

    ok = (len(failures) == 0 and not (bloat_issues and fail_on_bloat))

    _write_ops(
        account_label=account_label,
        ok=ok,
        details={
            "automation_mode": mode,
            "contract": contract_details,
            "warnings": warnings,
            "failures": failures,
            "log_sizes_mb": {"public_trades": pub_mb, "ws_executions": exe_mb},
            "log_policy": {"warn_mb": warn_mb, "cap_mb": cap_mb, "keep": keep_n, "auto_rotate": auto_rotate, "fail_on_bloat": fail_on_bloat},
            "rotations": rotations,
            "bloat_issues": bloat_issues,
            "ai_memory": {
                "snapshot_age_sec": mem_meta.get("age_sec"),
                "snapshot_size_mb": _file_size_mb(MEMORY_SNAPSHOT),
                "records_size_mb": _file_size_mb(MEMORY_RECORDS),
                "parse_ok": mem_meta.get("parse_ok"),
                "count": mem_meta.get("count"),
                "sample_key": mem_meta.get("sample_key"),
                "policy": {"max_age_sec": mem_max_age},
            },
            "ai_decisions": {
                "exists": AI_DECISIONS_JSONL.exists(),
                "age_sec": dec_age,
                "tail_parse_ok": dec_tail_ok,
                "tail_error": dec_tail_err,
                "schema_valid": dec_valid_ok,
                "schema_reason": dec_valid_reason,
                "schema_errs": dec_valid_errs,
                "policy": {"max_age_sec": dec_max_age},
            },
        },
    )

    if warnings:
        print("\nWARN:")
        for w in warnings:
            print(f" - {w}")

    if failures:
        print("\nFAIL:")
        for f in failures:
            print(f" - {f}")
        return 2

    if bloat_issues and fail_on_bloat:
        print("\nFAIL (LOG BLOAT):")
        for b in bloat_issues:
            print(f" - {b}")
        return 3

    print("\nPASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
