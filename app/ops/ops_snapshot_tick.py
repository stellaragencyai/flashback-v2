from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict

ROOT = Path(__file__).resolve().parents[2]
STATE = ROOT / "state"
WATCHDOG_STATE = STATE / "orchestrator_watchdog.json"
ORCH_STATE = STATE / "orchestrator_state.json"
MANIFEST = ROOT / "config" / "fleet_manifest.yaml"
OUT = STATE / "ops_snapshot.json"
HEARTBEATS = STATE / "heartbeats.json"
def _atomic_write_text(path: Path, text: str, *, encoding: str = "utf-8", retries: int = 12, sleep_ms: int = 50) -> None:
    """
    Windows-safe atomic-ish write with retries:
    - Write to temp file in same directory
    - os.replace to target (atomic on Windows when possible)
    - Retry on PermissionError (transient lock by AV/editor/other reader)
    """
    tmp = path.with_suffix(path.suffix + ".tmp")
    last_err: Exception | None = None
    for _ in range(max(1, retries)):
        try:
            tmp.write_text(text, encoding=encoding)
            os.replace(str(tmp), str(path))
            return
        except PermissionError as e:
            last_err = e
            try:
                time.sleep(max(0.0, sleep_ms / 1000.0))
            except Exception:
                pass
        except Exception as e:
            # cleanup tmp then re-raise
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass
            raise
    # final cleanup attempt
    try:
        if tmp.exists():
            tmp.unlink()
    except Exception:
        pass
    if last_err:
        raise last_err

def _load_watchdog_state() -> dict:
    try:
        if not WATCHDOG_STATE.exists():
            return {}
        return json.loads(WATCHDOG_STATE.read_text(encoding="utf-8", errors="ignore") or "{}")
    except Exception:
        return {}

def _load_orchestrator_state() -> dict:
    try:
        if not ORCH_STATE.exists():
            return {}
        return json.loads(ORCH_STATE.read_text(encoding="utf-8", errors="ignore") or "{}")
    except Exception:
        return {}


def _now_ms() -> int:
    return int(time.time() * 1000)

def _load_json(p: Path) -> Any:
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8", errors="ignore") or "null")
    except Exception:
        return None

def _load_manifest_rows() -> list[dict[str, Any]]:
    if not MANIFEST.exists():
        return []
    try:
        import yaml  # type: ignore
        d = yaml.safe_load(MANIFEST.read_text(encoding="utf-8", errors="ignore")) or {}
        fleet = d.get("fleet") or []
        if not isinstance(fleet, list):
            return []
        out: list[dict[str, Any]] = []
        for r in fleet:
            if isinstance(r, dict):
                out.append(r)
        return out
    except Exception:
        return []

def _extract_supervisor_from_heartbeats(hb: Any, label: str) -> dict[str, Any]:
    """
    Best-effort extraction.
    heartbeats.json format is unknown here, so we try a few patterns and fall back to empty.
    """
    empty = {"ok": None, "pid": None, "error": None, "ts_ms": None}

    if hb is None:
        return empty

    # Pattern A: {"accounts": {"flashback01": {...}}}
    if isinstance(hb, dict):
        accounts = hb.get("accounts")
        if isinstance(accounts, dict):
            a = accounts.get(label)
            if isinstance(a, dict):
                sup = a.get("supervisor_ai_stack")
                if isinstance(sup, dict):
                    return {
                        "ok": sup.get("ok"),
                        "pid": sup.get("pid"),
                        "error": sup.get("error"),
                        "ts_ms": sup.get("ts_ms") or a.get("ts_ms") or hb.get("ts_ms"),
                    }

    # Pattern B: list of events [{label:..., supervisor_ai_stack:{...}}]
    if isinstance(hb, list):
        # take newest match by ts_ms if present
        best = None
        best_ts = -1
        for e in hb:
            if not isinstance(e, dict):
                continue
            if str(e.get("account_label") or e.get("label") or "").strip() != label:
                continue
            sup = e.get("supervisor_ai_stack")
            if not isinstance(sup, dict):
                continue
            ts = int(sup.get("ts_ms") or e.get("ts_ms") or 0)
            if ts >= best_ts:
                best_ts = ts
                best = sup
        if isinstance(best, dict):
            return {
                "ok": best.get("ok"),
                "pid": best.get("pid"),
                "error": best.get("error"),
                "ts_ms": best.get("ts_ms"),
            }

    return empty

def main() -> int:
    STATE.mkdir(parents=True, exist_ok=True)

    rows = _load_manifest_rows()
    hb = _load_json(HEARTBEATS)

    accounts: Dict[str, Any] = {}

    for r in rows:
        label = str(r.get("account_label") or "").strip()
        if not label:
            continue

        enabled = bool(r.get("enabled", True))
        enable_ai_stack = bool(r.get("enable_ai_stack", True))
        automation_mode = str(r.get("automation_mode") or "UNKNOWN").strip().upper()

        sup = _extract_supervisor_from_heartbeats(hb, label)

        accounts[label] = {
            "enabled": enabled,
            "enable_ai_stack": enable_ai_stack,
            "automation_mode": automation_mode,
            "supervisor_ai_stack": sup,
        }

    out = {
        "ts_ms": _now_ms(),
        "source": {
            "manifest_path": str(MANIFEST),
            "heartbeats_path": str(HEARTBEATS),
        },
        "accounts": accounts,
    }
    # Phase8 FINAL: Merge orchestrator_state -> accounts.supervisor_ai_stack right before writing snapshot (authoritative)
    orch = _load_orchestrator_state()
    procs = (orch.get("procs") or {}) if isinstance(orch, dict) else {}
    if isinstance(accounts, dict) and isinstance(procs, dict):
        for lbl, pinfo in procs.items():
            if not isinstance(lbl, str) or not isinstance(pinfo, dict):
                continue
            if lbl not in accounts or not isinstance(accounts.get(lbl), dict):
                accounts[lbl] = {}
            pid = pinfo.get("pid")
            alive = bool(pinfo.get("alive"))
            try:
                pid_int = int(pid) if pid is not None else None
            except Exception:
                pid_int = None
            accounts[lbl]["supervisor_ai_stack"] = {
                "ok": bool(alive),
                "pid": pid_int,
                "source": "orchestrator_state",
                "stdout_log": pinfo.get("stdout_log"),
                "stderr_log": pinfo.get("stderr_log"),
            }

    # Phase8: Merge watchdog into accounts (writer-anchor)
    wd = _load_watchdog_state()
    wd_labels = (wd.get("labels") or {}) if isinstance(wd, dict) else {}
    
    # locate the accounts dict without assuming variable names
    _acc = None
    if "accounts" in locals() and isinstance(locals().get("accounts"), dict):
        _acc = locals().get("accounts")
    
    if _acc is None:
        for _name, obj in list(locals().items()):
            if isinstance(obj, dict) and isinstance(obj.get("accounts"), dict):
                _acc = obj.get("accounts")
                break
    
    if isinstance(_acc, dict) and isinstance(wd_labels, dict):
        for lbl, w in wd_labels.items():
            if not isinstance(lbl, str) or not isinstance(w, dict):
                continue
            if lbl not in _acc or not isinstance(_acc.get(lbl), dict):
                _acc[lbl] = {}
            try:
                rc = int(w.get("restart_count") or 0)
            except Exception:
                rc = 0
            try:
                bo = float(w.get("backoff_sec") or 0.0)
            except Exception:
                bo = 0.0
    
            _acc[lbl]["watchdog"] = {
                "alive": bool(w.get("alive")) if "alive" in w else None,
                "pid": w.get("pid"),
                "restart_count": rc,
                "backoff_sec": bo,
                "next_restart_allowed_ts_ms": w.get("next_restart_allowed_ts_ms"),
                "blocked": bool(w.get("blocked")) if "blocked" in w else False,
                "blocked_reason": w.get("blocked_reason"),
                "last_checked_ts_ms": w.get("last_checked_ts_ms"),
                "last_restart_ts_ms": w.get("last_restart_ts_ms"),
                "source": "orchestrator_watchdog",
            }

    _atomic_write_text(OUT, json.dumps(out, indent=2), encoding="utf-8")
    print(f"OK: wrote ops_snapshot.json accounts_len={len(accounts)}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
