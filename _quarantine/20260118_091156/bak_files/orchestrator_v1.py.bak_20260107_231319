from __future__ import annotations

# === FLASHBACK_ROOT_INJECT ===
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
# === FLASHBACK_ROOT_INJECT ===

import json
import os
import subprocess
import time
import socket
import hashlib
import platform
from typing import Any, Dict, List, Optional

# writer_lock signature has changed across your repo history.
# We must be defensive: sometimes acquire_lock() takes 0 args, sometimes a lock path.
try:
    from app.ops.writer_lock import acquire_lock as _acquire_lock, release_lock as _release_lock
except Exception:
    _acquire_lock = None  # type: ignore
    _release_lock = None  # type: ignore


# =========================
# CANONICAL EXECUTION ENTRYPOINT
# =========================
# This is the ONLY allowed entrypoint for LIVE trading.
# All LIVE / LIVE_CANARY executions MUST pass through here.
# Bypassing this file is a policy violation.

STATE = ROOT / "state"
LOGDIR = STATE / "orchestrator_logs"
MANIFEST = ROOT / "config" / "fleet_manifest.yaml"
OUT = STATE / "orchestrator_state.json"
BOOT = STATE / "boot_record.json"
LOCK_FP = STATE / "orchestrator.lock"

ALLOWED_MODES = {"DRY", "LIVE_CANARY", "LIVE"}
DEFAULT_MODE = "DRY"


# =========================
# UTILITIES
# =========================

def _now_ms() -> int:
    return int(time.time() * 1000)


def _fatal(msg: str) -> None:
    print(f"FATAL: {msg}", file=sys.stderr)
    raise SystemExit(1)


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        r = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}"],
            capture_output=True,
            text=True,
        )
        return str(pid) in (r.stdout or "")
    except Exception:
        return False


def _acquire_lock_safe(lock_path: Path) -> bool:
    """
    Acquire orchestrator lock defensively across differing writer_lock implementations.
    Returns True if acquired (or if locking not available), False if failed.
    """
    try:
        lock_path.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    if not callable(_acquire_lock):
        # No lock available; fail-open (but this is not ideal)
        return True

    try:
        # Some implementations: acquire_lock()
        _acquire_lock()
        return True
    except TypeError:
        # Some implementations: acquire_lock(path)
        try:
            _acquire_lock(str(lock_path))
            return True
        except Exception:
            return False
    except Exception:
        return False


def _release_lock_safe(lock_path: Path) -> None:
    try:
        if callable(_release_lock):
            try:
                _release_lock()
            except TypeError:
                # Some variants may want a path, tolerate it.
                try:
                    _release_lock(str(lock_path))
                except Exception:
                    pass
    except Exception:
        pass


# =========================
# VPN / NETWORK GATE (WINDOWS)
# =========================

def _vpn_active_avast() -> bool:
    """
    Avast Secure VPN creates a TAP / Wintun-style adapter.
    We detect it via 'ipconfig' presence.
    """
    try:
        r = subprocess.run(
            ["ipconfig"],
            capture_output=True,
            text=True,
        )
        out = (r.stdout or "").lower()
        return ("avast" in out) or ("secure vpn" in out)
    except Exception:
        return False


def _enforce_vpn(mode: str) -> None:
    if mode in {"LIVE", "LIVE_CANARY"}:
        if not _vpn_active_avast():
            _fatal(f"{mode} blocked: Avast Secure VPN not detected")


# =========================
# MACHINE FINGERPRINT
# =========================

def _machine_fingerprint() -> Dict[str, Any]:
    h = hashlib.sha256()
    h.update(platform.node().encode(errors="ignore"))
    h.update(platform.platform().encode(errors="ignore"))
    h.update(sys.executable.encode(errors="ignore"))
    return {
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "python": sys.executable,
        "fingerprint": h.hexdigest(),
    }


# =========================
# MANIFEST LOADING
# =========================

def _load_manifest_rows() -> List[Dict[str, Any]]:
    if not MANIFEST.exists():
        return []
    try:
        import yaml  # type: ignore
        d = yaml.safe_load(MANIFEST.read_text(encoding="utf-8", errors="ignore")) or {}
        fleet = d.get("fleet") or []
        return [r for r in fleet if isinstance(r, dict)]
    except Exception:
        return []


def _should_run(row: Dict[str, Any]) -> bool:
    enabled = bool(row.get("enabled", True))
    enable_ai_stack = bool(row.get("enable_ai_stack", True))
    mode = str(row.get("automation_mode") or "").strip().upper()
    mode_ok = mode not in ("", "OFF", "DISABLED", "NONE")
    return bool(enabled and enable_ai_stack and mode_ok)


def _as_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return default


def _as_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


# =========================
# MAIN
# =========================

def main() -> int:
    STATE.mkdir(parents=True, exist_ok=True)
    LOGDIR.mkdir(parents=True, exist_ok=True)

    mode = (os.environ.get("FLASHBACK_MODE") or DEFAULT_MODE).strip().upper()
    if mode not in ALLOWED_MODES:
        _fatal(f"Invalid FLASHBACK_MODE={mode}")

    _enforce_vpn(mode)

    fp = _machine_fingerprint()
    boot = {
        "ts_ms": _now_ms(),
        "mode": mode,
        "root": str(ROOT),
        "machine": fp,
    }
    BOOT.write_text(json.dumps(boot, indent=2), encoding="utf-8")

    rows = _load_manifest_rows()

    only = (os.getenv("ORCH_ONLY_LABELS") or "").strip()
    only_set = {x.strip() for x in only.split(",") if x.strip()}

    max_procs = _as_int(os.getenv("ORCH_MAX_PROCS"), 3)
    if max_procs <= 0:
        max_procs = 3

    procs: Dict[str, Any] = {}
    started: List[str] = []
    skipped: List[str] = []

    # Fleet truth: always populate subaccounts for EVERY manifest row.
    subaccounts_state: Dict[str, Any] = {}

    # Start budget
    started_count = 0

    for r in rows:
        label = str(r.get("account_label") or "").strip()
        if not label:
            continue

        enabled = _as_bool(r.get("enabled"), False)
        enable_ai_stack = _as_bool(r.get("enable_ai_stack"), False)
        automation_mode = str(r.get("automation_mode") or "").strip()
        should_run = _should_run(r)

        entry: Dict[str, Any] = {
            "label": label,
            "enabled": enabled,
            "enable_ai_stack": enable_ai_stack,
            "automation_mode": automation_mode,
            "should_run": should_run,
            "online": False,
            "last_heartbeat_ms": 0,
            "phase": "unknown",
            "reason": None,
            "pid": None,
            "alive": False,
            "stdout_log": None,
            "stderr_log": None,
            "started_ts_ms": None,
            # pass-through useful config for cockpit display
            "strategy": {
                "name": r.get("strategy_name") or "unknown",
                "version": r.get("strategy_version") or "unknown",
            },
            "risk_pct": r.get("risk_pct"),
            "role": r.get("role"),
            "symbols": r.get("symbols") or [],
            "timeframes": r.get("timeframes") or [],
            "setup_types": r.get("setup_types") or [],
        }

        # Apply label filters
        if only_set and label not in only_set:
            entry["status"] = "SKIPPED"
            entry["reason"] = "not in ORCH_ONLY_LABELS"
            skipped.append(f"{label}: not in ORCH_ONLY_LABELS")
            subaccounts_state[label] = entry
            continue

        if not should_run:
            # Distinguish disabled vs misconfigured
            if not enabled:
                entry["status"] = "DISABLED"
                entry["reason"] = "enabled=false"
                skipped.append(f"{label}: enabled=false")
            elif not enable_ai_stack:
                entry["status"] = "DISABLED"
                entry["reason"] = "enable_ai_stack=false"
                skipped.append(f"{label}: enable_ai_stack=false")
            else:
                entry["status"] = "SKIPPED"
                entry["reason"] = f"automation_mode={automation_mode or 'EMPTY'}"
                skipped.append(f"{label}: automation_mode={automation_mode or 'EMPTY'}")
            subaccounts_state[label] = entry
            continue

        # Enforce start cap
        if started_count >= max_procs:
            entry["status"] = "SKIPPED"
            entry["reason"] = f"ORCH_MAX_PROCS cap ({max_procs})"
            skipped.append(f"{label}: ORCH_MAX_PROCS cap ({max_procs})")
            subaccounts_state[label] = entry
            continue

        env = os.environ.copy()
        env["ACCOUNT_LABEL"] = label
        env["FLASHBACK_MODE"] = mode
        env["PYTHONUTF8"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"

        cmd = [sys.executable, "-m", "app.bots.supervisor_ai_stack"]

        ts = _now_ms()
        out_log = LOGDIR / f"{label}.stdout.log"
        err_log = LOGDIR / f"{label}.stderr.log"

        try:
            with out_log.open("ab") as fo, err_log.open("ab") as fe:
                header = f"\n\n=== START {label} ts_ms={ts} mode={mode} cmd={cmd} ===\n"
                fo.write(header.encode("utf-8", errors="ignore"))
                fe.write(header.encode("utf-8", errors="ignore"))

                p = subprocess.Popen(
                    cmd,
                    cwd=str(ROOT),
                    env=env,
                    stdout=fo,
                    stderr=fe,
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
                )

            time.sleep(0.35)
            alive = _pid_alive(int(p.pid))

            procs[label] = {
                "pid": int(p.pid),
                "cmd": cmd,
                "started_ts_ms": ts,
                "alive": bool(alive),
                "stdout_log": str(out_log),
                "stderr_log": str(err_log),
            }

            entry["pid"] = int(p.pid)
            entry["alive"] = bool(alive)
            entry["stdout_log"] = str(out_log)
            entry["stderr_log"] = str(err_log)
            entry["started_ts_ms"] = ts
            entry["online"] = bool(alive)
            entry["status"] = "RUNNING" if alive else "STARTED_NOT_CONFIRMED"
            entry["reason"] = None if alive else "process not confirmed alive"

            started.append(label)
            started_count += 1

        except Exception as e:
            procs[label] = {
                "pid": None,
                "cmd": cmd,
                "error": repr(e),
                "started_ts_ms": ts,
                "alive": False,
                "stdout_log": str(out_log),
                "stderr_log": str(err_log),
            }
            entry["status"] = "ERROR"
            entry["reason"] = f"spawn_error: {repr(e)}"
            skipped.append(f"{label}: spawn_error {repr(e)}")

        subaccounts_state[label] = entry

    out = {
        "ts_ms": _now_ms(),
        "mode": mode,
        "boot_record": str(BOOT),
        "manifest": str(MANIFEST),
        "only_labels": sorted(list(only_set)),
        "max_procs": max_procs,
        "started": started,
        "skipped": skipped,
        "procs": procs,
        # canonical field the cockpit hydrator reads
        "subaccounts": subaccounts_state,
    }
    OUT.write_text(json.dumps(out, indent=2), encoding="utf-8")

    print(f"OK: orchestrator_v1 mode={mode} started={len(started)} max_procs={max_procs}")
    print(f"OK: state={OUT}")
    print(f"OK: boot_record={BOOT}")
    print(f"OK: logs_dir={LOGDIR}")
    return 0


if __name__ == "__main__":
    got = _acquire_lock_safe(LOCK_FP)
    if not got:
        _fatal(f"Could not acquire lock: {LOCK_FP}")
    try:
        raise SystemExit(main())
    finally:
        _release_lock_safe(LOCK_FP)
