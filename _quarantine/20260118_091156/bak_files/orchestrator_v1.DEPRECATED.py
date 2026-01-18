# === FLASHBACK_ROOT_INJECT ===
from __future__ import annotations
import sys
from pathlib import Path
import json
import os
import subprocess
import time
import socket
import hashlib
import platform
import traceback
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
# === FLASHBACK_ROOT_INJECT ===



EXPECTED = ROOT / ".venv" / "Scripts" / "python.exe"

if Path(sys.executable).resolve() != EXPECTED.resolve():
    raise SystemExit(
        f"FATAL: orchestrator_v1 must be run with {EXPECTED}, "
        f"but got {sys.executable}"
    )

# Force this process environment to be venv-pinned too.
os.environ["FLASHBACK_PY"] = str(EXPECTED)
os.environ["ORCH_PY"] = str(EXPECTED)

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
FATAL_LOG = STATE / "orchestrator_fatal.log"

# IMPORTANT:
# Your system uses LEARN_DRY/PAPER/LIVE semantics across the fleet.
# Orchestrator must accept them as valid top-level modes.
ALLOWED_MODES = {"DRY", "LEARN_DRY", "PAPER", "LIVE_CANARY", "LIVE"}
DEFAULT_MODE = "DRY"


# =========================
# UTILITIES
# =========================

def _now_ms() -> int:
    return int(time.time() * 1000)


def _append_fatal(line: str) -> None:
    try:
        STATE.mkdir(parents=True, exist_ok=True)
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        FATAL_LOG.parent.mkdir(parents=True, exist_ok=True)
        with FATAL_LOG.open("a", encoding="utf-8", errors="replace") as f:
            f.write(f"[{ts}] {line}\n")
    except Exception:
        pass


def _fatal(msg: str) -> None:
    # Make failures impossible to miss: stderr + persistent file.
    try:
        print(f"FATAL: {msg}", file=sys.stderr)
        try:
            sys.stderr.flush()
        except Exception:
            pass
    finally:
        _append_fatal(f"FATAL: {msg}")
    raise SystemExit(1)


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        r = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}"],
            capture_output=True,
            text=True,
            timeout=3,
        )
        return str(pid) in (r.stdout or "")
    except subprocess.TimeoutExpired:
        return False
    except Exception:
        return False


def _run_powershell_json(ps: str) -> Any:
    """
    Run a PowerShell snippet that ends with ConvertTo-Json -Compress.
    Return parsed JSON (dict/list) or None.
    """
    try:
        r = subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", ps],
            capture_output=True,
            text=True,
            timeout=10,
        )
        out = (r.stdout or "").strip()
        if not out:
            return None
        return json.loads(out)
    except Exception:
        return None


def _kill_rogue_orchestrators() -> None:
    """
    HARD GUARANTEE:
    - Exactly ONE orchestrator process may exist on this machine.
    - If a second one appears (Python312 or even a second venv), kill it.

    Why:
    Your evidence shows the venv orchestrator can spawn a Python312 orchestrator.
    We enforce reality.
    """
    me = os.getpid()
    expected_exe = str(EXPECTED.resolve()).lower()

    ps = r"""
$me = """ + str(me) + r"""
$procs = Get-CimInstance Win32_Process |
  Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -like '*orchestrator_v1.py*' } |
  Select-Object ProcessId, ExecutablePath, CommandLine

$procs | ConvertTo-Json -Compress
"""
    data = _run_powershell_json(ps)
    if not data:
        return

    rows = data if isinstance(data, list) else [data]

    for p in rows:
        try:
            pid = int(p.get("ProcessId") or 0)
            exe = str(p.get("ExecutablePath") or "").strip().lower()
            cmd = str(p.get("CommandLine") or "")
        except Exception:
            continue

        if pid <= 0 or pid == me:
            continue

        # Kill any orchestrator not running with the expected venv python,
        # OR any duplicate orchestrator even if venv.
        if exe and exe != expected_exe:
            kill_reason = f"exe_mismatch exe={exe}"
        else:
            kill_reason = "duplicate_orchestrator"

        try:
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            print(f"[orchestrator] KILLED rogue orchestrator pid={pid} reason={kill_reason} cmd={cmd}")
        except Exception:
            pass


def _acquire_lock_safe(lock_path: Path) -> bool:
    """
    Acquire orchestrator lock defensively across differing writer_lock implementations.

    HARD RULES:
      - writer_lock must exist (fail-closed)
      - lock file must be owned by THIS process PID (prevents stale/foreign locks)
    """
    try:
        lock_path.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    if not callable(_acquire_lock):
        _fatal("writer_lock.acquire_lock unavailable — refusing to start (fail-closed)")

    try:
        try:
            _acquire_lock()
        except TypeError:
            _acquire_lock(str(lock_path))
    except Exception as e:
        _fatal(f"Could not acquire writer lock: {repr(e)}")

    try:
        if lock_path.exists():
            txt = lock_path.read_text(encoding="utf-8", errors="replace").strip()
            digits = "".join([c for c in txt if c.isdigit()])
            if digits:
                lock_pid = int(digits)
                if lock_pid != os.getpid():
                    _fatal(f"Writer lock owned by PID {lock_pid}, current PID {os.getpid()} (foreign/stale lock)")
    except Exception:
        pass

    return True


def _release_lock_safe(lock_path: Path) -> None:
    try:
        if callable(_release_lock):
            try:
                _release_lock()
            except TypeError:
                try:
                    _release_lock(str(lock_path))
                except Exception:
                    pass
    except Exception:
        pass


def _resolve_orch_python(root: Path) -> str:
    """
    Permanently pin the interpreter used to spawn ALL workers.

    Priority:
      1) ORCH_PY
      2) FLASHBACK_PY
      3) <root>\\.venv\\Scripts\\python.exe if exists
      4) sys.executable fallback
    """
    env_orch = (os.getenv("ORCH_PY") or "").strip()
    if env_orch:
        return env_orch

    env_fb = (os.getenv("FLASHBACK_PY") or "").strip()
    if env_fb:
        return env_fb

    venv_py = root / ".venv" / "Scripts" / "python.exe"
    if venv_py.exists():
        return str(venv_py)

    return sys.executable


def _machine_fingerprint(python_path: str) -> Dict[str, Any]:
    h = hashlib.sha256()
    h.update(platform.node().encode(errors="ignore"))
    h.update(platform.platform().encode(errors="ignore"))
    h.update(python_path.encode(errors="ignore"))
    return {
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "python": python_path,
        "fingerprint": h.hexdigest(),
    }


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


def main() -> int:
    _kill_rogue_orchestrators()

    if os.getenv("ORCHESTRATOR_PARENT", "").strip() == "1":
        _fatal("Nested orchestrator execution blocked (ORCHESTRATOR_PARENT=1)")

    STATE.mkdir(parents=True, exist_ok=True)
    LOGDIR.mkdir(parents=True, exist_ok=True)

    mode = (os.environ.get("FLASHBACK_MODE") or DEFAULT_MODE).strip().upper()
    if mode not in ALLOWED_MODES:
        _fatal(f"Invalid FLASHBACK_MODE={mode} (allowed={sorted(ALLOWED_MODES)})")

    orch_py = _resolve_orch_python(ROOT)

    fp = _machine_fingerprint(orch_py)
    boot = {
        "ts_ms": _now_ms(),
        "mode": mode,
        "root": str(ROOT),
        "orch_python": orch_py,
        "machine": fp,
    }
    BOOT.write_text(json.dumps(boot, indent=2), encoding="utf-8")

    rows = _load_manifest_rows()

    only = (os.getenv("ORCH_ONLY_LABELS") or "").strip()
    only_set = {x.strip() for x in only.split(",") if x.strip()}

    max_procs = 10

    procs: Dict[str, Any] = {}
    started: List[str] = []
    skipped: List[str] = []

    subaccounts_state: Dict[str, Any] = {}
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

        if only_set and label not in only_set:
            entry["status"] = "SKIPPED"
            entry["reason"] = "not in ORCH_ONLY_LABELS"
            skipped.append(f"{label}: not in ORCH_ONLY_LABELS")
            subaccounts_state[label] = entry
            continue

        if not should_run:
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

        if started_count >= max_procs:
            entry["status"] = "SKIPPED"
            entry["reason"] = f"ORCH_MAX_PROCS cap ({max_procs})"
            skipped.append(f"{label}: ORCH_MAX_PROCS cap ({max_procs})")
            subaccounts_state[label] = entry
            continue

        env = os.environ.copy()
        env["ACCOUNT_LABEL"] = label
        env["FLASHBACK_MODE"] = mode

        env["FLASHBACK_PY"] = orch_py
        env["ORCH_PY"] = orch_py
        env["ORCHESTRATOR_PARENT"] = "1"
        env["PYTHONUTF8"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"

        state_dir = ROOT / "state"
        lab = (label or "").strip().lower()

        if lab in ("", "main"):
            env["POSITIONS_BUS_PATH"] = str(state_dir / "positions_bus.json")
            env["ORDERBOOK_BUS_PATH"] = str(state_dir / "orderbook_bus.json")
            env["TRADES_BUS_PATH"] = str(state_dir / "trades_bus.json")
            env["PUBLIC_TRADES_PATH"] = str(state_dir / "public_trades.jsonl")
            env["EXEC_BUS_PATH"] = str(state_dir / "ws_executions.jsonl")
        else:
            env["POSITIONS_BUS_PATH"] = str(state_dir / f"positions_bus_{lab}.json")
            env["ORDERBOOK_BUS_PATH"] = str(state_dir / f"orderbook_bus_{lab}.json")
            env["TRADES_BUS_PATH"] = str(state_dir / f"trades_bus_{lab}.json")
            env["PUBLIC_TRADES_PATH"] = str(state_dir / f"public_trades_{lab}.jsonl")
            env["EXEC_BUS_PATH"] = str(state_dir / f"ws_executions_{lab}.jsonl")

        cmd = [orch_py, "-m", "app.bots.supervisor_ai_stack"]

        ts = _now_ms()
        out_log = LOGDIR / f"{label}.stdout.log"
        err_log = LOGDIR / f"{label}.stderr.log"

        try:
            with out_log.open("ab") as fo, err_log.open("ab") as fe:
                header = f"\n\n=== START {label} ts_ms={ts} mode={mode} orch_py={orch_py} cmd={cmd} ===\n"
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
                "orch_py": orch_py,
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
                "orch_py": orch_py,
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
        "orch_python": orch_py,
        "boot_record": str(BOOT),
        "manifest": str(MANIFEST),
        "only_labels": sorted(list(only_set)),
        "max_procs": max_procs,
        "started": started,
        "skipped": skipped,
        "procs": procs,
        "subaccounts": subaccounts_state,
    }
    OUT.write_text(json.dumps(out, indent=2), encoding="utf-8")

    print(f"OK: orchestrator_v1 mode={mode} started={len(started)} max_procs={max_procs}")
    print(f"OK: orch_python={orch_py}")
    print(f"OK: state={OUT}")
    print(f"OK: boot_record={BOOT}")
    print(f"OK: logs_dir={LOGDIR}")

    tick_sec = _as_int(os.getenv("ORCH_TICK_SEC"), 5)
    if tick_sec < 1:
        tick_sec = 5

    restart_dead = os.getenv("ORCH_RESTART_DEAD", "true").strip().lower() in ("1", "true", "yes", "y", "on")

    def _read_hb_ms(label: str) -> int:
        try:
            fp = STATE / f"ws_switchboard_heartbeat_{label}.txt"
            if not fp.exists():
                return 0
            txt = fp.read_text(encoding="utf-8", errors="replace").strip()
            v = int("".join([c for c in txt if c.isdigit()]) or "0")
            return v
        except Exception:
            return 0

    print(f"OK: tick_sec={tick_sec} restart_dead={restart_dead}")

    while True:
        _kill_rogue_orchestrators()

        for label, meta in list(procs.items()):
            pid = int(meta.get("pid") or 0)
            alive = _pid_alive(pid) if pid else False
            meta["alive"] = bool(alive)

            entry = subaccounts_state.get(label) or {}
            entry["pid"] = pid or None
            entry["alive"] = bool(alive)
            entry["online"] = bool(alive)
            entry["status"] = "RUNNING" if alive else "DEAD"
            entry["reason"] = None if alive else "process not alive"
            entry["last_heartbeat_ms"] = _read_hb_ms(label)
            subaccounts_state[label] = entry

        out = {
            "ts_ms": _now_ms(),
            "mode": mode,
            "orch_python": orch_py,
            "boot_record": str(BOOT),
            "manifest": str(MANIFEST),
            "only_labels": sorted(list(only_set)),
            "max_procs": max_procs,
            "started": started,
            "skipped": skipped,
            "procs": procs,
            "subaccounts": subaccounts_state,
        }

        try:
            OUT.write_text(json.dumps(out, indent=2), encoding="utf-8")
        except Exception:
            pass

        time.sleep(tick_sec)


if __name__ == "__main__":
    try:
        _kill_rogue_orchestrators()

        got = _acquire_lock_safe(LOCK_FP)
        if not got:
            _fatal(f"Could not acquire lock: {LOCK_FP}")

        try:
            raise SystemExit(main())
        finally:
            _release_lock_safe(LOCK_FP)

    except BaseException as e:
        # If anything kills us before printing, we still write proof.
        _append_fatal("UNHANDLED_EXCEPTION: " + repr(e))
        _append_fatal(traceback.format_exc())
        try:
            print("FATAL: Unhandled exception in orchestrator_v1. See state/orchestrator_fatal.log", file=sys.stderr)
            sys.stderr.flush()
        except Exception:
            pass
        raise
