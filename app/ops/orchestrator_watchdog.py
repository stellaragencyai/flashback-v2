from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[2]
STATE = ROOT / "state"
LOGDIR = STATE / "orchestrator_logs"
MANIFEST = ROOT / "config" / "fleet_manifest.yaml"

ORCH_STATE = STATE / "orchestrator_state.json"
WATCHDOG_STATE = STATE / "orchestrator_watchdog.json"

# Safety: stop infinite resurrection
MAX_RESTARTS = 5                 # after this, BLOCK
WINDOW_SEC = 10 * 60             # rolling window for MAX_RESTARTS
BACKOFF_MIN = 2.0
BACKOFF_MAX = 60.0

def _now_ms() -> int:
    return int(time.time() * 1000)

def _load_json(p: Path) -> dict:
    try:
        if not p.exists():
            return {}
        return json.loads(p.read_text(encoding="utf-8", errors="ignore") or "{}")
    except Exception:
        return {}

def _write_json(p: Path, d: dict) -> None:
    p.write_text(json.dumps(d, indent=2), encoding="utf-8")

def _pid_alive(pid: Optional[int]) -> bool:
    if not isinstance(pid, int) or pid <= 0:
        return False
    try:
        r = subprocess.run(["tasklist", "/FI", f"PID eq {pid}"], capture_output=True, text=True)
        return str(pid) in (r.stdout or "")
    except Exception:
        return False

def _load_manifest_rows() -> list[dict[str, Any]]:
    if not MANIFEST.exists():
        return []
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

def _should_run(row: dict[str, Any]) -> bool:
    enabled = bool(row.get("enabled", True))
    enable_ai_stack = bool(row.get("enable_ai_stack", True))
    mode = str(row.get("automation_mode") or "").strip().upper()
    mode_ok = mode not in ("", "OFF", "DISABLED", "NONE")
    return bool(enabled and enable_ai_stack and mode_ok)

def _backoff_for(restart_count: int) -> float:
    # 0->2,1->4,2->8... capped
    b = BACKOFF_MIN * (2 ** max(0, restart_count))
    return float(min(max(b, BACKOFF_MIN), BACKOFF_MAX))

def _prune_history_ms(history: list[int], now_ms: int) -> list[int]:
    cutoff = now_ms - (WINDOW_SEC * 1000)
    return [x for x in history if isinstance(x, int) and x >= cutoff]

def _start_supervisor(label: str) -> dict:
    STATE.mkdir(parents=True, exist_ok=True)
    LOGDIR.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["ACCOUNT_LABEL"] = label

    cmd = [sys.executable, "-m", "app.bots.supervisor_ai_stack"]

    ts = _now_ms()
    out_log = LOGDIR / f"{label}.stdout.log"
    err_log = LOGDIR / f"{label}.stderr.log"

    with out_log.open("ab") as fo, err_log.open("ab") as fe:
        fo.write(f"\n\n=== WATCHDOG RESTART {label} ts_ms={ts} cmd={cmd} ===\n".encode("utf-8", errors="ignore"))
        fe.write(f"\n\n=== WATCHDOG RESTART {label} ts_ms={ts} cmd={cmd} ===\n".encode("utf-8", errors="ignore"))

        p = subprocess.Popen(
            cmd,
            cwd=str(ROOT),
            env=env,
            stdout=fo,
            stderr=fe,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
        )

    # give it a moment to crash loudly
    time.sleep(0.25)
    pid = int(p.pid)
    alive = _pid_alive(pid)

    return {
        "pid": pid,
        "alive": bool(alive),
        "cmd": cmd,
        "started_ts_ms": ts,
        "stdout_log": str(out_log),
        "stderr_log": str(err_log),
    }

def main() -> int:
    STATE.mkdir(parents=True, exist_ok=True)

    rows = _load_manifest_rows()
    only = (os.getenv("ORCH_ONLY_LABELS") or "").strip()
    only_set = set([x.strip() for x in only.split(",") if x.strip()]) if only else set()

    orch = _load_json(ORCH_STATE)
    procs = (orch.get("procs") or {}) if isinstance(orch, dict) else {}
    if not isinstance(procs, dict):
        procs = {}

    wd = _load_json(WATCHDOG_STATE)
    labels_state = (wd.get("labels") or {}) if isinstance(wd, dict) else {}
    if not isinstance(labels_state, dict):
        labels_state = {}

    checked: List[str] = []
    restarted: List[str] = []
    blocked: List[str] = []

    # Build expected labels from manifest
    for r in rows:
        label = str(r.get("account_label") or "").strip()
        if not label:
            continue
        if only_set and label not in only_set:
            continue
        if not _should_run(r):
            continue

        checked.append(label)

        # existing supervisor from orch_state (may be stale if previous watchdog didn't update it)
        pinfo = procs.get(label) if isinstance(procs.get(label), dict) else {}
        pid = pinfo.get("pid")
        try:
            pid_int = int(pid) if pid is not None else None
        except Exception:
            pid_int = None

        alive = _pid_alive(pid_int)

        # watchdog per-label state
        st = labels_state.get(label) if isinstance(labels_state.get(label), dict) else {}
        now = _now_ms()
        hist = st.get("restart_history_ms") if isinstance(st.get("restart_history_ms"), list) else []
        hist = [int(x) for x in hist if isinstance(x, int) or (isinstance(x, str) and x.isdigit())]
        hist = _prune_history_ms(hist, now)

        restart_count = int(st.get("restart_count") or 0)
        blocked_flag = bool(st.get("blocked")) if "blocked" in st else False
        blocked_reason = st.get("blocked_reason")

        # If we have too many restarts in window, block hard.
        if (len(hist) >= MAX_RESTARTS) or (restart_count >= MAX_RESTARTS):
            blocked_flag = True
            blocked_reason = blocked_reason or "too_many_restarts"
        # KILL_ON_BLOCK: blocked means STOP the process (best-effort)
        try:
            _pid = int(pid) if pid is not None else None
        except Exception:
            _pid = None
        
        if _pid:
            try:
                import subprocess
                subprocess.run(["taskkill", "/PID", str(_pid), "/F"], capture_output=True, text=True)
            except Exception:
                pass
        
        # Ensure our watchdog view reflects blocked as not alive
        alive = False
        pid = None
        blocked.append(label)
