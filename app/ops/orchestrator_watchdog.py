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
    p.write_text(json.dumps(d, indent=2, sort_keys=True), encoding="utf-8")


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
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONLEGACYWINDOWSSTDIO"] = "0"
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


def _kill_pid(pid: Optional[int]) -> None:
    if not isinstance(pid, int) or pid <= 0:
        return
    try:
        subprocess.run(["taskkill", "/PID", str(pid), "/F"], capture_output=True, text=True)
    except Exception:
        pass


def main() -> int:
    STATE.mkdir(parents=True, exist_ok=True)
    LOGDIR.mkdir(parents=True, exist_ok=True)

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
    alive_labels: List[str] = []
    skipped_backoff: List[str] = []

    now = _now_ms()

    for r in rows:
        label = str(r.get("account_label") or "").strip()
        if not label:
            continue
        if only_set and label not in only_set:
            continue
        if not _should_run(r):
            continue

        checked.append(label)

        # existing supervisor from orch_state
        pinfo = procs.get(label) if isinstance(procs.get(label), dict) else {}
        pid_val = pinfo.get("pid")
        try:
            pid_int = int(pid_val) if pid_val is not None else None
        except Exception:
            pid_int = None

        is_alive = _pid_alive(pid_int)

        st = labels_state.get(label) if isinstance(labels_state.get(label), dict) else {}
        hist = st.get("restart_history_ms") if isinstance(st.get("restart_history_ms"), list) else []
        hist = [int(x) for x in hist if isinstance(x, int) or (isinstance(x, str) and x.isdigit())]
        hist = _prune_history_ms(hist, now)

        restart_count = int(st.get("restart_count") or 0)
        last_restart_ts = int(st.get("last_restart_ts_ms") or 0)
        blocked_flag = bool(st.get("blocked")) if "blocked" in st else False
        blocked_reason = st.get("blocked_reason")

        # too many restarts -> block
        if (len(hist) >= MAX_RESTARTS) or (restart_count >= MAX_RESTARTS):
            blocked_flag = True
            blocked_reason = blocked_reason or "too_many_restarts"

        if blocked_flag:
            # If blocked, ensure process is dead
            if is_alive:
                _kill_pid(pid_int)
            procs[label] = {
                **(pinfo if isinstance(pinfo, dict) else {}),
                "pid": None,
                "alive": False,
                "blocked": True,
                "blocked_reason": blocked_reason,
                "last_checked_ts_ms": now,
            }
            labels_state[label] = {
                **(st if isinstance(st, dict) else {}),
                "blocked": True,
                "blocked_reason": blocked_reason,
                "restart_history_ms": hist,
                "restart_count": restart_count,
                "last_checked_ts_ms": now,
                "last_restart_ts_ms": last_restart_ts,
            }
            blocked.append(label)
            continue

        if is_alive:
            procs[label] = {
                **(pinfo if isinstance(pinfo, dict) else {}),
                "pid": pid_int,
                "alive": True,
                "blocked": False,
                "last_checked_ts_ms": now,
            }
            labels_state[label] = {
                **(st if isinstance(st, dict) else {}),
                "blocked": False,
                "blocked_reason": None,
                "restart_history_ms": hist,
                "restart_count": restart_count,
                "last_checked_ts_ms": now,
                "last_restart_ts_ms": last_restart_ts,
            }
            alive_labels.append(label)
            continue

        # dead -> respect backoff
        backoff_sec = _backoff_for(restart_count)
        if last_restart_ts and (now - last_restart_ts) < int(backoff_sec * 1000):
            skipped_backoff.append(label)
            procs[label] = {
                **(pinfo if isinstance(pinfo, dict) else {}),
                "pid": None,
                "alive": False,
                "blocked": False,
                "backoff_sec": backoff_sec,
                "last_checked_ts_ms": now,
            }
            labels_state[label] = {
                **(st if isinstance(st, dict) else {}),
                "blocked": False,
                "blocked_reason": None,
                "restart_history_ms": hist,
                "restart_count": restart_count,
                "last_checked_ts_ms": now,
                "last_restart_ts_ms": last_restart_ts,
            }
            continue

        # restart
        info = _start_supervisor(label)
        restarted.append(label)

        hist = _prune_history_ms(hist + [now], now)
        restart_count = restart_count + 1

        procs[label] = {
            **(pinfo if isinstance(pinfo, dict) else {}),
            **info,
            "blocked": False,
            "blocked_reason": None,
            "restart_count": restart_count,
            "backoff_sec": _backoff_for(restart_count),
            "last_checked_ts_ms": now,
        }

        labels_state[label] = {
            **(st if isinstance(st, dict) else {}),
            "blocked": False,
            "blocked_reason": None,
            "restart_history_ms": hist,
            "restart_count": restart_count,
            "last_restart_ts_ms": now,
            "last_checked_ts_ms": now,
        }

    orch_out = {"ts_ms": now, "procs": procs}
    wd_out = {"ts_ms": now, "labels": labels_state, "checked": checked}

    _write_json(ORCH_STATE, orch_out)
    _write_json(WATCHDOG_STATE, wd_out)

    print(
        f"[watchdog] checked={len(checked)} alive={len(alive_labels)} restarted={len(restarted)} "
        f"blocked={len(blocked)} backoff_skips={len(skipped_backoff)}"
    )
    if restarted:
        print("[watchdog] restarted:", ", ".join(restarted[:50]))
    if blocked:
        print("[watchdog] blocked:", ", ".join(blocked[:50]))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
