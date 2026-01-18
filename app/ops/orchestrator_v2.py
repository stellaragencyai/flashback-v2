#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback Orchestrator v2 (Minimal + Reliable + Terminal Health Proof)

Goal:
- Launch multiple subaccounts at once by spawning supervisor_ai_stack.py per lane.
- Enforce venv python.
- Enforce per-account file isolation via env vars.
- Write per-account stdout/stderr logs.
- Optionally restart crashed lanes.
- NEW: Terminal health reporter (FILES > LOGS > METRICS > DASHBOARD > OPINION)

Default lanes: flashback01..flashback09
Optional: --include-main, --include-10

NEW (v2.1):
- HARD single-instance lock per lane label (prevents duplicate orchestrators)
  - lock file: state/orchestrator_locks/orchestrator_v2_<label>.lock
  - if lock exists and PID is alive -> this instance exits immediately
  - if lock exists but PID is dead -> lock is reclaimed automatically
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple


@dataclass
class LaneProc:
    label: str
    popen: subprocess.Popen
    stdout_path: Path
    stderr_path: Path
    started_ts: float
    stdout_f: object
    stderr_f: object


# ----------------------------
# Paths / environment
# ----------------------------

def _root_from_here() -> Path:
    # C:\Flashback\app\ops\orchestrator_v2.py -> ROOT is 3 parents up
    return Path(__file__).resolve().parents[2]


def _venv_python(root: Path) -> Path:
    py = root / ".venv" / "Scripts" / "python.exe"
    if not py.exists():
        raise FileNotFoundError(f"Missing venv python: {py}")
    return py


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _norm_path_str(p: Path) -> str:
    # Normalize for comparisons on Windows (case-insensitive, slashes, etc.)
    try:
        return str(p.resolve()).lower()
    except Exception:
        return str(p).lower()


def _enforce_running_under_venv(py_expected: Path) -> None:
    """
    Hard-fails if sys.executable != venv python.
    """
    exe_now = _norm_path_str(Path(sys.executable))
    exe_need = _norm_path_str(py_expected)
    if exe_now != exe_need:
        msg = (
            "[orchestrator_v2] HARD FAIL: orchestrator_v2 must be launched with venv python.\n"
            f"[orchestrator_v2]   expected: {py_expected}\n"
            f"[orchestrator_v2]   actual  : {sys.executable}\n"
            "[orchestrator_v2] Fix: run like:\n"
            f'  {py_expected} .\\app\\ops\\orchestrator_v2.py --labels "flashback01,flashback02,..."'
        )
        print(msg)
        raise SystemExit(2)


# ----------------------------
# Single-instance lock
# ----------------------------

def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        if os.name != "nt":
            os.kill(pid, 0)
            return True
        # Windows: use tasklist (available by default)
        out = subprocess.check_output(["tasklist", "/FI", f"PID eq {pid}"], text=True, errors="replace")
        # When PID not found, tasklist prints "No tasks are running which match the specified criteria."
        return str(pid) in out and "No tasks are running" not in out
    except Exception:
        return False


def _lock_dir(root: Path) -> Path:
    d = root / "state" / "orchestrator_locks"
    _ensure_dir(d)
    return d


def _lock_path(root: Path, label: str) -> Path:
    safe = (label or "unknown").strip().lower()
    return _lock_dir(root) / f"orchestrator_v2_{safe}.lock"


def _acquire_label_lock_or_exit(root: Path, label: str) -> None:
    """
    Enforce that only ONE orchestrator instance controls a given label.
    If lock exists and PID is alive -> exit immediately.
    If lock exists but PID dead -> reclaim lock.
    """
    lp = _lock_path(root, label)
    this_pid = os.getpid()
    now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    if lp.exists():
        try:
            raw = lp.read_text(encoding="utf-8", errors="replace").strip().splitlines()
            old_pid = int(raw[0].strip()) if raw and raw[0].strip().isdigit() else -1
        except Exception:
            old_pid = -1

        if old_pid > 0 and old_pid != this_pid and _pid_is_alive(old_pid):
            print(
                f"[orchestrator_v2] HARD EXIT: label lock already held for {label}\n"
                f"[orchestrator_v2]   lock_file={lp}\n"
                f"[orchestrator_v2]   holder_pid={old_pid}\n"
                f"[orchestrator_v2] This prevents duplicate orchestrators from wrecking state. Exiting."
            )
            raise SystemExit(4)

    # Reclaim / create lock
    payload = "\n".join([
        str(this_pid),
        now,
        f"sys.executable={sys.executable}",
        f"cmd={' '.join(sys.argv)}",
    ]) + "\n"
    lp.write_text(payload, encoding="utf-8", errors="replace")
    print(f"[orchestrator_v2] LOCK ACQUIRED label={label} file={lp} pid={this_pid}")
    sys.stdout.flush()


def _release_label_lock(root: Path, label: str) -> None:
    lp = _lock_path(root, label)
    try:
        if lp.exists():
            raw = lp.read_text(encoding="utf-8", errors="replace").strip().splitlines()
            old_pid = int(raw[0].strip()) if raw and raw[0].strip().isdigit() else -1
            if old_pid == os.getpid():
                lp.unlink(missing_ok=True)  # type: ignore[arg-type]
    except Exception:
        pass


def _lane_env(root: Path, label: str) -> Dict[str, str]:
    """
    The entire point: isolate each lane's state paths.
    """
    state = root / "state"
    _ensure_dir(state)

    if (label or "").strip().lower() in ("main", "primary"):
        ai_events_dir = state / "ai_events"
    else:
        lab = (label or "").strip().lower()
        ai_events_dir = state / f"ai_events_{lab}"
    _ensure_dir(ai_events_dir)

    env = dict(os.environ)

    env["ROOT"] = str(root)
    env["ACCOUNT_LABEL"] = label

    # Per-lane signals input for executor_v2 (keeps cursors isolated)
    env["EXEC_SIGNAL_FILE"] = str(root / "signals" / f"observed_{label}.jsonl")

    # Tell supervisor: orchestrator already loaded env into process context (avoid double dotenv)
    env["ORCH_ENV_LOADED"] = "1"

    # Keep python clean and deterministic
    env["PYTHONNOUSERSITE"] = "1"
    env["PYTHONUTF8"] = env.get("PYTHONUTF8", "1")
    env["PYTHONIOENCODING"] = env.get("PYTHONIOENCODING", "utf-8")

    # Bus paths
    env["POSITIONS_BUS_PATH"] = str(state / f"positions_bus_{label}.json")
    env["ORDERBOOK_BUS_PATH"] = str(state / f"orderbook_bus_{label}.json")
    env["TRADES_BUS_PATH"] = str(state / f"trades_bus_{label}.json")
    env["PUBLIC_TRADES_PATH"] = str(state / f"public_trades_{label}.jsonl")

    # Execution stream (critical isolation)
    env["EXEC_BUS_PATH"] = str(state / f"ws_executions_{label}.jsonl")

    # Legacy compatibility: some workers prefer EXECUTIONS_PATH
    env["EXECUTIONS_PATH"] = str(state / f"ws_executions_{label}.jsonl")

    # Outcome recorder isolation
    env["TRADE_OUTCOME_CURSOR_PATH"] = str(state / f"trade_outcome_recorder_{label}.cursor")
    env["AI_EVENTS_DIR"] = str(ai_events_dir)

    # -----------------------------------------------------------------
    # HARD lane isolation: decisions + inbox MUST be per-label
    # -----------------------------------------------------------------
    _lab = (label or "").strip().lower() or "unknown"
    env["AI_DECISIONS_PATH"] = str(state / f"ai_decisions_{_lab}.jsonl")
    env["AI_EVENTS_INBOX_PATH"] = str(state / f"ai_events_inbox_{_lab}.jsonl")
    env["AI_EVENTS_INBOX_CURSOR_PATH"] = str(state / f"ai_events_inbox_{_lab}.cursor")
    env["AI_EVENTS_INBOX_BADLINES_PATH"] = str(state / f"ai_events_inbox_{_lab}.bad.jsonl")

    # Force writers to refuse global fallbacks (safety rail)
    env["LANE_REQUIRED"] = "1"

    # Heartbeat path (ws_switchboard already uses label, but enforce determinism)
    env["WS_HEARTBEAT_PATH"] = str(state / f"ws_switchboard_heartbeat_{label}.txt")

    return env


def _spawn_lane(root: Path, py: Path, label: str, logs_dir: Path) -> LaneProc:
    _ensure_dir(logs_dir)

    stdout_path = logs_dir / f"{label}.stdout.log"
    stderr_path = logs_dir / f"{label}.stderr.log"

    env = _lane_env(root, label)

    cmd = [
        str(py),
        str(root / "app" / "bots" / "supervisor_ai_stack.py"),
    ]

    # Append mode keeps history across restarts
    stdout_f = open(stdout_path, "a", encoding="utf-8", errors="replace")
    stderr_f = open(stderr_path, "a", encoding="utf-8", errors="replace")

    creationflags = 0
    if os.name == "nt":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP  # type: ignore[attr-defined]

    p = subprocess.Popen(
        cmd,
        cwd=str(root),
        env=env,
        stdout=stdout_f,
        stderr=stderr_f,
        creationflags=creationflags,
    )

    return LaneProc(
        label=label,
        popen=p,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        started_ts=time.time(),
        stdout_f=stdout_f,
        stderr_f=stderr_f,
    )


def _parse_labels(s: Optional[str]) -> List[str]:
    if not s:
        return [f"flashback{n:02d}" for n in range(1, 10)]
    parts = [x.strip() for x in s.split(",") if x.strip()]
    out: List[str] = []
    for x in parts:
        if x.lower() in ("main", "flashback10"):
            out.append(x.lower() if x.lower() == "main" else "flashback10")
        else:
            if x.isdigit() and len(x) in (1, 2):
                out.append(f"flashback{int(x):02d}")
            else:
                out.append(x)
    return out


# ----------------------------
# Health / telemetry helpers
# ----------------------------

def _file_age_seconds(path: Path) -> Optional[float]:
    try:
        if not path.exists():
            return None
        return max(0.0, time.time() - path.stat().st_mtime)
    except Exception:
        return None


def _tail_lines(path: Path, n: int) -> List[str]:
    if n <= 0:
        return []
    try:
        if not path.exists():
            return []
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            end = f.tell()
            block = 4096
            data = b""
            pos = end
            while pos > 0 and data.count(b"\n") < (n + 1):
                step = block if pos - block > 0 else pos
                pos -= step
                f.seek(pos)
                data = f.read(step) + data
            text = data.decode("utf-8", errors="replace")
        lines = text.splitlines()
        return lines[-n:]
    except Exception:
        return []


def _stderr_signal(lines: List[str]) -> Tuple[str, str]:
    if not lines:
        return "OK", ""

    blob = "\n".join(lines[-200:])

    if "Traceback (most recent call last)" in blob:
        last = ""
        for ln in reversed(lines):
            s = ln.strip()
            if s:
                last = s
                break
        return "FAIL", (last or "Traceback detected")

    for token in ("Import failed:", "PermissionError:", "WinError 5", "ERROR", "CRITICAL"):
        if token in blob:
            hit = ""
            for ln in reversed(lines):
                if token in ln:
                    hit = ln.strip()
                    break
            return "WARN", (hit or token)

    return "OK", ""


def _lane_paths(root: Path, label: str) -> Dict[str, Path]:
    state = root / "state"
    lab = label

    hb = state / f"ws_switchboard_heartbeat_{lab}.txt"
    pos_bus = state / f"positions_bus_{lab}.json"
    exec_bus = state / f"ws_executions_{lab}.jsonl"

    if label.lower() in ("main", "primary"):
        outcomes = state / "ai_events" / "outcomes.v1.jsonl"
    else:
        outcomes = state / f"ai_events_{label.lower()}" / "outcomes.v1.jsonl"

    return {
        "hb": hb,
        "pos_bus": pos_bus,
        "exec_bus": exec_bus,
        "outcomes": outcomes,
    }


def _fmt_age(age: Optional[float]) -> str:
    if age is None:
        return "MISSING"
    return f"{age:.2f}s"


def _health_report(root: Path, procs: Dict[str, LaneProc], warn_secs: float, tail_n: int) -> Tuple[int, int, int]:
    now_utc = time.strftime("%Y-%m-%d %H:%M:%SZ", time.gmtime())

    ok = 0
    warn = 0
    fail = 0

    print(f"\n[orchestrator_v2] HEALTH REPORT @ {now_utc}")
    print(f"[orchestrator_v2] thresholds: warn_secs={warn_secs} tail={tail_n} lines")
    print("-" * 96)

    for lab, lp in procs.items():
        rc = lp.popen.poll()
        running = (rc is None)

        paths = _lane_paths(root, lab)
        hb_age = _file_age_seconds(paths["hb"])
        pos_age = _file_age_seconds(paths["pos_bus"])
        exec_age = _file_age_seconds(paths["exec_bus"])
        out_age = _file_age_seconds(paths["outcomes"])

        crit_missing = (hb_age is None) or (pos_age is None)
        crit_stale = (hb_age is not None and hb_age > warn_secs) or (pos_age is not None and pos_age > warn_secs)

        tail = _tail_lines(lp.stderr_path, tail_n)
        sig_level, sig_msg = _stderr_signal(tail)

        lane_level = "OK"
        if not running:
            lane_level = "FAIL"
        elif sig_level == "FAIL":
            lane_level = "FAIL"
        elif sig_level == "WARN":
            lane_level = "WARN"
        elif crit_missing or crit_stale:
            lane_level = "WARN"

        if lane_level == "OK":
            ok += 1
            badge = "✅"
        elif lane_level == "WARN":
            warn += 1
            badge = "⚠️"
        else:
            fail += 1
            badge = "🛑"

        print(
            f"{badge} {lab:<12} pid={lp.popen.pid:<6} running={str(running):<5} "
            f"hb_age={_fmt_age(hb_age):>10} pos_age={_fmt_age(pos_age):>10} "
            f"exec_age={_fmt_age(exec_age):>10} out_age={_fmt_age(out_age):>10}"
        )

        if (crit_missing or crit_stale) and running:
            miss = []
            if hb_age is None:
                miss.append("hb_missing")
            elif hb_age > warn_secs:
                miss.append(f"hb_stale>{warn_secs}s")
            if pos_age is None:
                miss.append("pos_bus_missing")
            elif pos_age > warn_secs:
                miss.append(f"pos_bus_stale>{warn_secs}s")
            if miss:
                print(f"    telemetry: {' '.join(miss)}")

        if sig_level in ("WARN", "FAIL"):
            print(f"    stderr: {sig_level} {sig_msg}")

    print("-" * 96)
    print(f"[orchestrator_v2] HEALTH SUMMARY ok={ok} warn={warn} fail={fail}\n")
    sys.stdout.flush()
    return ok, warn, fail


# ----------------------------
# Main
# ----------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--labels", default="", help="Comma list: flashback01,flashback02,... or '01,02,03'. Default = 01..09")
    ap.add_argument("--include-main", action="store_true", help="Also spawn ACCOUNT_LABEL=main")
    ap.add_argument("--include-10", action="store_true", help="Also spawn ACCOUNT_LABEL=flashback10")
    ap.add_argument("--restart", action="store_true", help="Restart crashed lanes")
    ap.add_argument("--status-every", type=float, default=5.0, help="Seconds between status lines")

    ap.add_argument("--health", action="store_true", help="Print a terminal health report after launch")
    ap.add_argument("--health-every", type=float, default=0.0, help="If >0, print health report every N seconds")
    ap.add_argument("--health-warn-secs", type=float, default=15.0, help="Telemetry age threshold for WARN")
    ap.add_argument("--health-tail", type=int, default=60, help="Tail N lines from each lane stderr log")
    ap.add_argument("--fail-fast", action="store_true", help="Exit if any lane FAILs in health report")
    args = ap.parse_args()

    root = _root_from_here()
    py = _venv_python(root)

    _enforce_running_under_venv(py)

    logs_dir = root / "state" / "orchestrator_logs"
    _ensure_dir(logs_dir)

    labels = _parse_labels(args.labels)
    if args.include_main and "main" not in labels:
        labels.append("main")
    if args.include_10 and "flashback10" not in labels:
        labels.append("flashback10")

    seen = set()
    labels = [x for x in labels if not (x in seen or seen.add(x))]

    # Acquire hard locks BEFORE spawning anything
    for lab in labels:
        _acquire_label_lock_or_exit(root, lab)

    print(f"[orchestrator_v2] ROOT={root}")
    print(f"[orchestrator_v2] PY={py}")
    print(f"[orchestrator_v2] LOGS={logs_dir}")
    print(f"[orchestrator_v2] LABELS={labels}")
    print(f"[orchestrator_v2] RESTART={bool(args.restart)}")
    print(f"[orchestrator_v2] HEALTH={(bool(args.health) or float(args.health_every) > 0.0)} health_every={float(args.health_every)}")
    sys.stdout.flush()

    procs: Dict[str, LaneProc] = {}

    try:
        for lab in labels:
            lp = _spawn_lane(root, py, lab, logs_dir)
            procs[lab] = lp
            print(f"[orchestrator_v2] STARTED {lab} pid={lp.popen.pid} stdout={lp.stdout_path.name} stderr={lp.stderr_path.name}")
            sys.stdout.flush()

        if args.health or float(args.health_every) > 0.0:
            time.sleep(1.0)
            ok, wrn, fl = _health_report(
                root=root,
                procs=procs,
                warn_secs=float(args.health_warn_secs),
                tail_n=int(args.health_tail),
            )
            if args.fail_fast and fl > 0:
                print("[orchestrator_v2] FAIL-FAST: health report has FAIL lanes. Exiting.")
                return 3

        stop = False

        def _handle_sigint(signum, frame):
            nonlocal stop
            stop = True

        signal.signal(signal.SIGINT, _handle_sigint)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, _handle_sigint)

        last_status = 0.0
        last_health = 0.0

        while not stop:
            now = time.time()

            if now - last_status >= float(args.status_every):
                last_status = now
                alive = 0
                dead = 0
                for _lab, lp in procs.items():
                    rc = lp.popen.poll()
                    if rc is None:
                        alive += 1
                    else:
                        dead += 1
                print(f"[orchestrator_v2] STATUS alive={alive} dead={dead} (ctrl+c to stop)")
                sys.stdout.flush()

            health_every = float(args.health_every)
            if health_every > 0.0 and (now - last_health) >= health_every:
                last_health = now
                ok, wrn, fl = _health_report(
                    root=root,
                    procs=procs,
                    warn_secs=float(args.health_warn_secs),
                    tail_n=int(args.health_tail),
                )
                if args.fail_fast and fl > 0:
                    print("[orchestrator_v2] FAIL-FAST: health report has FAIL lanes. Exiting.")
                    stop = True

            if args.restart:
                for lab, lp in list(procs.items()):
                    rc = lp.popen.poll()
                    if rc is not None:
                        print(f"[orchestrator_v2] CRASH {lab} rc={rc} -> restarting")
                        sys.stdout.flush()
                        try:
                            try:
                                lp.stdout_f.close()  # type: ignore[attr-defined]
                            except Exception:
                                pass
                            try:
                                lp.stderr_f.close()  # type: ignore[attr-defined]
                            except Exception:
                                pass

                            procs[lab] = _spawn_lane(root, py, lab, logs_dir)
                            print(f"[orchestrator_v2] RESTARTED {lab} pid={procs[lab].popen.pid}")
                            sys.stdout.flush()
                        except Exception as e:
                            print(f"[orchestrator_v2] RESTART FAIL {lab}: {e}")
                            sys.stdout.flush()

            time.sleep(0.25)

    finally:
        # Always release locks for this PID on exit
        for lab in labels:
            _release_label_lock(root, lab)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
