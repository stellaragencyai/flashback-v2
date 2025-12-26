from __future__ import annotations

from pathlib import Path
import json
import os
import subprocess
import sys
import time
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[2]
STATE = ROOT / "state"
LOGDIR = STATE / "orchestrator_logs"
MANIFEST = ROOT / "config" / "fleet_manifest.yaml"
OUT = STATE / "orchestrator_state.json"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        r = subprocess.run(["tasklist", "/FI", f"PID eq {pid}"], capture_output=True, text=True)
        return str(pid) in (r.stdout or "")
    except Exception:
        return False


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


def _should_run(row: dict[str, Any]) -> bool:
    enabled = bool(row.get("enabled", True))
    enable_ai_stack = bool(row.get("enable_ai_stack", True))
    mode = str(row.get("automation_mode") or "").strip().upper()
    mode_ok = mode not in ("", "OFF", "DISABLED", "NONE")
    # NOTE: "main" might be enabled but manual; if enable_ai_stack is false it won't start.
    return bool(enabled and enable_ai_stack and mode_ok)


def main() -> int:
    STATE.mkdir(parents=True, exist_ok=True)
    LOGDIR.mkdir(parents=True, exist_ok=True)

    rows = _load_manifest_rows()

    only = (os.getenv("ORCH_ONLY_LABELS") or "").strip()
    only_set = set([x.strip() for x in only.split(",") if x.strip()]) if only else set()

    procs: Dict[str, Any] = {}
    started: List[str] = []
    skipped: List[str] = []

    for r in rows:
        label = str(r.get("account_label") or "").strip()
        if not label:
            continue

        if only_set and label not in only_set:
            skipped.append(f"{label}: not in ORCH_ONLY_LABELS")
            continue

        if not _should_run(r):
            skipped.append(f"{label}: should_run=false by manifest")
            continue

        env = os.environ.copy()
        env["ACCOUNT_LABEL"] = label

        # Long-term fix: ensure UTF-8 so emoji/log lines don't crash on Windows cp1252
        env["PYTHONUTF8"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"

        # Long-term fix: run as module so package imports work (app.tools, etc.)
        cmd = [sys.executable, "-m", "app.bots.supervisor_ai_stack"]

        ts = _now_ms()
        out_log = LOGDIR / f"{label}.stdout.log"
        err_log = LOGDIR / f"{label}.stderr.log"

        try:
            with out_log.open("ab") as fo, err_log.open("ab") as fe:
                header = f"\n\n=== START {label} ts_ms={ts} cmd={cmd} ===\n"
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

            time.sleep(0.35)  # let it crash loudly if it wants
            alive = _pid_alive(int(p.pid))

            procs[label] = {
                "pid": int(p.pid),
                "cmd": cmd,
                "started_ts_ms": ts,
                "alive": bool(alive),
                "stdout_log": str(out_log),
                "stderr_log": str(err_log),
            }
            started.append(label)

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

    out = {
        "ts_ms": _now_ms(),
        "root": str(ROOT),
        "manifest": str(MANIFEST),
        "only_labels": sorted(list(only_set)),
        "started": started,
        "skipped": skipped,
        "procs": procs,
    }
    OUT.write_text(json.dumps(out, indent=2), encoding="utf-8")

    print(f"OK: orchestrator_v1 started={len(started)} wrote={OUT}")
    print(f"OK: logs_dir={LOGDIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
