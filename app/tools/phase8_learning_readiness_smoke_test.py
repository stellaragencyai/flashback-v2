from __future__ import annotations

import shutil
import subprocess
import time
from pathlib import Path

OBSERVED = Path("signals/observed.jsonl")

def _run(cmd: str) -> int:
    print(f"\n>>> {cmd}")
    r = subprocess.run(cmd, shell=True)
    return int(r.returncode)

def _run_signal_engine_for_seconds(seconds: int = 25) -> int:
    cmds = [
        "python -m app.bots.signal_engine",
        r"python .\app\bots\signal_engine.py",
    ]
    last_rc = 1
    for cmd in cmds:
        print(f"\n>>> {cmd}  (timeout={seconds}s)")
        try:
            p = subprocess.Popen(cmd, shell=True)
        except Exception as e:
            print("WARN: failed to start:", e)
            continue

        try:
            p.wait(timeout=seconds)
            last_rc = int(p.returncode or 0)
            return last_rc
        except subprocess.TimeoutExpired:
            print("INFO: timeout reached; terminating signal engine")
            p.terminate()
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                print("INFO: terminate did not exit; killing signal engine")
                p.kill()
                p.wait(timeout=3)
            return 0
    return last_rc

def main() -> int:
    print("=== PHASE 8 SMOKE TEST (Learning Readiness) v3 ===")

    if OBSERVED.exists():
        ts = int(time.time())
        bak = OBSERVED.with_name(f"observed.jsonl.bak_{ts}")
        shutil.copyfile(OBSERVED, bak)
        print("backup_ok=", str(bak))
    else:
        OBSERVED.parent.mkdir(parents=True, exist_ok=True)

    OBSERVED.write_text("", encoding="utf-8")
    print("cleared_ok=", str(OBSERVED))

    rc = _run_signal_engine_for_seconds(seconds=25)
    if rc != 0:
        print("WARN: bounded signal_engine run returned non-zero; continuing to verifier anyway.")
    time.sleep(0.5)

    if not OBSERVED.exists():
        print("FAIL: observed.jsonl missing after run")
        return 1
    if OBSERVED.stat().st_size == 0:
        print("FAIL: observed.jsonl empty after bounded run")
        return 1

    rc = _run("python -m app.tools.verify_signal_engine_observed_contract")
    if rc != 0:
        print("FAIL: observed contract verifier failed")
        return 1

    rc = _run("python -m app.tools.ingest_observed_to_ai_events")
    if rc != 0:
        print("FAIL: observed intake failed")
        return 1

    rc = _run("python -m app.tools.verify_ai_decisions_tradeid_determinism")
    if rc != 0:
        print("FAIL: ai_decisions determinism failed")
        return 1

    rc = _run("python -m app.tools.verify_ai_sampling_guardrails")
    if rc != 0:
        print("FAIL: sampling guardrails failed")
        return 1

    print("\nPASS: Phase 8 Learning Readiness Smoke Test (v3)")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
