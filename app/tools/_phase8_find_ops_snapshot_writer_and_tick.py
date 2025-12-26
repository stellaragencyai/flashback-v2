from __future__ import annotations

from pathlib import Path
import json
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
APP = ROOT / "app"
STATE = ROOT / "state"

TARGETS = ["flashback01", "flashback02", "flashback07"]

def find_ops_snapshot_writers() -> list[str]:
    hits = []
    for p in APP.rglob("*.py"):
        try:
            s = p.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        if "ops_snapshot.json" in s or "ops_snapshot" in s and "STATE" in s:
            hits.append(str(p))
    return sorted(set(hits))

def try_run(mod: str) -> int:
    print(f"\n=== TRY RUN: python -m {mod} ===")
    try:
        r = subprocess.run([sys.executable, "-m", mod], cwd=str(ROOT), capture_output=True, text=True)
        print("rc=", r.returncode)
        if r.stdout.strip():
            print("--- stdout ---")
            print(r.stdout[:4000])
        if r.stderr.strip():
            print("--- stderr ---")
            print(r.stderr[:4000])
        return r.returncode
    except Exception as e:
        print("EXC:", repr(e))
        return 999

def load_ops() -> dict:
    p = STATE / "ops_snapshot.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8", errors="ignore") or "{}")
    except Exception:
        return {}

def report_ops_accounts(d: dict) -> None:
    accounts = (d.get("accounts") or {}) if isinstance(d, dict) else {}
    print("\n=== OPS_SNAPSHOT ACCOUNT COVERAGE ===")
    print("has_accounts_dict=", isinstance(accounts, dict), "accounts_len=", (len(accounts) if isinstance(accounts, dict) else None))
    if not isinstance(accounts, dict):
        return
    for lbl in TARGETS:
        a = accounts.get(lbl)
        print(lbl, "present=", (lbl in accounts), "value_type=", type(a).__name__ if a is not None else None)

def main() -> int:
    print("ROOT=", ROOT)
    print("STATE=", STATE)

    writers = find_ops_snapshot_writers()
    print("\n=== CANDIDATE FILES (mention ops_snapshot) ===")
    for x in writers[:120]:
        print(x)
    if len(writers) > 120:
        print(f"... +{len(writers)-120} more")

    # Before
    report_ops_accounts(load_ops())

    # Try common module names (long-term fix: an explicit tick module should exist)
    candidates = [
        "app.ops.ops_snapshot_tick",
        "app.ops.ops_snapshot",
        "app.ops.snapshot_ops",
        "app.ops.ops_tick",
        "app.ops.ops_status_tick",
    ]
    for mod in candidates:
        rc = try_run(mod)
        # After each attempt, check if accounts appeared
        report_ops_accounts(load_ops())
        if rc == 0:
            # if accounts are now present for targets, stop
            d = load_ops()
            acc = (d.get("accounts") or {}) if isinstance(d, dict) else {}
            if isinstance(acc, dict) and all(lbl in acc for lbl in TARGETS):
                print("\nOK: ops_snapshot now includes target accounts ✅")
                return 0

    print("\nFATAL: Could not auto-run an ops_snapshot tick module, or it still doesn't emit target accounts.")
    print("Next step: we wire ops_snapshot to manifest explicitly (tooling script will be generated next).")
    return 2

if __name__ == "__main__":
    raise SystemExit(main())
