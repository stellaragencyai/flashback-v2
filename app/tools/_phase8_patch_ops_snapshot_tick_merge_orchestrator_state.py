from __future__ import annotations

from pathlib import Path
import re

P = Path(r"app\ops\ops_snapshot_tick.py")
s = P.read_text(encoding="utf-8", errors="ignore")

# Hard requirements
need = ["orchestrator_state.json", "accounts", "ops_snapshot.json"]
missing = [x for x in need if x not in s]
if missing:
    print("WARN: expected strings not found (still patching):", missing)

# 1) Ensure imports include os/json (most likely already there)
if "import os" not in s:
    s = s.replace("import json", "import json\nimport os", 1)

# 2) Insert ORCH_STATE path near other STATE paths
if "ORCH_STATE" not in s:
    # Insert after STATE definition if possible
    m = re.search(r"^\s*STATE\s*=.*$\r?\n", s, flags=re.M)
    if not m:
        raise SystemExit("FATAL: could not find STATE = ... line in ops_snapshot_tick.py")
    insert_at = m.end()
    s = s[:insert_at] + 'ORCH_STATE = STATE / "orchestrator_state.json"\n' + s[insert_at:]

# 3) Add helper to load orchestrator_state.json (best-effort)
if "def _load_orchestrator_state" not in s:
    m = re.search(r"^\s*def\s+_load_.*\(\)\s*->\s*dict", s, flags=re.M)
    # If no helpers exist, inject after imports/paths block by finding first def
    m2 = re.search(r"^\s*def\s+", s, flags=re.M)
    if not m2:
        raise SystemExit("FATAL: could not find any function defs in ops_snapshot_tick.py")
    insert_at = m2.start()

    helper = r'''
def _load_orchestrator_state() -> dict:
    try:
        if not ORCH_STATE.exists():
            return {}
        return json.loads(ORCH_STATE.read_text(encoding="utf-8", errors="ignore") or "{}")
    except Exception:
        return {}
'''.lstrip("\n")
    s = s[:insert_at] + helper + "\n" + s[insert_at:]

# 4) Inject merge logic into main() after accounts dict is created/available
# We’ll anchor on the line that creates the top-level output dict, OR on accounts assignment.
anchor = None
lines = s.splitlines(True)

# Find the first obvious "accounts =" line in main
for i,ln in enumerate(lines):
    if re.search(r"^\s*accounts\s*=\s*", ln):
        anchor = i
        break

if anchor is None:
    # fallback: find where out = { ... "accounts": accounts ... }
    for i,ln in enumerate(lines):
        if re.search(r"^\s*out\s*=\s*{", ln):
            anchor = i
            break

if anchor is None:
    raise SystemExit("FATAL: could not find an anchor in ops_snapshot_tick.py to inject merge logic")

# Check if we already merged
if "orchestrator_state" in s and "supervisor_ai_stack" in s and "source\": \"orchestrator_state" in s:
    raise SystemExit("FATAL: ops_snapshot_tick already appears patched with orchestrator merge (won't double patch)")

inject = r'''
    # Phase8: Merge orchestrator_state into accounts as supervisor_ai_stack truth
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
            # Normalize PID to int or None
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
'''.lstrip("\n")

# Insert inject block immediately AFTER accounts = ... line (best), else after out = { (fallback)
insert_at = anchor + 1
lines.insert(insert_at, inject)

P.write_text("".join(lines), encoding="utf-8")
print("OK: patched ops_snapshot_tick.py to merge orchestrator_state -> supervisor_ai_stack {ok,pid}")
