from __future__ import annotations

from pathlib import Path
import re

P = Path(r"app\ops\ops_snapshot_tick.py")
s = P.read_text(encoding="utf-8", errors="ignore")

# 0) Make sure json is imported
if "import json" not in s:
    raise SystemExit("FATAL: ops_snapshot_tick.py does not import json (unexpected).")

# 1) Ensure ORCH_STATE exists
if "ORCH_STATE" not in s:
    m = re.search(r"^\s*STATE\s*=.*$\r?\n", s, flags=re.M)
    if not m:
        raise SystemExit("FATAL: could not find STATE = ... line to place ORCH_STATE")
    s = s[:m.end()] + 'ORCH_STATE = STATE / "orchestrator_state.json"\n' + s[m.end():]

# 2) Ensure helper exists
if "def _load_orchestrator_state" not in s:
    m2 = re.search(r"^\s*def\s+", s, flags=re.M)
    if not m2:
        raise SystemExit("FATAL: could not find function boundary to insert helper")
    helper = r'''
def _load_orchestrator_state() -> dict:
    try:
        if not ORCH_STATE.exists():
            return {}
        return json.loads(ORCH_STATE.read_text(encoding="utf-8", errors="ignore") or "{}")
    except Exception:
        return {}
'''.lstrip("\n")
    s = s[:m2.start()] + helper + "\n" + s[m2.start():]

# 3) Remove any prior injected merge blocks (so we don't double-merge or get overwritten later)
before_len = len(s.splitlines())
pattern = r"(?ms)^\s*# Phase8: Merge orchestrator_state into accounts as supervisor_ai_stack truth.*?^\s*accounts\[lbl\]\[\"supervisor_ai_stack\"\]\s*=\s*\{.*?^\s*\}\s*\r?\n"
s2, n = re.subn(pattern, "", s)
s = s2
after_len = len(s.splitlines())

# 4) Insert FINAL merge block right before ops_snapshot write.
# Anchor on the line that writes ops_snapshot.json. We search for write_text + ops_snapshot.json, else generic OUT_PATH.write_text.
anchor = re.search(r"(?m)^\s*.*write_text\(\s*.*ops_snapshot\.json.*\)\s*$", s)
if not anchor:
    anchor = re.search(r"(?m)^\s*.*write_text\(\s*.*\)\s*$", s)  # fallback but still end-ish

if not anchor:
    raise SystemExit("FATAL: could not find a write_text(...) line to anchor final merge injection")

# We also need an "accounts" variable in scope. If the file doesn't use 'accounts', bail with guidance.
if re.search(r"(?m)^\s*accounts\s*=", s) is None and '"accounts"' not in s:
    raise SystemExit("FATAL: could not find accounts variable usage in ops_snapshot_tick.py; file differs from expected.")

final_merge = r'''
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
'''.lstrip("\n")

# Insert just BEFORE the anchor line
pos = anchor.start()
s = s[:pos] + final_merge + s[pos:]

P.write_text(s, encoding="utf-8")
print(f"OK: ops_snapshot_tick FINAL merge injected. removed_blocks={n} line_delta={before_len-after_len}")
