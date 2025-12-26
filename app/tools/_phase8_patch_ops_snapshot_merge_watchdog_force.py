from __future__ import annotations

from pathlib import Path
import re

P = Path(r"app\ops\ops_snapshot_tick.py")
s = P.read_text(encoding="utf-8", errors="ignore")

# Guard: don't double-patch
if re.search(r'accounts\[lbl\]\["watchdog"\]\s*=', s) or "Merge orchestrator_watchdog into accounts" in s:
    raise SystemExit("FATAL: ops_snapshot_tick.py already contains watchdog merge logic. Refusing to double-patch.")

# Ensure ORCH_WATCHDOG path constant exists
if "ORCH_WATCHDOG" not in s:
    m = re.search(r"^\s*ORCH_STATE\s*=.*$\r?\n", s, flags=re.M)
    if not m:
        # If ORCH_STATE doesn't exist, anchor on STATE definition
        m2 = re.search(r"^\s*STATE\s*=.*$\r?\n", s, flags=re.M)
        if not m2:
            raise SystemExit("FATAL: could not find STATE= or ORCH_STATE= anchor in ops_snapshot_tick.py")
        insert_at = m2.end()
        s = s[:insert_at] + 'ORCH_STATE = STATE / "orchestrator_state.json"\n' + s[insert_at:]
        m = re.search(r"^\s*ORCH_STATE\s*=.*$\r?\n", s, flags=re.M)
        if not m:
            raise SystemExit("FATAL: failed to inject ORCH_STATE")

    insert_at = m.end()
    s = s[:insert_at] + 'ORCH_WATCHDOG = STATE / "orchestrator_watchdog.json"\n' + s[insert_at:]

# Ensure helper exists
if "def _load_orchestrator_watchdog" not in s:
    m = re.search(r"^\s*def\s+_load_orchestrator_state\s*\(\)\s*->\s*dict\s*:\s*\r?\n", s, flags=re.M)
    if not m:
        # Insert before first def as fallback
        m2 = re.search(r"^\s*def\s+", s, flags=re.M)
        if not m2:
            raise SystemExit("FATAL: could not find any def anchor in ops_snapshot_tick.py")
        insert_at = m2.start()
    else:
        # Insert immediately after _load_orchestrator_state def block header line by placing before next def
        m_next = re.search(r"^\s*def\s+", s[m.end():], flags=re.M)
        insert_at = (m.end() + (m_next.start() if m_next else 0))

    helper = r'''
def _load_orchestrator_watchdog() -> dict:
    try:
        if not ORCH_WATCHDOG.exists():
            return {}
        return json.loads(ORCH_WATCHDOG.read_text(encoding="utf-8", errors="ignore") or "{}")
    except Exception:
        return {}
'''.lstrip("\n")
    s = s[:insert_at] + helper + "\n" + s[insert_at:]

# Inject merge block inside main() AFTER accounts is defined
lines = s.splitlines(True)
anchor = None
for i,ln in enumerate(lines):
    if re.search(r"^\s*accounts\s*=\s*\(", ln) or re.search(r"^\s*accounts\s*=\s*{", ln) or re.search(r"^\s*accounts\s*=\s*", ln):
        anchor = i
        break
if anchor is None:
    raise SystemExit("FATAL: could not find 'accounts =' anchor in main() to inject watchdog merge")

inject = r'''
    # Phase8: Merge orchestrator_watchdog into accounts[lbl]["watchdog"]
    wd = _load_orchestrator_watchdog()
    wd_labels = (wd.get("labels") or {}) if isinstance(wd, dict) else {}
    if isinstance(accounts, dict) and isinstance(wd_labels, dict):
        for lbl, winfo in wd_labels.items():
            if not isinstance(lbl, str) or not isinstance(winfo, dict):
                continue
            if lbl not in accounts or not isinstance(accounts.get(lbl), dict):
                accounts[lbl] = {}
            # Normalize fields
            try:
                rc = int(winfo.get("restart_count") or 0)
            except Exception:
                rc = 0
            try:
                bo = float(winfo.get("backoff_sec") or 0.0)
            except Exception:
                bo = 0.0
            blocked = bool(winfo.get("blocked")) if "blocked" in winfo else False
            accounts[lbl]["watchdog"] = {
                "restart_count": rc,
                "backoff_sec": bo,
                "blocked": blocked,
                "blocked_reason": winfo.get("blocked_reason"),
                "alive": bool(winfo.get("alive")) if "alive" in winfo else None,
                "pid": winfo.get("pid"),
                "source": "orchestrator_watchdog",
            }
'''.lstrip("\n")

# Insert right after the accounts assignment line
lines.insert(anchor + 1, inject)

P.write_text("".join(lines), encoding="utf-8")
print("OK: patched ops_snapshot_tick.py to merge orchestrator_watchdog -> accounts[lbl].watchdog")
