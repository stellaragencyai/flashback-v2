from __future__ import annotations

from pathlib import Path
import re

P = Path(r"app\ops\ops_snapshot_tick.py")
s = P.read_text(encoding="utf-8", errors="ignore")

if "orchestrator_watchdog.json" in s and "watchdog" in s and "restart_count" in s:
    raise SystemExit("FATAL: ops_snapshot_tick.py already appears to include watchdog merge. Refusing to double-patch.")

# Ensure WATCHDOG path exists near STATE paths
if "WATCHDOG_STATE" not in s:
    m = re.search(r"^\s*STATE\s*=.*$\r?\n", s, flags=re.M)
    if not m:
        raise SystemExit("FATAL: could not find STATE = ... line in ops_snapshot_tick.py")
    insert_at = m.end()
    s = s[:insert_at] + 'WATCHDOG_STATE = STATE / "orchestrator_watchdog.json"\n' + s[insert_at:]

# Ensure loader helper exists
if "def _load_watchdog_state" not in s:
    m2 = re.search(r"^\s*def\s+", s, flags=re.M)
    if not m2:
        raise SystemExit("FATAL: could not find any function defs in ops_snapshot_tick.py")
    insert_at = m2.start()
    helper = r'''
def _load_watchdog_state() -> dict:
    try:
        if not WATCHDOG_STATE.exists():
            return {}
        return json.loads(WATCHDOG_STATE.read_text(encoding="utf-8", errors="ignore") or "{}")
    except Exception:
        return {}
'''.lstrip("\n")
    s = s[:insert_at] + helper + "\n" + s[insert_at:]

lines = s.splitlines(True)

# Find anchor: accounts = ...
anchor = None
for i, ln in enumerate(lines):
    if re.search(r"^\s*accounts\s*=\s*", ln):
        anchor = i
        break
if anchor is None:
    raise SystemExit("FATAL: could not find 'accounts =' line in ops_snapshot_tick.py to anchor watchdog merge")

inject = r'''
    # Phase8: Merge watchdog restart/backoff/blocked state into accounts
    wd = _load_watchdog_state()
    wd_labels = (wd.get("labels") or {}) if isinstance(wd, dict) else {}
    if isinstance(accounts, dict) and isinstance(wd_labels, dict):
        for lbl, w in wd_labels.items():
            if not isinstance(lbl, str) or not isinstance(w, dict):
                continue
            if lbl not in accounts or not isinstance(accounts.get(lbl), dict):
                accounts[lbl] = {}
            # Normalize and attach watchdog status
            accounts[lbl]["watchdog"] = {
                "alive": bool(w.get("alive")),
                "pid": w.get("pid"),
                "restart_count": int(w.get("restart_count") or 0),
                "backoff_sec": float(w.get("backoff_sec") or 0.0),
                "next_restart_allowed_ts_ms": w.get("next_restart_allowed_ts_ms"),
                "blocked": bool(w.get("blocked")) if "blocked" in w else False,
                "blocked_reason": w.get("blocked_reason"),
                "last_checked_ts_ms": w.get("last_checked_ts_ms"),
                "last_restart_ts_ms": w.get("last_restart_ts_ms"),
            }
'''.lstrip("\n")

lines.insert(anchor + 1, inject)

P.write_text("".join(lines), encoding="utf-8")
print("OK: patched ops_snapshot_tick.py to merge orchestrator_watchdog -> accounts[lbl].watchdog")
