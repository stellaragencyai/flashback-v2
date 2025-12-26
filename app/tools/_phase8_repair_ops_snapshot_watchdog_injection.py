from __future__ import annotations

from pathlib import Path
import re

P = Path(r"app\ops\ops_snapshot_tick.py")
s = P.read_text(encoding="utf-8", errors="ignore").splitlines(True)

start_pat = re.compile(r'^\s*#\s*Phase8:\s*Merge watchdog restart/backoff/blocked state into accounts\s*$')
# End when we hit the next obvious block in that function: the "if isinstance(accounts, dict):" that returns supervisor info
end_pat = re.compile(r'^\s*if\s+isinstance\(accounts,\s*dict\)\s*:\s*$')

# 1) Remove the wrongly injected block (inside _extract_supervisor_from_heartbeats)
start = None
end = None
for i, ln in enumerate(s):
    if start is None and start_pat.search(ln):
        start = i
        continue
    if start is not None and end is None and end_pat.search(ln):
        end = i  # do NOT remove this line, it's legit
        break

if start is None or end is None or end <= start:
    raise SystemExit(f"FATAL: could not locate bad injected block to remove (start={start}, end={end}). File drifted.")

removed = s[start:end]
del s[start:end]

# 2) Insert correct merge block into main() after accounts = ...
text = "".join(s)

# find main() body anchor and an accounts assignment inside main
lines = text.splitlines(True)

def_line = None
for i, ln in enumerate(lines):
    if re.match(r'^\s*def\s+main\s*\(\)\s*->\s*int\s*:\s*$', ln):
        def_line = i
        break
if def_line is None:
    raise SystemExit("FATAL: could not find def main() -> int: in ops_snapshot_tick.py")

# find first "accounts =" AFTER def main line
anchor = None
for i in range(def_line+1, len(lines)):
    if re.match(r'^\s*accounts\s*=\s*', lines[i]):
        anchor = i
        break
if anchor is None:
    raise SystemExit("FATAL: could not find 'accounts =' inside main() to inject watchdog merge")

# guard: don't double insert into main
if "Merge watchdog restart/backoff/blocked state into accounts (main)" in text:
    raise SystemExit("FATAL: main() already contains watchdog merge block marker. Refusing to double-insert.")

inject = r'''
    # Phase8: Merge watchdog restart/backoff/blocked state into accounts (main)
    wd = _load_watchdog_state()
    wd_labels = (wd.get("labels") or {}) if isinstance(wd, dict) else {}
    if isinstance(accounts, dict) and isinstance(wd_labels, dict):
        for lbl, w in wd_labels.items():
            if not isinstance(lbl, str) or not isinstance(w, dict):
                continue
            if lbl not in accounts or not isinstance(accounts.get(lbl), dict):
                accounts[lbl] = {}
            # Normalize and attach watchdog status
            try:
                rc = int(w.get("restart_count") or 0)
            except Exception:
                rc = 0
            try:
                bo = float(w.get("backoff_sec") or 0.0)
            except Exception:
                bo = 0.0
            accounts[lbl]["watchdog"] = {
                "alive": bool(w.get("alive")) if "alive" in w else None,
                "pid": w.get("pid"),
                "restart_count": rc,
                "backoff_sec": bo,
                "next_restart_allowed_ts_ms": w.get("next_restart_allowed_ts_ms"),
                "blocked": bool(w.get("blocked")) if "blocked" in w else False,
                "blocked_reason": w.get("blocked_reason"),
                "last_checked_ts_ms": w.get("last_checked_ts_ms"),
                "last_restart_ts_ms": w.get("last_restart_ts_ms"),
                "source": "orchestrator_watchdog",
            }
'''.lstrip("\n")

lines.insert(anchor + 1, inject)

P.write_text("".join(lines), encoding="utf-8")
print(f"OK: removed bad watchdog injection lines={len(removed)}; inserted watchdog merge into main() after line={anchor+1}")
