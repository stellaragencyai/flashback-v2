from __future__ import annotations

from pathlib import Path
import re

P = Path(r"app\ops\fleet_snapshot_tick.py")
s = P.read_text(encoding="utf-8", errors="ignore")

if '"restart_count"' in s and '"blocked"' in s and "watchdog" in s:
    raise SystemExit("FATAL: fleet_snapshot_tick.py already appears to include watchdog fields. Refusing to double-patch.")

lines = s.splitlines(True)

# Find where sup/alive/pid are computed and subs[label] dict is built.
# We'll inject AFTER pid is assigned and BEFORE subs[label] = {...}
idx_pid = None
idx_subs_assign = None

for i, ln in enumerate(lines):
    if re.search(r"^\s*pid\s*=\s*sup\.get\(", ln):
        idx_pid = i
    if re.search(r"^\s*subs\[label\]\s*=\s*{", ln):
        idx_subs_assign = i
        break

if idx_subs_assign is None:
    raise SystemExit("FATAL: could not find subs[label] assignment in fleet_snapshot_tick.py")

# We need ops_accounts already loaded (it is). We'll read watchdog from acc.get('watchdog')
inject = r'''
        wd = acc.get("watchdog") if isinstance(acc.get("watchdog"), dict) else {}
        restart_count = int(wd.get("restart_count") or 0) if isinstance(wd, dict) else 0
        blocked = bool(wd.get("blocked")) if isinstance(wd, dict) and "blocked" in wd else False
        blocked_reason = wd.get("blocked_reason") if isinstance(wd, dict) else None
        backoff_sec = float(wd.get("backoff_sec") or 0.0) if isinstance(wd, dict) else 0.0
        next_restart_allowed_ts_ms = wd.get("next_restart_allowed_ts_ms") if isinstance(wd, dict) else None
'''.lstrip("\n")

# Insert the inject block right before subs[label] = { ... }
lines.insert(idx_subs_assign, inject)

# Now add fields into subs[label] dict, right before closing "}"
# We'll find the block end by scanning forward for the closing brace at same indent.
indent = len(lines[idx_subs_assign+1]) - len(lines[idx_subs_assign+1].lstrip(" "))
end = None
for j in range(idx_subs_assign, min(len(lines), idx_subs_assign + 80)):
    if re.search(r"^\s*}\s*,?\s*$", lines[j]):
        end = j
        break
if end is None:
    raise SystemExit("FATAL: could not find end of subs[label] dict block to inject fields")

field_lines = [
    '            "restart_count": restart_count,\n',
    '            "blocked": blocked,\n',
    '            "blocked_reason": blocked_reason,\n',
    '            "backoff_sec": backoff_sec,\n',
    '            "next_restart_allowed_ts_ms": next_restart_allowed_ts_ms,\n',
]

# Insert fields just before the closing brace line
lines[end:end] = field_lines

P.write_text("".join(lines), encoding="utf-8")
print("OK: patched fleet_snapshot_tick.py to surface watchdog restart_count/blocked/backoff into fleet_snapshot subs")
