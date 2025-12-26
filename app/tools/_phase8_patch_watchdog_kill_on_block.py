from __future__ import annotations
from pathlib import Path
import re

P = Path(r"app\ops\orchestrator_watchdog.py")
s = P.read_text(encoding="utf-8", errors="ignore")

if "KILL_ON_BLOCK" in s:
    print("OK: orchestrator_watchdog.py already has kill-on-block logic")
    raise SystemExit(0)

# Anchor on where we set blocked_reason="too_many_restarts" (or similar)
m = re.search(r'blocked_reason\s*=\s*["\']too_many_restarts["\']', s)
if not m:
    raise SystemExit("FATAL: could not find blocked_reason='too_many_restarts' anchor in orchestrator_watchdog.py")

# Insert a block shortly AFTER that line: try taskkill pid; mark alive False locally
lines = s.splitlines(True)
idx = None
for i,ln in enumerate(lines):
    if 'blocked_reason' in ln and 'too_many_restarts' in ln:
        idx = i
        break

if idx is None:
    raise SystemExit("FATAL: failed to locate insertion line index")

inject = r'''
        # KILL_ON_BLOCK: blocked means STOP the process (best-effort)
        try:
            _pid = int(pid) if pid is not None else None
        except Exception:
            _pid = None

        if _pid:
            try:
                import subprocess
                subprocess.run(["taskkill", "/PID", str(_pid), "/F"], capture_output=True, text=True)
            except Exception:
                pass

        # Ensure our watchdog view reflects blocked as not alive
        alive = False
        pid = None
'''.lstrip("\n")

# Insert a little after the blocked_reason assignment (next line)
lines.insert(idx + 1, inject)

P.write_text("".join(lines), encoding="utf-8")
print("OK: patched orchestrator_watchdog.py to taskkill blocked processes (KILL_ON_BLOCK)")
