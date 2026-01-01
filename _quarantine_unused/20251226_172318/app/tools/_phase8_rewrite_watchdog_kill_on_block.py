from __future__ import annotations

from pathlib import Path
import re

P = Path(r"app\ops\orchestrator_watchdog.py")
s = P.read_text(encoding="utf-8", errors="ignore")

pat = re.compile(
    r"(?ms)^(?P<ind>[ \t]*)# KILL_ON_BLOCK:.*?\n(?:(?!^(?P=ind)# Decide restart).*\n)*",
)

m = pat.search(s)
if not m:
    raise SystemExit("FATAL: could not find KILL_ON_BLOCK -> Decide restart block to rewrite")

ind = m.group("ind").replace("\t", "    ")

replacement = (
    f"{ind}# KILL_ON_BLOCK: blocked means STOP the process (best-effort)\n"
    f"{ind}try:\n"
    f"{ind}    _pid = int(pid) if pid is not None else None\n"
    f"{ind}except Exception:\n"
    f"{ind}    _pid = None\n"
    f"{ind}\n"
    f"{ind}if _pid:\n"
    f"{ind}    try:\n"
    f"{ind}        import subprocess\n"
    f"{ind}        subprocess.run([\"taskkill\", \"/PID\", str(_pid), \"/F\"], capture_output=True, text=True)\n"
    f"{ind}    except Exception:\n"
    f"{ind}        pass\n"
    f"{ind}\n"
    f"{ind}# Ensure our watchdog view reflects blocked as not alive\n"
    f"{ind}alive = False\n"
    f"{ind}pid = None\n"
    f"{ind}blocked.append(label)\n"
)

s2, n = pat.subn(replacement, s, count=1)
if n != 1:
    raise SystemExit(f"FATAL: rewrite failed (sub_count={n})")

# Normalize tabs globally to spaces (safe)
s2 = s2.replace("\t", "    ")

P.write_text(s2, encoding="utf-8")
print("OK: rewrote KILL_ON_BLOCK block with correct try/except alignment")
