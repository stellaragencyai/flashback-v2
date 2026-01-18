from pathlib import Path
import re

p = Path(r"app\core\ai_profile.py")
s = p.read_text(encoding="utf-8", errors="ignore")

start_pat = r"(?m)^# COMPAT SHIM \(2026-01-01\): expected by ai_action_router and legacy callers\s*$"
m = re.search(start_pat, s)
if not m:
    raise SystemExit("STOP: Could not find COMPAT SHIM marker in ai_profile.py")

head = s[:m.start()]
tail = s[m.start():]

# Only de-escape within the compat shim block.
# Fix literal \" -> " that broke docstrings and strings.
tail2 = tail.replace(r"\\\"", "\"")

# Also fix literal \' if it ever got introduced (unlikely, but cheap insurance)
tail2 = tail2.replace(r"\\'", "'")

p.write_text(head + tail2, encoding="utf-8")
print("OK: de-escaped compat shim block in ai_profile.py")
