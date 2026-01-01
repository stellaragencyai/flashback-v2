from __future__ import annotations

from pathlib import Path
import re

p = Path(r"app\ai\ai_events_spine.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# Try to find a set/list of allowed setup types.
# Common patterns: ALLOWED_SETUP_TYPES = {...} or _ALLOWED_SETUP_TYPES = {...}
pat = re.compile(r'(?ms)^(?P<name>[_A-Z0-9]*ALLOWED[_A-Z0-9]*SETUP[_A-Z0-9]*TYPES)\s*=\s*\{(?P<body>.*?)\}\s*$', re.MULTILINE)
m = pat.search(s)
if not m:
    # Try list syntax: = [ ... ]
    pat2 = re.compile(r'(?ms)^(?P<name>[_A-Z0-9]*ALLOWED[_A-Z0-9]*SETUP[_A-Z0-9]*TYPES)\s*=\s*\[(?P<body>.*?)\]\s*$', re.MULTILINE)
    m = pat2.search(s)

if not m:
    raise SystemExit("FAIL: could not find ALLOWED_SETUP_TYPES in ai_events_spine.py (pattern mismatch).")

name = m.group("name")
body = m.group("body")

if "trend_pullback" in body:
    print("OK: ai_events_spine.py already allows trend_pullback")
    raise SystemExit(0)

# Insert 'trend_pullback' near existing pullback entries if possible
if "pullback" in body:
    body2 = body.replace("pullback", "pullback,\n    \"trend_pullback\"")
else:
    body2 = body + '\n    "trend_pullback",\n'

# Rebuild keeping original bracket type
full = m.group(0)
if "{" in full and "}" in full:
    replaced = f"{name} = {{{body2}}}"
else:
    replaced = f"{name} = [{body2}]"

s2 = s[:m.start()] + replaced + s[m.end():]
p.write_text(s2, encoding="utf-8")
print("OK: patched ai_events_spine.py (allowed trend_pullback)")
