from pathlib import Path
import re

p = Path(r"app/dashboard/templates/dashboard.html")
s = p.read_text(encoding="utf-8", errors="ignore")

bad = r'r\.funds_source == \\\\"REAL\\\\"'
if bad not in s:
    print("WARN: Escaped REAL not found; template may already be fixed")
    raise SystemExit(0)

s2 = re.sub(
    r'r\.funds_source == \\\\"REAL\\\\"',
    'r.funds_source == "REAL"',
    s
)

p.write_text(s2, encoding="utf-8")
print("OK: Fixed Jinja escaped quote in FUNDS badge")
