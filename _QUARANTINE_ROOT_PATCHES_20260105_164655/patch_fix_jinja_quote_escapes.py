from pathlib import Path
import re

p = Path(r"app/dashboard/templates/dashboard.html")
s = p.read_text(encoding="utf-8", errors="ignore")

# Replace Jinja comparisons like: == \"REAL\"  or != \"XYZ\"
# into: == "REAL" etc.
pat = r'([=!]=)\s*\\\"([A-Za-z0-9_]+)\\\"'
s2, n = re.subn(pat, r'\1 "\2"', s)

if n == 0:
    print("PATCH_FAIL: No escaped-quote Jinja comparisons found")
    raise SystemExit(2)

p.write_text(s2, encoding="utf-8")
print(f"OK: Fixed {n} escaped-quote Jinja comparisons (\\\"WORD\\\" -> \"WORD\")")
