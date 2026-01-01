from __future__ import annotations
from pathlib import Path
import re

P = Path(r"app\tools\ws_health_check.py")
s = P.read_text(encoding="utf-8", errors="ignore")
s = s.lstrip("\ufeff")

# Replace the PASS line that includes a unicode checkmark with plain ASCII
s2, n = re.subn(r'print\("\\nPASS .*?"\)', 'print("\\nPASS")', s)

# Also catch literal checkmark if present in source
s2 = s2.replace("PASS ✅", "PASS")

P.write_text(s2, encoding="utf-8")
print("PATCH_OK:", str(P), "replacements:", n)
