from __future__ import annotations
from pathlib import Path
import re

P = Path(r"app\ops\orchestrator_v1.py")
s = P.read_text(encoding="utf-8", errors="ignore")

# We want to inject these near: env = os.environ.copy()
needle = "env = os.environ.copy()"
if needle not in s:
    raise SystemExit("FATAL: could not find 'env = os.environ.copy()' in orchestrator_v1.py")

if 'env["PYTHONUTF8"]' in s or "PYTHONIOENCODING" in s:
    print("OK: orchestrator_v1 already sets PYTHONUTF8/PYTHONIOENCODING")
    raise SystemExit(0)

inj = needle + "\n        env[\"PYTHONUTF8\"] = \"1\"\n        env[\"PYTHONIOENCODING\"] = \"utf-8\""
s2 = s.replace(needle, inj, 1)

P.write_text(s2, encoding="utf-8")
print("OK: patched orchestrator_v1.py to force UTF-8 for child supervisors")
