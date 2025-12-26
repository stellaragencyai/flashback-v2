from __future__ import annotations
from pathlib import Path
import re

P=Path(r"app\ops\fleet_up.ps1")
s=P.read_text(encoding="utf-8", errors="ignore")

if "$script:OrchStarted" in s:
    print("OK: fleet_up.ps1 already loop-safe for orchestrator start")
    raise SystemExit(0)

# Add a script-scoped flag near the top after ErrorActionPreference
needle='$ErrorActionPreference = "Stop"'
if needle not in s:
    raise SystemExit("FATAL: could not find ErrorActionPreference anchor in fleet_up.ps1")

s=s.replace(needle, needle + "\n\n$script:OrchStarted = $false\n", 1)

# Replace the orchestrator start block inside OnePass()
# Original:
# if (-not $NoStart) { RunPy "app.ops.orchestrator_v1" }
# We change to: start only once per loop session
pat=re.compile(r'(?ms)\s*if\s*\(\s*-not\s+\$NoStart\s*\)\s*\{\s*RunPy\s+"app\.ops\.orchestrator_v1"\s*\}\s*')
m=pat.search(s)
if not m:
    raise SystemExit("FATAL: could not find orchestrator start block to patch")

replacement=r'''
  if (-not $NoStart) {
    if (-not $script:OrchStarted) {
      RunPy "app.ops.orchestrator_v1"
      $script:OrchStarted = $true
    }
  }
'''
s=pat.sub(replacement, s, count=1)

P.write_text(s, encoding="utf-8")
print("OK: patched fleet_up.ps1 to start orchestrator once per loop session")
