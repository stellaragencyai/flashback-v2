from __future__ import annotations
from pathlib import Path

P = Path(r"app\ops\fleet_up.ps1")
s = P.read_text(encoding="utf-8", errors="ignore")

if "effective_should_run" in s:
    print("OK: fleet_up.ps1 already prints effective_should_run")
    raise SystemExit(0)

needle = "print(lbl,\n          'should_run=', v.get('should_run'),"
if needle not in s:
    raise SystemExit("FATAL: could not find the print(lbl,... should_run=...) anchor in fleet_up.ps1")

replacement = "print(lbl,\n          'should_run=', v.get('should_run'),\n          'effective_should_run=', v.get('effective_should_run'),"
s2 = s.replace(needle, replacement, 1)

P.write_text(s2, encoding="utf-8")
print("OK: patched fleet_up.ps1 to print effective_should_run")
