from __future__ import annotations

from pathlib import Path
import re

P = Path(r"app\ops\fleet_snapshot_tick.py")
s = P.read_text(encoding="utf-8", errors="ignore").splitlines(True)

# Insert effective_should_run into subs[label] dict near "should_run"
out = []
inserted = 0

for ln in s:
    out.append(ln)
    # anchor: the line that writes "should_run": ...
    if re.search(r'^\s*"should_run"\s*:\s*bool\(', ln) or re.search(r'^\s*"should_run"\s*:\s*bool\(', ln.replace(" ", "")):
        # Avoid double insert
        if any("effective_should_run" in x for x in out[-8:]):
            continue
        # Insert after should_run line
        indent = re.match(r'^(\s*)', ln).group(1)
        out.append(f'{indent}"effective_should_run": bool((enabled and enable_ai_stack and should_run_mode_ok) and (not blocked)),\n')
        inserted += 1

P.write_text("".join(out), encoding="utf-8")
print(f"OK: patched fleet_snapshot_tick.py effective_should_run inserted={inserted}")
