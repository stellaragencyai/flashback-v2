from pathlib import Path
import re

p = Path(r"app\ops\orchestrator_v1.py")
lines = p.read_text(encoding="utf-8", errors="ignore").splitlines(True)

out = []
fixed = 0

for ln in lines:
    # Match any cmd = [...] line that references supervisor_ai_stack
    if "cmd" in ln and "supervisor_ai_stack" in ln:
        indent = ln[:len(ln) - len(ln.lstrip(" "))]
        out.append(f'{indent}cmd = [sys.executable, "-m", "app.bots.supervisor_ai_stack"]\n')
        fixed += 1
    else:
        out.append(ln)

if fixed == 0:
    raise SystemExit("FATAL: Could not find cmd line referencing supervisor_ai_stack to fix.")

p.write_text("".join(out), encoding="utf-8")
print(f"OK: fixed cmd launch line with correct indentation (edits={fixed})")
