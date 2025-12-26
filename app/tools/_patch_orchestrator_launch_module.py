from pathlib import Path
import re

p = Path(r"app\ops\orchestrator_v1.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# Replace script-path execution with module execution.
# Old style: cmd = [sys.executable, str(ROOT / "app" / "bots" / "supervisor_ai_stack.py")]
# New style: cmd = [sys.executable, "-m", "app.bots.supervisor_ai_stack"]

# Do a resilient replacement: find the cmd assignment line and replace it.
pattern = r'cmd\s*=\s*\[sys\.executable,\s*str\(ROOT\s*/\s*"app"\s*/\s*"bots"\s*/\s*"supervisor_ai_stack\.py"\)\]\s*'
m = re.search(pattern, s)
if not m:
    # fallback: replace any line that mentions supervisor_ai_stack.py
    lines = s.splitlines(True)
    out = []
    changed = 0
    for ln in lines:
        if "supervisor_ai_stack.py" in ln and "cmd" in ln and "=" in ln:
            out.append('        cmd = [sys.executable, "-m", "app.bots.supervisor_ai_stack"]\n')
            changed += 1
        else:
            out.append(ln)
    if changed == 0:
        raise SystemExit("FATAL: Could not locate cmd=... supervisor_ai_stack.py line to patch.")
    s2 = "".join(out)
else:
    s2 = s[:m.start()] + 'cmd = [sys.executable, "-m", "app.bots.supervisor_ai_stack"]\n' + s[m.end():]

p.write_text(s2, encoding="utf-8")
print("OK: orchestrator_v1 now launches supervisor via python -m app.bots.supervisor_ai_stack")
