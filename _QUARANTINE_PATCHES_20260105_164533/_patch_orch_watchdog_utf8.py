from pathlib import Path

p = Path(r"app\ops\orchestrator_watchdog.py")
s = p.read_text(encoding="utf-8", errors="ignore").splitlines()

out = []
patched = False

for line in s:
    out.append(line)
    if (not patched) and (line.strip() == "env = os.environ.copy()"):
        out.append('    env["PYTHONUTF8"] = "1"')
        out.append('    env["PYTHONIOENCODING"] = "utf-8"')
        out.append('    env["PYTHONLEGACYWINDOWSSTDIO"] = "0"')
        patched = True

p.write_text("\n".join(out) + "\n", encoding="utf-8")
print("PATCHED", patched, "FILE", str(p))
