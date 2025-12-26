from pathlib import Path
import re

p = Path(r"config\fleet_manifest.yaml")
lines = p.read_text(encoding="utf-8", errors="replace").splitlines(True)

# Fix: within fleet row blocks, force top-level keys to indent=2.
# Specifically: any line that is exactly 4 spaces then "enabled:" becomes 2 spaces.
fixed = 0
out = []

for ln in lines:
    if re.match(r"^ {4}enabled\s*:\s*(true|false)\s*$", ln.rstrip("\r\n")):
        out.append("  " + ln.lstrip(" "))
        fixed += 1
    else:
        out.append(ln)

p.write_text("".join(out), encoding="utf-8")
print(f"OK: fixed enabled indent (4->2) edits={fixed}")
