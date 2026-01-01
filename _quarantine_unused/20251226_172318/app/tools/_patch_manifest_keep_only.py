from pathlib import Path
import re

p = Path(r"config\fleet_manifest.yaml")
s = p.read_text(encoding="utf-8", errors="ignore").splitlines(True)

# Keep these enabled, disable all other flashbackXX rows
KEEP = {"flashback01","flashback02","flashback07"}  # edit if needed

out = []
cur_label = None

for line in s:
    m = re.match(r"^\s*-\s*account_label:\s*['\"]?([^'\"\n]+)['\"]?\s*$", line)
    if m:
        cur_label = m.group(1).strip()
        out.append(line)
        continue

    if cur_label and cur_label.startswith("flashback") and cur_label not in KEEP:
        if re.match(r"^\s*enabled:\s*", line):
            out.append(re.sub(r"^\s*enabled:\s*.*$", "    enabled: false\n", line))
            continue

    out.append(line)

p.write_text("".join(out), encoding="utf-8")
print("OK: patched fleet_manifest.yaml (disabled non-KEEP flashback labels)")
