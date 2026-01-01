from __future__ import annotations

from pathlib import Path
import re

P = Path(r"app\ops\fleet_snapshot_tick.py")
s = P.read_text(encoding="utf-8", errors="ignore")
lines = s.splitlines()

needles = [
    "fleet_snapshot",
    "FLEET_SNAPSHOT",
    "write_text",
    "write_bytes",
    "open(",
    "json.dump",
    "json.dumps",
    "_write",
    "_atomic",
    "STATE /",
    "Path(",
]

print("FILE=", str(P.resolve()))
print("LINES=", len(lines))

hits = []
for i, l in enumerate(lines, start=1):
    if any(n in l for n in needles):
        if ("fleet" in l.lower()) or ("snapshot" in l.lower()) or ("write" in l.lower()) or ("dump" in l.lower()):
            hits.append((i, l))

print("\n=== HITS (potential writer / anchors) ===")
for i, l in hits[:220]:
    print(f"{i:04d}: {l}")
if len(hits) > 220:
    print(f"... +{len(hits)-220} more")

# Also show 40 lines around the first occurrence of "fleet_snapshot"
idx = None
for i, l in enumerate(lines, start=1):
    if "fleet_snapshot" in l.lower():
        idx = i
        break

if idx is not None:
    a = max(1, idx-20)
    b = min(len(lines), idx+20)
    print(f"\n=== CONTEXT around first 'fleet_snapshot' (line {idx}) ===")
    for n in range(a, b+1):
        print(f"{n:04d}: {lines[n-1]}")
else:
    print("\nNOTE: No literal 'fleet_snapshot' substring found. It may be built via a helper or imported constant.")
