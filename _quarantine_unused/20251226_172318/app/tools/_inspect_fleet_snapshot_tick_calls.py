from __future__ import annotations

from pathlib import Path

P = Path(r"app\ops\fleet_snapshot_tick.py")

if not P.exists():
    raise SystemExit(f"FATAL: {P} does not exist")

lines = P.read_text(encoding="utf-8", errors="ignore").splitlines()

print("FILE =", str(P.resolve()))
print("TOTAL_LINES =", len(lines))

keywords = [
    "write",
    "json",
    "dump",
    "atomic",
    "replace",
    "rename",
    "state",
    "snapshot",
    "fleet",
    "ops",
    "persist",
    "save",
    "Path(",
]

print("\n=== SIGNAL LINES ===")
for i, l in enumerate(lines, start=1):
    if any(k.lower() in l.lower() for k in keywords):
        print(f"{i:04d}: {l}")

print("\n=== LAST 120 LINES (writer usually lives here) ===")
start = max(1, len(lines) - 120)
for i in range(start, len(lines) + 1):
    print(f"{i:04d}: {lines[i-1]}")
