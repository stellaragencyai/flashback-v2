from __future__ import annotations

from pathlib import Path
import time

P = Path(r"C:\flashback\app\bots\executor_v2.py")
txt = P.read_text(encoding="utf-8", errors="replace")
lines = txt.splitlines()

targets = []
for i, line in enumerate(lines):
    if 'with SIGNAL_FILE.open("rb") as f:' in line:
        targets.append(i)

if not targets:
    raise SystemExit("ERROR: Could not find any 'with SIGNAL_FILE.open(\"rb\") as f:' blocks")

inserted = 0

# For each rb open block, find the next "for line in f:" and insert BOM-strip right after it.
for start_idx in targets:
    for_idx = None
    for j in range(start_idx, min(len(lines), start_idx + 200)):
        if "for line in f:" in lines[j].replace(" ", ""):  # tolerate spacing
            for_idx = j
            break
        if "for line in f:" in lines[j]:
            for_idx = j
            break

    if for_idx is None:
        continue

    indent = " " * (len(lines[for_idx]) - len(lines[for_idx].lstrip(" ")))
    bom_block = [
        f"{indent}    # Strip UTF-8 BOM if present (PowerShell UTF8 can add BOM).",
        f"{indent}    if isinstance(line, (bytes, bytearray)) and line.startswith(b\"\\xef\\xbb\\xbf\"):",
        f"{indent}        line = line[3:]",
    ]

    # Only insert if not already present nearby
    window = "\n".join(lines[for_idx: min(len(lines), for_idx + 8)])
    if "line.startswith(b\"\\xef\\xbb\\xbf\")" in window or "Strip UTF-8 BOM" in window:
        continue

    lines = lines[:for_idx + 1] + bom_block + lines[for_idx + 1:]
    inserted += 1

txt2 = "\n".join(lines) + "\n"

# Compile check before writing
try:
    compile(txt2, str(P), "exec")
except Exception as e:
    print("ERROR: Modified executor_v2.py does not compile. No changes written.")
    print("Compile error:", repr(e))
    if getattr(e, "lineno", None):
        ln = int(e.lineno)
        lo = max(1, ln - 6)
        hi = min(len(lines), ln + 5)
        print("\n--- excerpt ---")
        for k in range(lo, hi + 1):
            print(f"{k:5d}: {txt2.splitlines()[k-1]}")
        print("--------------")
    raise

bak = P.with_suffix(".py.bak_" + str(int(time.time())))
bak.write_text(txt, encoding="utf-8")
P.write_text(txt2, encoding="utf-8", newline="\n")

print("OK: executor_v2 now strips UTF-8 BOM on observed.jsonl lines")
print(" - backup:", bak.name)
print(" - bom_blocks_inserted:", inserted)
