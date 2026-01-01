from __future__ import annotations

import time
from pathlib import Path

P = Path(r"C:\flashback\app\bots\executor_v2.py")
txt = P.read_text(encoding="utf-8", errors="replace")
lines = txt.splitlines()

WITH = 'with SIGNAL_FILE.open("rb") as f:'
FOR  = "for line in f:"

targets = []
for i, line in enumerate(lines):
    if WITH in line:
        # find the for-loop line soon after
        j_hit = None
        for j in range(i+1, min(i+40, len(lines))):
            if FOR in lines[j]:
                j_hit = j
                break
        if j_hit is None:
            raise SystemExit(f"ERROR: Found SIGNAL_FILE.open('rb') at line {i+1} but no '{FOR}' within 40 lines.")
        targets.append((i, j_hit))

if len(targets) < 1:
    raise SystemExit("ERROR: Could not find any SIGNAL_FILE.open('rb') blocks")

# Insert after each "for line in f:" (in reverse order so indices don't shift)
inserted = 0
for _, for_idx in reversed(targets):
    indent = " " * (len(lines[for_idx]) - len(lines[for_idx].lstrip(" ")))
    strip_line = f"{indent}    if isinstance(line, (bytes, bytearray)) and line.startswith(b'\\xef\\xbb\\xbf'): line = line[3:]  # strip UTF-8 BOM"
    # check if already present right after
    already = False
    for k in range(for_idx+1, min(for_idx+6, len(lines))):
        if "strip UTF-8 BOM" in lines[k] or "line.startswith(b'\\xef\\xbb\\xbf')" in lines[k]:
            already = True
            break
    if already:
        continue
    lines.insert(for_idx+1, strip_line)
    inserted += 1

txt2 = "\n".join(lines) + "\n"

# compile check before writing
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
            print(f"{k:5d}: {lines[k-1]}")
        print("--------------")
    raise

bak = P.with_suffix(".py.bak_" + str(int(time.time())))
bak.write_text(txt, encoding="utf-8")
P.write_text(txt2, encoding="utf-8", newline="\n")

print("OK: patched executor_v2 to strip UTF-8 BOM on signal lines")
print(" - backup:", bak.name)
print(" - insertions:", inserted)
print(" - blocks_found:", len(targets))
