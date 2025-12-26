from pathlib import Path
import re

p = Path(r"app\bots\signal_engine.py")
s = p.read_text(encoding="utf-8", errors="ignore").splitlines()

# 1) Add LOOP_START print right after "while True:"
if any("DBG_LOOP_START_V1" in l for l in s):
    raise SystemExit("REFUSE: DBG_LOOP_START_V1 already applied")

out = []
inserted = False
for i, line in enumerate(s):
    out.append(line)
    if (not inserted) and line.strip() == "while True:":
        indent = line[:len(line) - len(line.lstrip())] + "    "
        out.append(indent + "# DBG_LOOP_START_V1")
        out.append(indent + 'print("[DBG] LOOP_START (entered while loop)", flush=True)')
        inserted = True

if not inserted:
    raise SystemExit("PATCH FAILED: could not find 'while True:'")

# 2) Force flush on the existing [DBG] counters print (if present)
for i, line in enumerate(out):
    if 'print(f"[DBG] setups_checked=' in line and "flush=True" not in line:
        out[i] = line.rstrip(")") + ", flush=True)"
        break

p.write_text("\n".join(out) + "\n", encoding="utf-8", newline="\n")
print("OK: patched signal_engine.py (DBG loop start + DBG flush)")
