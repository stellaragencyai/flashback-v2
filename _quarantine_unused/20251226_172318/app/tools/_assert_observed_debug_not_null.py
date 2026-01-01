from pathlib import Path
import json

p = Path(r"C:\Flashback\signals\observed.jsonl")
if not p.exists():
    raise SystemExit("MISSING observed.jsonl")

bad = 0
total = 0
for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
    line = line.strip()
    if not line:
        continue
    total += 1
    try:
        r = json.loads(line)
    except Exception:
        continue
    dbg = r.get("debug") or {}
    if dbg.get("last_close") is None or dbg.get("prev_close") is None or dbg.get("ma") is None:
        bad += 1

print("rows=", total, "bad_debug_rows=", bad)
if total > 0 and bad > 0:
    raise SystemExit("FAIL: debug contains nulls")
print("PASS")
