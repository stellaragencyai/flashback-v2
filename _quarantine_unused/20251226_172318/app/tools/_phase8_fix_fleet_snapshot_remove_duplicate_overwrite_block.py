from __future__ import annotations

from pathlib import Path

P = Path(r"app\ops\fleet_snapshot_tick.py")
s = P.read_text(encoding="utf-8", errors="ignore").splitlines(True)

def find_lines_containing(sub: str):
    return [i for i,ln in enumerate(s) if sub in ln]

acc_hits = find_lines_containing("acc = ops_accounts.get(label)")
mode_hits = find_lines_containing("mode_upper =")

if len(acc_hits) < 2:
    raise SystemExit(f"FATAL: expected >=2 'acc = ops_accounts.get(label)' hits, found {len(acc_hits)}")

# We want to remove the SECOND overwrite block (the one that starts at the 2nd acc-hit)
start = acc_hits[1]

# Find the first mode_upper AFTER that start (this is our end anchor)
end = None
for j in mode_hits:
    if j > start:
        end = j
        break

if end is None:
    raise SystemExit("FATAL: could not find 'mode_upper =' after second acc block to anchor removal")

# Safety: ensure we are not deleting too much (should be a small block)
removed_lines = end - start
if removed_lines < 3 or removed_lines > 80:
    raise SystemExit(f"FATAL: suspicious removal size={removed_lines} lines (start={start+1}, end={end+1}). Aborting.")

before = "".join(s)
s2 = s[:start] + s[end:]
after = "".join(s2)

# Extra safety: ensure only one acc-hit remains now
acc_hits_after = [i for i,ln in enumerate(s2) if "acc = ops_accounts.get(label)" in ln]
if len(acc_hits_after) != 1:
    raise SystemExit(f"FATAL: post-edit expected exactly 1 acc-hit, got {len(acc_hits_after)}. Aborting.")

P.write_text(after, encoding="utf-8")
print(f"OK: removed duplicate overwrite block lines={removed_lines} (start_line={start+1} end_line={end})")
