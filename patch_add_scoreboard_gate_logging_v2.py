from __future__ import annotations

from pathlib import Path
import time

P = Path(r"C:\flashback\app\bots\executor_v2.py")
txt = P.read_text(encoding="utf-8", errors="replace")
lines = txt.splitlines()

needle = "scoreboard_gate = scoreboard_gate_decide("
idx = None
for i, line in enumerate(lines):
    if needle in line:
        idx = i
        break

if idx is None:
    raise SystemExit("ERROR: Could not find scoreboard_gate = scoreboard_gate_decide( in executor_v2.py")

# Find end of the call by tracking parentheses balance starting from the first "(" on that line
balance = 0
started = False
end_idx = None

for j in range(idx, len(lines)):
    s = lines[j]
    for ch in s:
        if ch == "(":
            balance += 1
            started = True
        elif ch == ")":
            balance -= 1
    if started and balance == 0:
        end_idx = j
        break

if end_idx is None:
    raise SystemExit("ERROR: Could not find the closing ')' for scoreboard_gate_decide(...) call.")

# Determine indent level from the call line (leading spaces)
call_indent = len(lines[idx]) - len(lines[idx].lstrip(" "))
# We want to insert at the same block indentation as the call line (inside the try:)
ins_indent = " " * call_indent

insert_block = [
    f"{ins_indent}try:",
    f"{ins_indent}    if scoreboard_gate is not None:",
    f"{ins_indent}        bound.info(",
    f"{ins_indent}            \"✅ Scoreboard gate MATCH trade_id=%s bucket=%s code=%s sm=%s reason=%s\",",
    f"{ins_indent}            client_trade_id,",
    f"{ins_indent}            scoreboard_gate.get(\"bucket_key\"),",
    f"{ins_indent}            scoreboard_gate.get(\"decision_code\"),",
    f"{ins_indent}            scoreboard_gate.get(\"size_multiplier\"),",
    f"{ins_indent}            scoreboard_gate.get(\"reason\"),",
    f"{ins_indent}        )",
    f"{ins_indent}except Exception:",
    f"{ins_indent}    pass",
]

# Insert after end_idx line
lines2 = lines[: end_idx + 1] + insert_block + lines[end_idx + 1 :]
txt2 = "\n".join(lines2) + "\n"

# Compile-check BEFORE writing
try:
    compile(txt2, str(P), "exec")
except Exception as e:
    print("ERROR: Modified executor_v2.py does not compile. No changes written.")
    print("Compile error:", repr(e))
    # excerpt
    if getattr(e, "lineno", None):
        ln = int(e.lineno)
        lo = max(1, ln - 6)
        hi = min(len(lines2), ln + 5)
        print("\n--- excerpt ---")
        for k in range(lo, hi + 1):
            print(f"{k:5d}: {lines2[k-1]}")
        print("--------------")
    raise

bak = P.with_suffix(".py.bak_" + str(int(time.time())))
bak.write_text(txt, encoding="utf-8")
P.write_text(txt2, encoding="utf-8", newline="\n")

print("OK: added scoreboard gate MATCH logging (safe insert after call close)")
print(" - backup:", bak.name)
print(" - inserted_after_line:", end_idx + 1)
