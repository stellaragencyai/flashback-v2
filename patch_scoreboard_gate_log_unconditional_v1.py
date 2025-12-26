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
    raise SystemExit("ERROR: Could not find scoreboard_gate = scoreboard_gate_decide(")

# find closing ) of the call
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
    raise SystemExit("ERROR: Could not find closing ')' for scoreboard_gate_decide call")

indent = " " * (len(lines[idx]) - len(lines[idx].lstrip(" ")))

insert_block = [
    f"{indent}try:",
    f"{indent}    bound.info(\"🧪 Scoreboard gate CALL trade_id=%s st=%s tf=%s sym=%s\", client_trade_id, str(setup_type_raw), str(tf), str(symbol))",
    f"{indent}    if scoreboard_gate is None:",
    f"{indent}        bound.info(\"🧪 Scoreboard gate RESULT = None (no match / not loaded)\")",
    f"{indent}    else:",
    f"{indent}        bound.info(\"🧪 Scoreboard gate RESULT code=%s sm=%s reason=%s bucket=%s\", scoreboard_gate.get('decision_code'), scoreboard_gate.get('size_multiplier'), scoreboard_gate.get('reason'), scoreboard_gate.get('bucket_key'))",
    f"{indent}except Exception:",
    f"{indent}    pass",
]

lines2 = lines[: end_idx + 1] + insert_block + lines[end_idx + 1 :]
txt2 = "\n".join(lines2) + "\n"

# compile check before writing
try:
    compile(txt2, str(P), "exec")
except Exception as e:
    print("ERROR: Modified executor_v2.py does not compile. No changes written.")
    print("Compile error:", repr(e))
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

print("OK: added unconditional scoreboard gate call/result logging")
print(" - backup:", bak.name)
print(" - inserted_after_line:", end_idx + 1)
