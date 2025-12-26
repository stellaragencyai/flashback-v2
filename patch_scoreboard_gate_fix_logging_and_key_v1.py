from __future__ import annotations

from pathlib import Path
import time

P = Path(r"C:\flashback\app\bots\executor_v2.py")
txt = P.read_text(encoding="utf-8", errors="replace")
lines = txt.splitlines()

# 1) Find scoreboard_gate_decide call start
needle = "scoreboard_gate = scoreboard_gate_decide("
idx = None
for i, line in enumerate(lines):
    if needle in line:
        idx = i
        break
if idx is None:
    raise SystemExit("ERROR: Could not find scoreboard_gate = scoreboard_gate_decide(")

# 2) Find closing line of that call by parentheses balancing
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

# 3) Patch call args: use normalized setup_type instead of setup_type_raw if present
changed_setup_arg = 0
for k in range(idx, end_idx + 1):
    if "setup_type=str(setup_type_raw)" in lines[k]:
        lines[k] = lines[k].replace("setup_type=str(setup_type_raw)", "setup_type=str(setup_type)")
        changed_setup_arg += 1

# If the arg wasn't found exactly, try a looser replace
if changed_setup_arg == 0:
    for k in range(idx, end_idx + 1):
        if "setup_type=" in lines[k] and "setup_type_raw" in lines[k]:
            lines[k] = lines[k].replace("setup_type_raw", "setup_type")
            changed_setup_arg += 1

# 4) Remove any previous injected 🧪 block directly after the call (best-effort)
# We'll delete a small window of lines after end_idx if they contain our previous marker.
window_lo = end_idx + 1
window_hi = min(len(lines), end_idx + 30)
cleaned = 0
new_lines = []
i = 0
while i < len(lines):
    if i == window_lo:
        # detect old injected block by marker text
        block = lines[window_lo:window_hi]
        if any("🧪 Scoreboard gate" in x or "🧪 SCOREBOARD_GATE" in x for x in block):
            # delete until we hit a blank line or an obvious next section start
            j = window_lo
            while j < window_hi:
                if "if scoreboard_gate is not None" in lines[j]:
                    break
                j += 1
            # keep from j onward; skip window_lo..j-1
            cleaned = (j - window_lo)
            i = j
            continue
    new_lines.append(lines[i])
    i += 1

lines = new_lines

# Recompute indices because we may have removed lines
txt_tmp = "\n".join(lines) + "\n"
lines = txt_tmp.splitlines()

# Re-find call and end_idx again
idx = None
for i, line in enumerate(lines):
    if needle in line:
        idx = i
        break
if idx is None:
    raise SystemExit("ERROR: Could not re-find scoreboard gate call after cleanup")

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
    raise SystemExit("ERROR: Could not re-find closing ')' after cleanup")

indent = " " * (len(lines[idx]) - len(lines[idx].lstrip(" ")))

# 5) Insert SAFE unconditional logs (cannot NameError)
insert_block = [
    f"{indent}try:",
    f"{indent}    _tid = locals().get('client_trade_id') or locals().get('trade_id') or '?'",
    f"{indent}    _st_norm = locals().get('setup_type')",
    f"{indent}    _st_raw = locals().get('setup_type_raw')",
    f"{indent}    _tf = locals().get('tf')",
    f"{indent}    _sym = locals().get('symbol')",
    f"{indent}    _code = None if scoreboard_gate is None else scoreboard_gate.get('decision_code')",
    f"{indent}    log.info(\"🧪 SCOREBOARD_GATE call trade_id=%s st_norm=%s st_raw=%s tf=%s sym=%s -> %s\", _tid, _st_norm, _st_raw, _tf, _sym, _code)",
    f"{indent}    if scoreboard_gate is not None:",
    f"{indent}        log.info(\"🧪 SCOREBOARD_GATE decision allow=%s sm=%s reason=%s bucket=%s\", scoreboard_gate.get('allow'), scoreboard_gate.get('size_multiplier'), scoreboard_gate.get('reason'), scoreboard_gate.get('bucket_key'))",
    f"{indent}except Exception as e:",
    f"{indent}    log.warning(\"Scoreboard gate debug logging failed (non-fatal): %r\", e)",
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
bak.write_text(P.read_text(encoding="utf-8", errors="replace"), encoding="utf-8")
P.write_text(txt2, encoding="utf-8", newline="\n")

print("OK: fixed scoreboard gate key + added safe unconditional logging")
print(" - backup:", bak.name)
print(" - setup_type_arg_changed:", changed_setup_arg)
print(" - cleaned_old_lines:", cleaned)
print(" - inserted_after_line:", end_idx + 1)
