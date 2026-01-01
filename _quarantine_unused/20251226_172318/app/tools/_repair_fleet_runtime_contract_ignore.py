from pathlib import Path

p = Path(r"app\ops\fleet_runtime_contract.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# 1) Remove the bad injected block anywhere in file (global)
start_tag = "# --- Phase8: ignore flags (fleet-level overrides) ---"
end_tag = "# --- end ignore flags ---"
if start_tag in s and end_tag in s:
    a = s.find(start_tag)
    b = s.find(end_tag, a)
    if b != -1:
        b2 = b + len(end_tag)
        # remove whole lines containing the block
        pre = s[:a]
        post = s[b2:]
        # clean up potential dangling whitespace/newlines
        s = pre.rstrip() + "\n\n" + post.lstrip()
        print("OK: removed previously injected ignore block")
else:
    print("OK: no prior ignore block found to remove")

# 2) Locate validate_runtime_contract function region
needle_def = "def validate_runtime_contract("
i = s.find(needle_def)
if i == -1:
    raise SystemExit("FATAL: validate_runtime_contract def not found")

# Find end of function by scanning forward for next top-level def/class at column 0
lines = s.splitlines(True)
# Map index->(line_no, col0 check) using cumulative offsets
offsets = []
pos = 0
for idx, line in enumerate(lines):
    offsets.append(pos)
    pos += len(line)

# Find line index containing the def
def_line_idx = None
for idx, off in enumerate(offsets):
    if off <= i < off + len(lines[idx]):
        def_line_idx = idx
        break
if def_line_idx is None:
    raise SystemExit("FATAL: could not locate def line index")

# Determine function block line range
end_line_idx = len(lines)
for j in range(def_line_idx + 1, len(lines)):
    l = lines[j]
    if l.startswith("def ") or l.startswith("class "):
        end_line_idx = j
        break

func_lines = lines[def_line_idx:end_line_idx]

# 3) Inject ignore filtering inside the function, right before the last return ContractResult in that function
ret_idx = None
for j in range(len(func_lines)-1, -1, -1):
    if "return ContractResult" in func_lines[j]:
        ret_idx = j
        break
if ret_idx is None:
    raise SystemExit("FATAL: return ContractResult not found inside validate_runtime_contract")

inject_block = [
"    # --- Phase8: ignore flags (fleet-level overrides) ---\n",
"    if ignore_ws_heartbeat:\n",
"        faults_fail = [f for f in faults_fail if getattr(f, 'value', None) != 'WS_HEARTBEAT_MISSING']\n",
"        faults_warn = [f for f in faults_warn if getattr(f, 'value', None) != 'WS_HEARTBEAT_MISSING']\n",
"    if ignore_memory:\n",
"        drop = {'MEMORY_MISSING','MEMORY_STALE','MEMORY_PARSE_FAIL','MEMORY_COUNT_EXCEEDED'}\n",
"        faults_fail = [f for f in faults_fail if getattr(f, 'value', None) not in drop]\n",
"        faults_warn = [f for f in faults_warn if getattr(f, 'value', None) not in drop]\n",
"    if ignore_decisions:\n",
"        drop = {'DECISIONS_MISSING','DECISIONS_STALE','DECISIONS_TAIL_PARSE_FAIL','DECISIONS_SCHEMA_INVALID'}\n",
"        faults_fail = [f for f in faults_fail if getattr(f, 'value', None) not in drop]\n",
"        faults_warn = [f for f in faults_warn if getattr(f, 'value', None) not in drop]\n",
"    # --- end ignore flags ---\n",
"\n",
]

# Avoid double-inject if already present in function
if any("Phase8: ignore flags" in l for l in func_lines):
    print("OK: ignore logic already present inside function (no reinject)")
else:
    func_lines = func_lines[:ret_idx] + inject_block + func_lines[ret_idx:]
    print("OK: injected ignore logic inside validate_runtime_contract")

# Rebuild whole file
lines[def_line_idx:end_line_idx] = func_lines
s2 = "".join(lines)
p.write_text(s2, encoding="utf-8")
print("OK: wrote repaired fleet_runtime_contract.py")
