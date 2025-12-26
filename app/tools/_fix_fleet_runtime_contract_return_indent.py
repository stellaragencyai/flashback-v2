from pathlib import Path

p = Path(r"app\ops\fleet_runtime_contract.py")
lines = p.read_text(encoding="utf-8", errors="ignore").splitlines(True)

# Find validate_runtime_contract def line
def_idx = None
for i, l in enumerate(lines):
    if l.startswith("def validate_runtime_contract("):
        def_idx = i
        break
if def_idx is None:
    raise SystemExit("FATAL: def validate_runtime_contract(...) not found")

# Find end of function (next top-level def/class)
end_idx = len(lines)
for j in range(def_idx + 1, len(lines)):
    if lines[j].startswith("def ") or lines[j].startswith("class "):
        end_idx = j
        break

fixed = 0
for k in range(def_idx + 1, end_idx):
    l = lines[k]
    # Fix the exact known offender(s)
    if l.startswith("return ContractResult"):
        lines[k] = "    " + l  # indent into function
        fixed += 1

p.write_text("".join(lines), encoding="utf-8")
print(f"OK: indented_return_lines={fixed}")
