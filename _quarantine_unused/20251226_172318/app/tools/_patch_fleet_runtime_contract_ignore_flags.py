from pathlib import Path

p = Path(r"app\ops\fleet_runtime_contract.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# If already patched, exit
if "ignore_ws_heartbeat" in s and "ignore_memory" in s and "ignore_decisions" in s:
    print("OK: fleet_runtime_contract already has ignore flags")
    raise SystemExit(0)

# We patch the validate_runtime_contract signature by adding 3 optional kwargs with defaults.
needle = "def validate_runtime_contract("
idx = s.find(needle)
if idx == -1:
    raise SystemExit("FATAL: could not find validate_runtime_contract definition")

# Find the end of the function signature line(s) up to the closing ): of the def
start = idx
end = s.find("):", start)
if end == -1:
    raise SystemExit("FATAL: could not find end of validate_runtime_contract signature")

sig_block = s[start:end+2]

# Only patch if we see force_orderbook_optional in signature (your version likely has it)
if "force_orderbook_optional" not in sig_block:
    raise SystemExit("FATAL: unexpected validate_runtime_contract signature (missing force_orderbook_optional). Paste the function header if this triggers.")

# Insert new args just before the closing ):
# We insert after force_orderbook_optional parameter line if present.
insertion = "\n    ignore_ws_heartbeat: bool = False,\n    ignore_memory: bool = False,\n    ignore_decisions: bool = False,\n"

# Try to inject before the final '):'
sig_block2 = sig_block.replace("):", insertion + "):", 1)

s2 = s[:start] + sig_block2 + s[end+2:]
p.write_text(s2, encoding="utf-8")
print("OK: patched fleet_runtime_contract validate_runtime_contract signature (added ignore flags)")
