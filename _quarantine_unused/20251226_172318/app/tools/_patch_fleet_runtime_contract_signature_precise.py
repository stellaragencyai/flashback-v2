from pathlib import Path

p = Path(r"app\ops\fleet_runtime_contract.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# If signature already has ignore args, exit
if "ignore_ws_heartbeat" in s and "ignore_memory" in s and "ignore_decisions" in s and "def validate_runtime_contract" in s:
    # Still need to ensure they're in the signature, but this is a quick skip.
    pass

needle = "force_orderbook_optional: 'bool' = False"
if needle not in s:
    needle = "force_orderbook_optional: bool = False"
if needle not in s:
    raise SystemExit("FATAL: could not find force_orderbook_optional parameter in validate_runtime_contract signature")

# Inject immediately after the force_orderbook_optional parameter line
inject = needle + ",\n    ignore_ws_heartbeat: bool = False,\n    ignore_memory: bool = False,\n    ignore_decisions: bool = False"
s2 = s.replace(needle, inject, 1)

p.write_text(s2, encoding="utf-8")
print("OK: injected ignore flags into validate_runtime_contract signature")
