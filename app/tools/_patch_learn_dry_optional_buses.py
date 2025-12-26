from pathlib import Path
import re

p = Path(r"app\ops\fleet_runtime_contract.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# Already patched?
if "Phase8: LEARN_DRY must tolerate WS/REST gaps" in s and "require_positions=False" in s and "require_trades=False" in s:
    print("OK: LEARN_DRY already patched (idempotent)")
    raise SystemExit(0)

# Patch by regex inside the LEARN_DRY ModeSpec block (less brittle than exact string match)
pattern = r'(if m == "LEARN_DRY":\s*return ModeSpec\(\s*name="LEARN_DRY",)(.*?)(\)\s*)'
m = re.search(pattern, s, flags=re.DOTALL)
if not m:
    raise SystemExit("FATAL: could not locate LEARN_DRY ModeSpec block")

block = m.group(0)

# Replace require_positions/trades lines conservatively
block2 = block
block2 = re.sub(r"require_positions\s*=\s*True", "require_positions=False", block2)
block2 = re.sub(r"require_trades\s*=\s*True", "require_trades=False", block2)

# Ensure comment is present
if "Phase8: LEARN_DRY must tolerate WS/REST gaps" not in block2:
    block2 = block2.replace('name="LEARN_DRY",', 'name="LEARN_DRY",\n            # Phase8: LEARN_DRY must tolerate WS/REST gaps at the fleet layer.\n            # Buses are observed and WARNed, not FAILed.')

if block2 == block:
    raise SystemExit("FATAL: block found but no changes were applied (unexpected state)")

p.write_text(s.replace(block, block2, 1), encoding="utf-8")
print("OK: LEARN_DRY patch applied (idempotent regex)")
