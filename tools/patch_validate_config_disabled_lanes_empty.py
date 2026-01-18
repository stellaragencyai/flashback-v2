from pathlib import Path

ROOT = Path(r"C:\Flashback")
V = ROOT / "app" / "tools" / "validate_config.py"
backup = V.with_suffix(".py.bak_disabled_empty")

s = V.read_text(encoding="utf-8")

MARK = "DISABLED_LANES_CAN_BE_EMPTY"
if MARK in s:
    print("SKIP: validate_config.py already patched.")
    raise SystemExit(0)

# Backup once
if not backup.exists():
    backup.write_text(s, encoding="utf-8")
    print(f"OK: backup created: {backup}")
else:
    print(f"NOTE: backup already exists: {backup}")

needle = 'ctx = f"strategies.yaml: subaccounts[{idx}]"'
if needle not in s:
    print("FAIL: could not find anchor line:", needle)
    raise SystemExit(2)

# Insert enabled_bool early (before symbols/timeframes checks)
injection = (
    needle
    + "\n"
    + f"        # {MARK}: allow disabled/manual lanes (flashback10/main) to keep symbols/timeframes empty.\n"
    + "        enabled_bool = bool(s.get('enabled', True))\n"
)

s2 = s.replace(needle, injection, 1)

# Make the symbols/timeframes validations conditional on enabled_bool.
# We patch the *if* guards, not the error strings.
sym_if = "if not isinstance(symbols, list) or len(symbols) == 0:"
tf_if  = "if not isinstance(timeframes, list) or len(timeframes) == 0:"

if sym_if not in s2:
    print("FAIL: expected symbols guard not found:", sym_if)
    raise SystemExit(3)
if tf_if not in s2:
    print("FAIL: expected timeframes guard not found:", tf_if)
    raise SystemExit(4)

s2 = s2.replace(sym_if, "if enabled_bool and (not isinstance(symbols, list) or len(symbols) == 0):", 1)
s2 = s2.replace(tf_if,  "if enabled_bool and (not isinstance(timeframes, list) or len(timeframes) == 0):", 1)

V.write_text(s2, encoding="utf-8")
print("OK: patched validate_config.py (disabled lanes may have empty symbols/timeframes).")
