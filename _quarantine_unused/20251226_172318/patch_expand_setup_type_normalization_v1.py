from __future__ import annotations

from pathlib import Path
import time
import re

P = Path(r"C:\flashback\app\bots\executor_v2.py")
txt = P.read_text(encoding="utf-8", errors="replace")

# --- 1) Expand _SETUP_TYPE_ALIASES dict safely ---
# We'll insert new aliases if missing.
def ensure_alias(key: str, value: str) -> None:
    global txt
    if f'"{key}":' in txt or f"'{key}':" in txt:
        return
    # Insert near the end of the alias dict, before the closing "}"
    m = re.search(r"_SETUP_TYPE_ALIASES\s*=\s*\{", txt)
    if not m:
        raise SystemExit("ERROR: Could not find _SETUP_TYPE_ALIASES = {")
    # Find the dict end by locating the first "\n}\n" after the alias start (good enough for this file style)
    start = m.end()
    end = txt.find("\n}\n", start)
    if end == -1:
        raise SystemExit("ERROR: Could not find end of _SETUP_TYPE_ALIASES dict")
    insertion = f'    "{key}": "{value}",\n'
    txt = txt[:end] + insertion + txt[end:]


# Canonicalize new setup types
ensure_alias("mm_spread_capture", "mm_spread_capture")
ensure_alias("spread_capture", "mm_spread_capture")
ensure_alias("market_making", "mm_spread_capture")
ensure_alias("pump_chase_momo", "momentum")
ensure_alias("pump_chase", "momentum")
ensure_alias("momo", "momentum")
ensure_alias("momentum", "momentum")
ensure_alias("swing_reversion_extreme", "mean_reversion")
ensure_alias("swing_reversion", "mean_reversion")

# --- 2) Expand _normalize_setup_type substring rules ---
# Insert rules after the existing scalp/breakout/pullback/range checks.
needle = '    if ("scalp" in tok) or ("liquidity_sweep" in tok):\n        return "scalp", "substring:scalp_or_liquidity_sweep"\n'
pos = txt.find(needle)
if pos == -1:
    raise SystemExit("ERROR: Could not find normalization scalp substring block to anchor patch.")

insert_block = (
    needle
    + '\n'
    + '    if ("mm_spread_capture" in tok) or ("spread_capture" in tok) or ("market_making" in tok):\n'
    + '        return "mm_spread_capture", "substring:mm_spread_capture"\n'
    + '\n'
    + '    if ("pump_chase_momo" in tok) or ("pump_chase" in tok) or ("momo" in tok) or ("momentum" in tok):\n'
    + '        return "momentum", "substring:momentum"\n'
    + '\n'
    + '    if ("swing_reversion_extreme" in tok) or ("swing_reversion" in tok):\n'
    + '        return "mean_reversion", "substring:swing_reversion"\n'
)

# Replace the needle with the expanded version (idempotent-ish)
if 'substring:mm_spread_capture' not in txt:
    txt = txt.replace(needle, insert_block)

# --- Compile check before writing ---
try:
    compile(txt, str(P), "exec")
except Exception as e:
    print("ERROR: Patched executor_v2.py does not compile. No changes written.")
    print("Compile error:", repr(e))
    raise

bak = P.with_suffix(".py.bak_" + str(int(time.time())))
bak.write_text(P.read_text(encoding="utf-8", errors="replace"), encoding="utf-8")

P.write_text(txt, encoding="utf-8", newline="\n")

print("OK: expanded setup_type normalization (aliases + substring rules)")
print(" - backup:", bak.name)
