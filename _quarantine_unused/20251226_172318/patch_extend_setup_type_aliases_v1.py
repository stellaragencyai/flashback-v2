from __future__ import annotations

import re
import time
from pathlib import Path

P = Path(r"C:\flashback\app\bots\executor_v2.py")
txt = P.read_text(encoding="utf-8", errors="replace")

# Locate the alias dict start robustly (handles type annotations).
m = re.search(r"(?m)^_SETUP_TYPE_ALIASES[^\n]*=\s*\{", txt)
if not m:
    raise SystemExit("ERROR: Could not find _SETUP_TYPE_ALIASES dict start in executor_v2.py")

start = m.end()  # position just after the opening "{"

# We'll insert only if keys aren't already present.
needed = {
    # New/observed setups -> canonical buckets
    "mm_spread_capture": "scalp",
    "spread_capture": "scalp",
    "market_maker": "scalp",

    "pump_chase_momo": "breakout",
    "momo": "breakout",
    "momentum": "breakout",

    "trend_pullback": "pullback",
    "pullback_trend": "pullback",

    "swing_reversion_extreme": "mean_reversion",
    "reversion_extreme": "mean_reversion",
}

# Quick scan of existing dict text (we only check presence of "key": patterns).
existing = set(re.findall(r'"\s*([^"]+)\s*"\s*:', txt[m.start():m.start()+5000]))

to_add = [(k, v) for k, v in needed.items() if k not in existing]
if not to_add:
    print("OK: alias keys already present. No changes needed.")
    raise SystemExit(0)

insert_lines = []
insert_lines.append("")  # newline buffer
insert_lines.append("    # --- added by patch_extend_setup_type_aliases_v1 ---")
for k, v in to_add:
    insert_lines.append(f'    "{k}": "{v}",')
insert_lines.append("    # --- end patch_extend_setup_type_aliases_v1 ---")
insert = "\n" + "\n".join(insert_lines) + "\n"

txt2 = txt[:start] + insert + txt[start:]

# Compile-check BEFORE writing.
try:
    compile(txt2, str(P), "exec")
except Exception as e:
    print("ERROR: Modified executor_v2.py does not compile. No changes written.")
    print("Compile error:", repr(e))
    raise

bak = P.with_suffix(".py.bak_" + str(int(time.time())))
bak.write_text(txt, encoding="utf-8")
P.write_text(txt2, encoding="utf-8", newline="\n")

print("OK: extended _SETUP_TYPE_ALIASES + compile-verified whole file")
print(" - backup:", bak.name)
print(" - added:", len(to_add), "aliases")
