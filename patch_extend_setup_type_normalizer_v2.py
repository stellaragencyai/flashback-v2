from __future__ import annotations

from pathlib import Path
import re
import time

P = Path(r"C:\flashback\app\bots\executor_v2.py")
txt = P.read_text(encoding="utf-8", errors="replace")

# Find the _normalize_setup_type function block
m = re.search(r"(?ms)^def _normalize_setup_type\(raw: Any\) -> Tuple\[str, str\]:\n(.*?)(?=^\S)", txt)
if not m:
    raise SystemExit("ERROR: Could not locate _normalize_setup_type(...) function block")

block = m.group(0)

# If already patched, do nothing
if "substring:mm_spread_capture" in block and "substring:reversion" in block:
    print("OK: normalizer already contains mm_spread_capture + reversion rules (no changes).")
    raise SystemExit(0)

# Insert new rules right after the existing scalp/liquidity_sweep rule if possible
needle = '    if "scalp" in s or "liquidity_sweep" in s:\n        return "scalp", "substring:scalp_or_liquidity_sweep"\n'
if needle not in block:
    # fallback: insert near the end before aliases section
    needle2 = "    # Exact aliases\n"
    if needle2 not in block:
        raise SystemExit("ERROR: Could not find insertion point in _normalize_setup_type (missing 'Exact aliases' comment).")
    insert_at = block.index(needle2)
    before = block[:insert_at]
    after = block[insert_at:]
else:
    insert_at = block.index(needle) + len(needle)
    before = block[:insert_at]
    after = block[insert_at:]

insert = (
    '    if "mm_spread_capture" in s:\n'
    '        return "scalp", "substring:mm_spread_capture"\n'
    '    if "reversion" in s:\n'
    '        return "mean_reversion", "substring:reversion"\n'
)

block2 = before + insert + after

txt2 = txt[:m.start()] + block2 + txt[m.end():]

# Full-file compile check BEFORE writing
try:
    compile(txt2, str(P), "exec")
except Exception as e:
    print("ERROR: Modified executor_v2.py does not compile. No changes written.")
    print("Compile error:", repr(e))
    raise

bak = P.with_suffix(".py.bak_" + str(int(time.time())))
bak.write_text(txt, encoding="utf-8")
P.write_text(txt2, encoding="utf-8", newline="\n")

print("OK: extended _normalize_setup_type with mm_spread_capture + reversion rules (compile-verified)")
print(" - backup:", bak.name)
