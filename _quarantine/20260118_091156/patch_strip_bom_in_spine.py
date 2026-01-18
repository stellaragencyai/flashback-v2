from pathlib import Path
import re

path = Path(r".\app\ai\ai_events_spine.py")
s = path.read_text(encoding="utf-8")

# Find the first "x = x.strip()" anywhere (we only patch ONE occurrence to minimize blast radius).
pat = re.compile(r'(?m)^(?P<indent>\s*)(?P<var>[A-Za-z_]\w*)\s*=\s*(?P=var)\.strip\(\)\s*$')

m = pat.search(s)
if not m:
    raise SystemExit("ERROR: could not find any 'x = x.strip()' pattern to patch")

var = m.group("var")
old = m.group(0)
new = f'{m.group("indent")}{var} = {var}.lstrip("\\ufeff").strip()'

s2 = s[:m.start()] + new + s[m.end():]
path.write_text(s2, encoding="utf-8")

print("OK: patched BOM strip (first self-strip occurrence)")
print("Patched var =", var)
print("OLD:", old)
print("NEW:", new)
