from __future__ import annotations

from pathlib import Path
import time
import re

P = Path(r"C:\flashback\app\bots\executor_v2.py")
txt = P.read_text(encoding="utf-8", errors="replace")
orig = txt
lines = txt.splitlines()

# locate function
def_line = None
for i, line in enumerate(lines):
    if line.startswith("def _normalize_setup_type("):
        def_line = i
        break
if def_line is None:
    raise SystemExit("ERROR: could not find def _normalize_setup_type(")

# find a good insertion point: right after the line that defines s0 (most common)
# e.g. s0 = s.split(":", 1)[0]
s0_line = None
for i in range(def_line, min(def_line + 120, len(lines))):
    if re.search(r"^\s*s0\s*=\s*", lines[i]):
        s0_line = i
        break
if s0_line is None:
    # fallback: after first "s =" assignment if it exists
    for i in range(def_line, min(def_line + 120, len(lines))):
        if re.search(r"^\s*s\s*=\s*", lines[i]):
            s0_line = i
            break
if s0_line is None:
    raise SystemExit("ERROR: could not find s0 = ... (or s = ...) inside _normalize_setup_type")

# do we already define s safely?
window = "\n".join(lines[def_line:min(def_line+140, len(lines))])
already = re.search(r"^\s*s\s*=\s*", window, re.M) is not None

changed = False
if not already:
    indent = re.match(r"^(\s*)", lines[s0_line]).group(1)
    # define s from s0 if present in scope, else define from raw conversion
    # safest: s = str(raw or '').lower()
    insert = f"{indent}s = str(raw or '').lower()"
    lines.insert(s0_line + 1, insert)
    changed = True

txt2 = "\n".join(lines) + "\n"

# compile check before write
try:
    compile(txt2, str(P), "exec")
except Exception as e:
    print("ERROR: Modified executor_v2.py does not compile. No changes written.")
    print("Compile error:", repr(e))
    if getattr(e, "lineno", None):
        ln = int(e.lineno)
        out_lines = txt2.splitlines()
        lo = max(1, ln - 6)
        hi = min(len(out_lines), ln + 5)
        print("\n--- excerpt ---")
        for k in range(lo, hi + 1):
            print(f"{k:5d}: {out_lines[k-1]}")
        print("--------------")
    raise

bak = P.with_suffix(".py.bak_" + str(int(time.time())))
bak.write_text(orig, encoding="utf-8")
P.write_text(txt2, encoding="utf-8", newline="\n")

print("OK: fixed _normalize_setup_type missing 's' (compile-verified)")
print(" - backup:", bak.name)
print(" - inserted_s_line:", changed)
