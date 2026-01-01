from pathlib import Path
import re

p = Path(r"app\bots\signal_engine.py")
s = p.read_text(encoding="utf-8", errors="ignore")

if "GET_STRAT_ATTR_OBJECTS_V1" in s:
    raise SystemExit("REFUSE: patch already applied (GET_STRAT_ATTR_OBJECTS_V1 found)")

# Find the helper def
m = re.search(r"(?m)^def _get_strat_attr\(", s)
if not m:
    raise SystemExit("PATCH FAILED: could not find def _get_strat_attr(")

# Find end of function by next top-level def
tail = s[m.start():]
m2 = re.search(r"(?m)^\ndef\s+", tail)
if not m2:
    raise SystemExit("PATCH FAILED: could not locate end of _get_strat_attr (next def)")

fn_block = tail[:m2.start()]
rest = tail[m2.start():]

# If it already handles getattr, refuse
if "getattr(" in fn_block:
    raise SystemExit("REFUSE: _get_strat_attr already appears to handle objects (getattr found)")

# Inject object support near the top, after the signature line
lines = fn_block.splitlines()

# Find first non-signature executable line to insert after doc/guards
insert_at = None
for i,l in enumerate(lines):
    if i == 0:
        continue
    # insert right after first blank line or after initial comments
    if l.strip() == "":
        insert_at = i+1
        break
if insert_at is None:
    insert_at = 1

inject = [
    "    # GET_STRAT_ATTR_OBJECTS_V1: support Strategy objects (pydantic/dataclass) as well as dicts",
    "    try:",
    "        if obj is not None and not isinstance(obj, dict):",
    "            if hasattr(obj, key):",
    "                v = getattr(obj, key)",
    "                return v if v is not None else default",
    "    except Exception:",
    "        pass",
    "",
]

lines = lines[:insert_at] + inject + lines[insert_at:]
new_fn_block = "\n".join(lines)

s2 = s[:m.start()] + new_fn_block + rest
p.write_text(s2, encoding="utf-8", newline="\n")
print("OK: patched _get_strat_attr to support Strategy objects (GET_STRAT_ATTR_OBJECTS_V1)")
