from pathlib import Path
import re

p = Path(r"app\bots\signal_engine.py")
s = p.read_text(encoding="utf-8", errors="ignore")

if "UNWRAP_RAW_DICT_V1" in s:
    raise SystemExit("REFUSE: patch already applied (UNWRAP_RAW_DICT_V1 found)")

# Locate _get_strat_attr definition
m = re.search(r"(?m)^def _get_strat_attr\(", s)
if not m:
    raise SystemExit("PATCH FAILED: could not find def _get_strat_attr(")

# Get function block until next top-level def
tail = s[m.start():]
m2 = re.search(r"(?m)^\ndef\s+", tail)
if not m2:
    raise SystemExit("PATCH FAILED: could not locate end of _get_strat_attr (next def)")

fn_block = tail[:m2.start()]
rest = tail[m2.start():]

lines = fn_block.splitlines()

# Find a safe insertion point: right after the initial object getattr handling (or after first few lines)
insert_at = None
for i, l in enumerate(lines):
    if "getattr(" in l:
        # insert shortly after this section
        insert_at = i + 3
        break
if insert_at is None:
    insert_at = 2

inject = [
    "    # UNWRAP_RAW_DICT_V1: if obj is a Strategy-like object whose real config lives in obj.raw (dict), unwrap it",
    "    try:",
    "        raw = getattr(obj, 'raw', None)",
    "        if isinstance(raw, dict) and key in raw:",
    "            v = raw.get(key)",
    "            return v if v is not None else default",
    "    except Exception:",
    "        pass",
    "",
]

lines = lines[:insert_at] + inject + lines[insert_at:]
new_fn = "\n".join(lines)

s2 = s[:m.start()] + new_fn + rest
p.write_text(s2, encoding="utf-8", newline="\n")

print("OK: patched _get_strat_attr (UNWRAP_RAW_DICT_V1)")
