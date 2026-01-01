from __future__ import annotations

from pathlib import Path
import time
import re

P = Path(r"C:\flashback\app\tools\ai_scoreboard_snapshot_v1.py")
txt = P.read_text(encoding="utf-8", errors="replace")

# Drop buckets that are not real signals (quarantine/test)
DENY_SETUP_TYPES = {"unknown", "this_is_not_real"}

# Inject denylist if missing
if "DENY_SETUP_TYPES" not in txt:
    m = re.search(r"OUTCOMES_PATH_DEFAULT\s*=.*\n", txt)
    deny_block = "\n# Drop buckets that are not real signals (quarantine/test)\nDENY_SETUP_TYPES = {\"unknown\", \"this_is_not_real\"}\n"
    if m:
        txt = txt[:m.end()] + deny_block + txt[m.end():]
    else:
        txt = deny_block + txt

# Patch bucket construction loop
pattern = r"key\s*=\s*BucketKey\(\s*\n\s*setup_type=str\(row\.get\(\"setup_type\", \"unknown\"\)\),"
m = re.search(pattern, txt, flags=re.MULTILINE)
if not m:
    raise SystemExit("ERROR: Could not find BucketKey(setup_type=...) block to patch")

key_pos = m.start()
guard = (
    "        st = str(row.get(\"setup_type\", \"unknown\"))\n"
    "        if st in DENY_SETUP_TYPES:\n"
    "            continue\n\n"
)

txt2 = txt[:key_pos] + guard + txt[key_pos:]
txt2 = re.sub(
    r"setup_type=str\(row\.get\(\"setup_type\", \"unknown\"\)\)",
    "setup_type=st",
    txt2,
    count=1
)

# Compile check BEFORE write
try:
    compile(txt2, str(P), "exec")
except Exception as e:
    print("ERROR: patched ai_scoreboard_snapshot_v1.py does not compile")
    print("Compile error:", repr(e))
    raise

bak = P.with_suffix(".py.bak_" + str(int(time.time())))
bak.write_text(txt, encoding="utf-8")

P.write_text(txt2, encoding="utf-8", newline="\n")

print("OK: patched ai_scoreboard_snapshot_v1.py (deny unknown/test buckets)")
print(" - backup:", bak.name)
