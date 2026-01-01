from __future__ import annotations

from pathlib import Path
import re
import sys

TARGET = Path(r"C:\Flashback\app\tools\ai_scoreboard_snapshot_v1.py")

def fail(msg: str) -> None:
    print(f"ERROR: {msg}")
    sys.exit(1)

txt = TARGET.read_text(encoding="utf-8")

if "min_conf" in txt and "SCOREBOARD_GATE_MIN_CONF" in txt:
    print("OK: already patched (min_conf present).")
    sys.exit(0)

# 1) Ensure we import os
if re.search(r"^\s*import\s+os\s*$", txt, flags=re.M) is None:
    # Insert `import os` after the last top-level import line.
    # We keep it simple and predictable: put it after `import json` if present, else after first import block.
    if "import json" in txt:
        txt = txt.replace("import json\n", "import json\nimport os\n", 1)
    else:
        # Insert after the first import line
        m = re.search(r"^(from\s+[^\n]+\s+import\s+[^\n]+|import\s+[^\n]+)\n", txt, flags=re.M)
        if not m:
            fail("Could not locate an import block to insert `import os`.")
        i = m.end()
        txt = txt[:i] + "import os\n" + txt[i:]

# 2) Inject min_conf variable near argparse parse (best-effort)
# We'll add a line right after args are parsed OR right before output dict is built.
if "SCOREBOARD_GATE_MIN_CONF" not in txt:
    # Try to place after "args = ap.parse_args()" (common pattern)
    if "args = ap.parse_args()" in txt:
        txt = txt.replace(
            "args = ap.parse_args()\n",
            "args = ap.parse_args()\n\n    # Gate threshold (kept in scoreboard for transparency)\n    min_conf = float(os.getenv(\"SCOREBOARD_GATE_MIN_CONF\", \"0.60\"))\n",
            1,
        )
    else:
        # We'll compute inline later if we can't find a nice spot
        pass

# 3) Add "min_conf" into the output JSON dict
# We look for the line that writes `"min_n": args.min_n,` and inject below it.
pat = r'(\n\s*"min_n"\s*:\s*args\.min_n\s*,\s*\n)'
m = re.search(pat, txt)
if not m:
    # Some versions use single quotes or different spacing. Try a more relaxed match.
    pat2 = r'(\n\s*[\'"]min_n[\'"]\s*:\s*args\.min_n\s*,\s*\n)'
    m2 = re.search(pat2, txt)
    if not m2:
        fail("Could not find the output dict line for min_n to inject min_conf.")
    inject_at = m2.end(1)
else:
    inject_at = m.end(1)

# Decide what to write: if min_conf variable exists, use it; otherwise inline compute.
use_var = "min_conf =" in txt and "SCOREBOARD_GATE_MIN_CONF" in txt
line = '    "min_conf": min_conf,\n' if use_var else '    "min_conf": float(os.getenv("SCOREBOARD_GATE_MIN_CONF", "0.60")),\n'

txt = txt[:inject_at] + line + txt[inject_at:]

TARGET.write_text(txt, encoding="utf-8")
print("OK: patched ai_scoreboard_snapshot_v1.py to write min_conf")
