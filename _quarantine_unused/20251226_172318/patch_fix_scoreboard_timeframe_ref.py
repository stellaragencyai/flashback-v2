from __future__ import annotations

import time
from pathlib import Path

TARGET = Path(r"C:\flashback\app\bots\executor_v2.py")
if not TARGET.exists():
    raise SystemExit(f"ERROR: missing {TARGET}")

src = TARGET.read_text(encoding="utf-8", errors="replace")
bak = TARGET.with_name(f"{TARGET.name}.bak_{int(time.time())}")
bak.write_text(src, encoding="utf-8")

changed = 0

# 1) Fix common scoreboard gate arg patterns:
#    - timeframe=timeframe  -> timeframe=str(tf)
#    - timeframe=str(timeframe) -> timeframe=str(tf)
patterns = [
    ("timeframe=timeframe", "timeframe=str(tf)"),
    ("timeframe=str(timeframe)", "timeframe=str(tf)"),
    ("timeframe=str(timeframe_norm)", "timeframe=str(tf)"),
]
for a, b in patterns:
    if a in src:
        src = src.replace(a, b)
        changed += 1

# 2) If the code uses a dict payload with 'timeframe': timeframe
if "'timeframe': timeframe" in src:
    src = src.replace("'timeframe': timeframe", "'timeframe': str(tf)")
    changed += 1
if '"timeframe": timeframe' in src:
    src = src.replace('"timeframe": timeframe', '"timeframe": str(tf)"')
    changed += 1

# 3) Hard safety: if there's a local var named timeframe used later, define it from tf once.
# Anchor near the scoreboard gate block to avoid messing other logic.
anchor = "Scoreboard gate"
if anchor in src and "timeframe = str(tf)" not in src:
    # Insert a defensive assignment right before the first "Scoreboard gate" log line
    idx = src.find(anchor)
    line_start = src.rfind("\n", 0, idx)
    insert_at = line_start if line_start >= 0 else idx
    injection = "\n    timeframe = str(tf)  # patch: scoreboard gate expects timeframe; use normalized tf\n"
    src = src[:insert_at] + injection + src[insert_at:]
    changed += 1

TARGET.write_text(src, encoding="utf-8")

print("OK: patched executor_v2.py (scoreboard timeframe fix)")
print(f" - backups: {bak.name}")
print(f" - change_blocks: {changed}")
