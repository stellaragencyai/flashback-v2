from pathlib import Path
import re

p = Path(r"app/dashboard/data_hydrator_v1.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# Add a helper to classify funds source based on automation_mode
if "def _funds_source" not in s:
    helper = """

def _funds_source(automation_mode: str) -> str:
    m = (automation_mode or "").strip().upper()
    if "LIVE" in m:
        return "REAL"
    if any(x in m for x in ("DRY", "PAPER", "SIM", "LEARN")):
        return "SIM"
    return "SIM"
"""
    # Insert helper near other helpers (after _now_ms is fine)
    s = s.replace("def _now_ms() -> int:", "def _now_ms() -> int:", 1) + helper

# Inject fields into row dict (look for "automation_mode": ...)
needle = '"automation_mode": sa.get("automation_mode") or "unknown",'
if needle not in s:
    raise SystemExit("PATCH_FAIL: could not find automation_mode field in row dict")

# Add funds_source + balance_display_mode
add = needle + "\n            \"funds_source\": _funds_source(sa.get(\"automation_mode\") or \"unknown\"),\n            \"balance_display_mode\": (\"SHOW\" if _funds_source(sa.get(\"automation_mode\") or \"unknown\") == \"REAL\" else \"HIDE\"),"
s2 = s.replace(needle, add, 1)

p.write_text(s2, encoding="utf-8")
print("OK Step 31 applied: funds_source + balance_display_mode added to rows")
