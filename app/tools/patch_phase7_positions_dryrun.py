from pathlib import Path
import re
import os

TARGET = Path(r"app\core\ai_state_bus.py")

s = TARGET.read_text(encoding="utf-8", errors="ignore")

# -------------------------------------------------
# 0) Ensure EXEC_DRY_RUN mirror exists
# -------------------------------------------------
if not re.search(r'^\s*EXEC_DRY_RUN\s*[:=]', s, flags=re.M):
    m = re.search(r'^import\s+os\s*\r?\n', s, flags=re.M)
    if not m:
        raise SystemExit("PATCH_FAIL: could not find import os anchor")
    insert = (
        m.group(0)
        + "\n# Mirror of flashback_common.EXEC_DRY_RUN (avoid import coupling)\n"
        + "EXEC_DRY_RUN: bool = os.getenv(\"EXEC_DRY_RUN\", \"false\").strip().lower() in (\"1\",\"true\",\"yes\",\"y\",\"on\")\n"
    )
    s = s[:m.start()] + insert + s[m.end():]

# -------------------------------------------------
# 1) Patch safety evaluator (REAL blocker)
# -------------------------------------------------
anchor = "    reasons: List[str] = []\n    is_safe = True\n"
if s.count(anchor) != 1:
    raise SystemExit(f"PATCH_FAIL: expected 1 eval anchor, got {s.count(anchor)}")

s = s.replace(
    anchor,
    anchor
    + "\n    # DRY_RUN: positions may be empty; do not fail safety on positions freshness\n"
    + "    if EXEC_DRY_RUN:\n"
    + "        positions_bus_age_sec = None\n",
    1,
)

# -------------------------------------------------
# 2) Patch BOTH snapshot builders
# -------------------------------------------------
needle = "    freshness = {\n        \"positions_bus_age_sec\": pos_age_sec,\n"
count = s.count(needle)
if count != 2:
    raise SystemExit(f"PATCH_FAIL: expected 2 freshness blocks, got {count}")

s = s.replace(
    needle,
    "    # DRY_RUN: positions freshness must not gate safety\n"
    "    if EXEC_DRY_RUN:\n"
    "        pos_age_sec = None\n\n"
    + needle,
    2,
)

TARGET.write_text(s, encoding="utf-8")
print("PATCH_OK: Phase 7 Step 10 applied successfully")
