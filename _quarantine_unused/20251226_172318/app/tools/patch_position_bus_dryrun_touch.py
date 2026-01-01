from pathlib import Path
import re

p = Path(r"app\core\position_bus.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# 1) Add EXEC_DRY_RUN env mirror near ACCOUNT_LABEL (simple + local, no import coupling)
needle_env = 'ACCOUNT_LABEL: str = os.getenv("ACCOUNT_LABEL", "main").strip() or "main"\n'
if needle_env not in s:
    raise SystemExit("PATCH_FAIL: could not find ACCOUNT_LABEL env line")

insert_env = (
    needle_env +
    '\n# Mirror of flashback_common.EXEC_DRY_RUN (avoid importing to keep this module stable)\n'
    'EXEC_DRY_RUN: bool = os.getenv("EXEC_DRY_RUN", "false").strip().lower() in ("1","true","yes","y","on")\n'
)

s = s.replace(needle_env, insert_env, 1)

# 2) Inject DRY_RUN touch behavior inside get_positions_for_label(), after reading snapshot
needle = "    # Snapshot path\n    snap, age = get_snapshot()\n"
if needle not in s:
    raise SystemExit("PATCH_FAIL: could not find get_snapshot() call block")

insert = (
    "    # Snapshot path\n"
    "    snap, age = get_snapshot()\n"
    "\n"
    "    # DRY_RUN: keep positions bus fresh even when empty (no WS/REST dependency)\n"
    "    if EXEC_DRY_RUN:\n"
    "        # If missing or stale, write a fresh empty snapshot for this label\n"
    "        if snap is None or age is None or age > float(max_age_seconds):\n"
    "            labels_block = {}\n"
    "            if isinstance(snap, dict):\n"
    "                labels_block = dict(snap.get(\"labels\") or {})\n"
    "            labels_block[label] = {\"category\": category, \"positions\": []}\n"
    "            _save_snapshot(labels_block)\n"
    "            # return empty (paper mode should not invent positions)\n"
    "            return []\n"
)

s = s.replace(needle, insert, 1)

p.write_text(s, encoding="utf-8")
print("PATCH_OK position_bus.py DRY_RUN touch refresh")
