from pathlib import Path
import shutil
import sys

p = Path(r"app\ai\ai_memory_entry_builder.py")
bak = Path(str(p) + ".bak_before_legacy_outcome_record_payload_wrap")
if not bak.exists():
    shutil.copy2(p, bak)

s = p.read_text(encoding="utf-8", errors="ignore")

# We want to replace the simple line:
# outcome = raw.get("outcome") if isinstance(raw.get("outcome"), dict) else {}
# with a legacy-aware block that wraps raw["payload"] into outcome["payload"]

old = '    outcome = raw.get("outcome") if isinstance(raw.get("outcome"), dict) else {}\n'

new = (
'    # outcome_record legacy rows may store data under "payload" instead of "outcome"\n'
'    outcome: Dict[str, Any]\n'
'    if isinstance(raw.get("outcome"), dict):\n'
'        outcome = raw.get("outcome") or {}\n'
'    elif isinstance(raw.get("payload"), dict):\n'
'        outcome = {"payload": raw.get("payload")}\n'
'    else:\n'
'        outcome = {}\n'
)

n = s.count(old)
if n != 1:
    print("PATCH_FAIL: expected 1 match for outcome assignment, found", n)
    print("Backup:", bak)
    sys.exit(1)

p.write_text(s.replace(old, new), encoding="utf-8")
print("PATCH_OK: legacy outcome_record payload wrapper added.")
print("Backup:", bak)
