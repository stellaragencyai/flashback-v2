from pathlib import Path
import shutil
import sys

p = Path(r"app\ai\ai_memory_entry_builder.py")
bak = Path(str(p) + ".bak_before_timeframe_unknown_default")
if not bak.exists():
    shutil.copy2(p, bak)

s = p.read_text(encoding="utf-8", errors="ignore")

old = (
'        "timeframe": (\n'
'            raw.get("timeframe")\n'
'            or setup.get("timeframe")\n'
'            or (raw.get("payload") if isinstance(raw.get("payload"), dict) else {}).get("timeframe")\n'
'            or (setup.get("payload") if isinstance(setup.get("payload"), dict) else {}).get("timeframe")\n'
'        ),\n'
)

new = (
'        "timeframe": (\n'
'            raw.get("timeframe")\n'
'            or setup.get("timeframe")\n'
'            or (raw.get("payload") if isinstance(raw.get("payload"), dict) else {}).get("timeframe")\n'
'            or (setup.get("payload") if isinstance(setup.get("payload"), dict) else {}).get("timeframe")\n'
'            or "unknown"\n'
'        ),\n'
)

n = s.count(old)
if n != 1:
    print("PATCH_FAIL: expected 1 match for timeframe block, found", n)
    print("Backup:", bak)
    sys.exit(1)

p.write_text(s.replace(old, new), encoding="utf-8")
print("PATCH_OK: timeframe now defaults to 'unknown' instead of None.")
print("Backup:", bak)
