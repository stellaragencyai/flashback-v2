from pathlib import Path
import shutil
import sys

p = Path(r"app\ai\ai_memory_entry_builder.py")
bak = Path(str(p) + ".bak_before_outcome_record_defaults")
if not bak.exists():
    shutil.copy2(p, bak)

s = p.read_text(encoding="utf-8", errors="ignore")

# We patch inside _try_enrich_outcome_record by replacing its "payload = ..." block
# with a safer version that:
# - supports payload-only rows
# - defaults symbol/timeframe when missing

needle = '    payload = raw.get("payload") if isinstance(raw.get("payload"), dict) else {}\n'
if needle not in s:
    print("PATCH_FAIL: could not find expected payload assignment inside _try_enrich_outcome_record")
    print("Backup:", bak)
    sys.exit(1)

replacement = (
'    payload = raw.get("payload") if isinstance(raw.get("payload"), dict) else {}\n'
'    # Legacy outcome_record rows may be payload-only and lack top-level context.\n'
'    # Default missing context so the row can be ingested and later enriched from setups.\n'
'    if not isinstance(raw, dict):\n'
'        return None\n'
'    if not raw.get("symbol"):\n'
'        raw["symbol"] = payload.get("symbol") or "UNKNOWN"\n'
'    if not raw.get("timeframe"):\n'
'        raw["timeframe"] = payload.get("timeframe") or "unknown"\n'
)

s2 = s.replace(needle, replacement, 1)
p.write_text(s2, encoding="utf-8")

print("PATCH_OK: _try_enrich_outcome_record now defaults missing symbol/timeframe for legacy payload-only rows.")
print("Backup:", bak)
