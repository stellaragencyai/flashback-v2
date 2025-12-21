from pathlib import Path
import shutil
import sys

p = Path(r"app\ai\ai_memory_entry_builder.py")
bak = Path(str(p) + ".bak_before_outcome_record_tsms_fallback")
if not bak.exists():
    shutil.copy2(p, bak)

s = p.read_text(encoding="utf-8", errors="ignore")

old = '        "ts_ms": raw.get("ts_ms"),\n'
new = '        "ts_ms": (raw.get("ts_ms") or raw.get("ts") or setup.get("ts_ms") or setup.get("ts")),\n'

n = s.count(old)
if n != 1:
    print("PATCH_FAIL: expected 1 match for ts_ms line, found", n)
    print("Backup:", bak)
    sys.exit(1)

p.write_text(s.replace(old, new), encoding="utf-8")
print("PATCH_OK: outcome_record enrichment now falls back ts->ts_ms.")
print("Backup:", bak)
