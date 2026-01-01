from pathlib import Path
import shutil
import sys

p = Path(r"app\ai\ai_memory_entry_builder.py")
bak = Path(str(p) + ".bak_before_badrow_debug_print")
if not bak.exists():
    shutil.copy2(p, bak)

s = p.read_text(encoding="utf-8", errors="ignore")

# Find the first occurrence of: bad_rows += 1
needle = "bad_rows += 1"
pos = s.find(needle)
if pos == -1:
    print("PATCH_FAIL: could not find 'bad_rows += 1'")
    print("Backup:", bak)
    sys.exit(1)

# Insert debug just BEFORE the first bad_rows += 1
# We assume there is a local 'raw' dict and an 'idx' or line counter nearby.
inject = (
'        try:\n'
'            if not globals().get("_FB_BADROW_PRINTED", False):\n'
'                globals()["_FB_BADROW_PRINTED"] = True\n'
'                _et = str(raw.get("event_type") or "") if isinstance(raw, dict) else ""\n'
'                _tid = str(raw.get("trade_id") or "") if isinstance(raw, dict) else ""\n'
'                import json as _json\n'
'                _preview = _json.dumps(raw)[:500] if isinstance(raw, dict) else str(raw)[:500]\n'
'                print("BAD_ROW_DEBUG:", {"event_type": _et, "trade_id": _tid, "preview": _preview})\n'
'        except Exception:\n'
'            pass\n'
)

# Make sure we don't double-inject if already present
if "BAD_ROW_DEBUG:" in s:
    print("PATCH_SKIP: BAD_ROW_DEBUG already present.")
    print("Backup:", bak)
    sys.exit(0)

s2 = s.replace(needle, inject + "        " + needle, 1)
p.write_text(s2, encoding="utf-8")
print("PATCH_OK: Added one-time BAD_ROW_DEBUG print.")
print("Backup:", bak)
