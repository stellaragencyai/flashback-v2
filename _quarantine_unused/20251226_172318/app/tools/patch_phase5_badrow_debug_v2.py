from pathlib import Path
import shutil
import sys

p = Path(r"app\ai\ai_memory_entry_builder.py")
bak = Path(str(p) + ".bak_before_badrow_debug_print_v2")
if not bak.exists():
    shutil.copy2(p, bak)

s = p.read_text(encoding="utf-8", errors="ignore")

if "_debug_bad_row_once" in s:
    print("PATCH_SKIP: debug helper already present.")
    print("Backup:", bak)
    sys.exit(0)

insert_after = "import time\n"
helper = (
"\n"
"def _debug_bad_row_once(raw):\n"
"    try:\n"
"        if globals().get('_FB_BADROW_PRINTED', False):\n"
"            return\n"
"        globals()['_FB_BADROW_PRINTED'] = True\n"
"        if isinstance(raw, dict):\n"
"            import json\n"
"            et = str(raw.get('event_type') or '')\n"
"            tid = str(raw.get('trade_id') or '')\n"
"            preview = json.dumps(raw)[:500]\n"
"        else:\n"
"            et = ''\n"
"            tid = ''\n"
"            preview = str(raw)[:500]\n"
"        print('BAD_ROW_DEBUG:', {'event_type': et, 'trade_id': tid, 'preview': preview})\n"
"    except Exception:\n"
"        pass\n"
"\n"
)

if insert_after not in s:
    print("PATCH_FAIL: could not find import time block")
    print("Backup:", bak)
    sys.exit(1)

s = s.replace(insert_after, insert_after + helper, 1)

needle = "bad_rows += 1"
if needle not in s:
    print("PATCH_FAIL: could not find 'bad_rows += 1'")
    print("Backup:", bak)
    sys.exit(1)

# Replace first occurrence, keep indentation identical by not hardcoding spaces:
# We insert a call on its own line at the same indentation level as bad_rows += 1
lines = s.splitlines(True)
out = []
replaced = False
for line in lines:
    if (not replaced) and (line.lstrip() == "bad_rows += 1\n" or line.lstrip() == "bad_rows += 1\r\n"):
        indent = line[:len(line) - len(line.lstrip())]
        out.append(f"{indent}_debug_bad_row_once(raw)\n")
        out.append(line)
        replaced = True
    else:
        out.append(line)

if not replaced:
    print("PATCH_FAIL: could not replace the first bad_rows += 1 line safely")
    print("Backup:", bak)
    sys.exit(1)

p.write_text("".join(out), encoding="utf-8")
print("PATCH_OK: indentation-safe BAD_ROW_DEBUG installed.")
print("Backup:", bak)
