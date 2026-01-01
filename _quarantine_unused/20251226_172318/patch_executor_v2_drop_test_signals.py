from __future__ import annotations

from pathlib import Path
import time
import re

P = Path(r"C:\flashback\app\bots\executor_v2.py")
txt = P.read_text(encoding="utf-8", errors="replace")

# Insert right after: "sig = json.loads(line)" block (after the JSON parse try/except)
marker = '    try:\n        sig = json.loads(line)\n    except Exception:\n        log.warning("Invalid JSON in observed.jsonl: %r", line[:200])\n        return\n\n'
i = txt.find(marker)
if i == -1:
    raise SystemExit("ERROR: Could not find process_signal_line JSON parse block. No changes made.")

insert = marker + (
"    # --- HARD FILTER: drop test/junk signals permanently ---\n"
"    try:\n"
"        src = sig.get('source')\n"
"        if src == 'emit_test_signal':\n"
"            return\n"
"        st = sig.get('setup_type') or sig.get('setup_type_raw') or sig.get('reason')\n"
"        if isinstance(st, str) and st.strip().lower() == 'tick':\n"
"            return\n"
"    except Exception:\n"
"        # never crash ingestion over filtering\n"
"        pass\n\n"
)

txt2 = txt[:i] + insert + txt[i+len(marker):]

# Full-file compile check BEFORE writing
try:
    compile(txt2, str(P), "exec")
except Exception as e:
    print("ERROR: executor_v2.py would not compile after patch. No changes written.")
    print("Compile error:", repr(e))
    raise

bak = P.with_suffix(".py.bak_" + str(int(time.time())))
bak.write_text(txt, encoding="utf-8")
P.write_text(txt2, encoding="utf-8", newline="\n")

print("OK: executor_v2 patched to permanently drop emit_test_signal + tick")
print(" - backup:", bak.name)
