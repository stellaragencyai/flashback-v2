from __future__ import annotations

from pathlib import Path
import time
import re

P = Path(r"C:\flashback\app\bots\executor_v2.py")
txt = P.read_text(encoding="utf-8", errors="replace")

# We insert a log line right after scoreboard_gate_decide() returns successfully.
# Target pattern: scoreboard_gate = scoreboard_gate_decide(...)
pattern = r"(scoreboard_gate\s*=\s*scoreboard_gate_decide\([\s\S]*?\)\s*)"
m = re.search(pattern, txt)
if not m:
    raise SystemExit("ERROR: Could not find scoreboard_gate_decide(...) call to patch.")

insert = m.group(1) + """
            try:
                if scoreboard_gate is not None:
                    bound.info(
                        "✅ Scoreboard gate MATCH trade_id=%s bucket=%s code=%s sm=%s reason=%s",
                        client_trade_id,
                        scoreboard_gate.get("bucket_key"),
                        scoreboard_gate.get("decision_code"),
                        scoreboard_gate.get("size_multiplier"),
                        scoreboard_gate.get("reason"),
                    )
            except Exception:
                pass
"""

txt2 = txt[:m.start(1)] + insert + txt[m.end(1):]

# Compile-check BEFORE writing
try:
    compile(txt2, str(P), "exec")
except Exception as e:
    print("ERROR: Modified executor_v2.py does not compile. No changes written.")
    print("Compile error:", repr(e))
    raise

bak = P.with_suffix(".py.bak_" + str(int(time.time())))
bak.write_text(txt, encoding="utf-8")
P.write_text(txt2, encoding="utf-8", newline="\n")

print("OK: added scoreboard gate MATCH logging")
print(" - backup:", bak.name)
