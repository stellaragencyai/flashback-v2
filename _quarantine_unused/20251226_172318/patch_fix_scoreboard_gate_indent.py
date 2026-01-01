from __future__ import annotations

from pathlib import Path
import re
import time

P = Path(r"C:\flashback\app\bots\executor_v2.py")
txt = P.read_text(encoding="utf-8", errors="replace")

old = r"""
    scoreboard_gate = None
    if use_scoreboard_gate:
        try:
            scoreboard_gate = scoreboard_gate_decide(
                setup_type=str\(setup_type_raw\),
                timeframe=str\(tf\),
                symbol=str\(symbol\),
                account_label=str\(account_label\) if account_label is not None else None,
            )
        except Exception as e:
            pass  # auto-fix: empty except block
        timeframe = str\(tf\)  # patch: scoreboard gate expects timeframe; use normalized tf

            log\.warning\("Scoreboard gate failed \(non-fatal\): %r", e\)
            scoreboard_gate = None
"""

new = r"""
    scoreboard_gate = None
    if use_scoreboard_gate:
        try:
            scoreboard_gate = scoreboard_gate_decide(
                setup_type=str(setup_type_raw),
                timeframe=str(tf),
                symbol=str(symbol),
                account_label=str(account_label) if account_label is not None else None,
            )
        except Exception as e:
            log.warning("Scoreboard gate failed (non-fatal): %r", e)
            scoreboard_gate = None
"""

pattern = re.compile(old, re.VERBOSE)

m = pattern.search(txt)
if not m:
    raise SystemExit("ERROR: Could not find the exact broken scoreboard gate block to replace. No changes made.")

txt2 = txt[:m.start()] + new + txt[m.end():]

bak = P.with_suffix(".py.bak_" + str(int(time.time())))
bak.write_text(txt, encoding="utf-8")
P.write_text(txt2, encoding="utf-8")

print("OK: fixed scoreboard gate indentation + removed stray timeframe line")
print(" - backup:", bak.name)
