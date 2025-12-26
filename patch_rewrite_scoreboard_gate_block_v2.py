from __future__ import annotations

from pathlib import Path
import time

P = Path(r"C:\flashback\app\bots\executor_v2.py")

# Read bytes so we can normalize BOM safely
raw = P.read_bytes()
BOM = b"\xef\xbb\xbf"
if raw.startswith(BOM):
    # Do NOT write here. Just normalize for parsing/patching.
    raw = raw[len(BOM):]

txt = raw.decode("utf-8", errors="replace")

start_marker = "    scoreboard_gate = None\n    if use_scoreboard_gate:"
end_marker = "\n    if scoreboard_gate is not None:"

start = txt.find(start_marker)
if start == -1:
    raise SystemExit("ERROR: Could not find scoreboard gate start block (scoreboard_gate/use_scoreboard_gate). No changes made.")

end = txt.find(end_marker, start)
if end == -1:
    raise SystemExit("ERROR: Could not find scoreboard gate end block (if scoreboard_gate is not None:). No changes made.")

replacement = """    scoreboard_gate = None
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

txt2 = txt[:start] + replacement + txt[end:]

# Full-file compile check BEFORE writing
compile(txt2, str(P), "exec")

bak = P.with_suffix(".py.bak_" + str(int(time.time())))
bak.write_text(txt, encoding="utf-8")

# Write clean UTF-8 (NO BOM) permanently
P.write_text(txt2, encoding="utf-8", newline="\n")

print("OK: rewrote scoreboard gate block + full compile verified + wrote UTF-8 without BOM")
print(" - backup:", bak.name)
