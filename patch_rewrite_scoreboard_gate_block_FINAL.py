from __future__ import annotations

from pathlib import Path
import time

P = Path(r"C:\flashback\app\bots\executor_v2.py")
txt = P.read_text(encoding="utf-8", errors="replace")

start_marker = "    scoreboard_gate = None\n    if use_scoreboard_gate:"
end_marker = "\n    if scoreboard_gate is not None:"

start = txt.find(start_marker)
if start == -1:
    raise SystemExit("ERROR: Could not find scoreboard gate start block. No changes made.")

end = txt.find(end_marker, start)
if end == -1:
    raise SystemExit("ERROR: Could not find scoreboard gate end marker. No changes made.")

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
try:
    compile(txt2, str(P), "exec")
except Exception as e:
    print("ERROR: Modified executor_v2.py still does not compile. No changes written.")
    print("Compile error:", repr(e))
    # Print a small excerpt around the failing line if available
    if hasattr(e, "lineno") and e.lineno:
        lines = txt2.splitlines()
        lo = max(0, int(e.lineno) - 6)
        hi = min(len(lines), int(e.lineno) + 5)
        print("\n--- excerpt ---")
        for i in range(lo, hi):
            print(f"{i+1:5d}: {lines[i]}")
        print("--------------")
    raise

bak = P.with_suffix(".py.bak_" + str(int(time.time())))
bak.write_text(txt, encoding="utf-8")

P.write_text(txt2, encoding="utf-8", newline="\n")

print("OK: rewrote scoreboard gate block + compile-verified whole file")
print(" - backup:", bak.name)
