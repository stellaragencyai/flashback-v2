from pathlib import Path
import time

P = Path(r"C:\Flashback\app\bots\executor_v2.py")
txt = P.read_text(encoding="utf-8", errors="replace")

before = txt

# 1) mm_spread_capture should NOT be "scalp"
txt = txt.replace('return "scalp", "substring:mm_spread_capture"', 'return "market_make", "substring:mm_spread_capture"')

# 2) pump_chase_momo should be separated from classic breakout
txt = txt.replace('return "breakout", "substring:pump_chase_momo"', 'return "momentum", "substring:pump_chase_momo"')

if txt == before:
    raise SystemExit("ERROR: No changes made. Patterns not found (file changed?).")

# compile check before writing
try:
    compile(txt, str(P), "exec")
except Exception as e:
    print("ERROR: Modified executor_v2.py does not compile. No changes written.")
    print("Compile error:", repr(e))
    raise

bak = P.with_suffix(".py.bak_" + str(int(time.time())))
bak.write_text(before, encoding="utf-8", newline="\n")
P.write_text(txt, encoding="utf-8", newline="\n")

print("OK: patched setup taxonomy")
print(" - backup:", bak.name)
