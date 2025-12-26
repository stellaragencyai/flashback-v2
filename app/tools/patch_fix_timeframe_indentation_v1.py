from pathlib import Path
import time

TARGET = Path(r"C:\flashback\app\bots\executor_v2.py")
if not TARGET.exists():
    raise SystemExit(f"ERROR: missing {TARGET}")

src = TARGET.read_text(encoding="utf-8", errors="replace")
bak = TARGET.with_suffix(f".py.bak_{int(time.time())}")
bak.write_text(src, encoding="utf-8")

lines = src.splitlines()
out = []
i = 0
fixed = False

while i < len(lines):
    line = lines[i]

    # Detect the exact except block that caused the issue
    if line.strip() == "except Exception as e:":
        out.append(line)
        i += 1

        # If the next line is our injected timeframe line but not indented, fix it
        if i < len(lines) and "timeframe = str(tf)" in lines[i] and not lines[i].startswith(" " * 8):
            out.append(" " * 8 + "timeframe = str(tf)  # patch: scoreboard gate expects timeframe; use normalized tf")
            fixed = True
            i += 1
            continue

    out.append(line)
    i += 1

TARGET.write_text("\n".join(out), encoding="utf-8")

if fixed:
    print("OK: fixed indentation of timeframe assignment inside except block")
    print(f" - backup: {bak.name}")
else:
    print("WARN: no indentation fix applied (pattern not found)")
