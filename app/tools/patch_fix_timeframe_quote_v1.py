from pathlib import Path
import time

TARGET = Path(r"C:\flashback\app\bots\executor_v2.py")
if not TARGET.exists():
    raise SystemExit(f"ERROR: missing {TARGET}")

src = TARGET.read_text(encoding="utf-8", errors="replace")
bak = TARGET.with_suffix(f".py.bak_{int(time.time())}")
bak.write_text(src, encoding="utf-8")

# Fix the exact syntax error introduced by the previous patch
bad = '"timeframe": str(tf)",'
good = '"timeframe": str(tf),'

if bad not in src:
    print("WARN: bad pattern not found; no changes made")
else:
    src = src.replace(bad, good)
    TARGET.write_text(src, encoding="utf-8")
    print("OK: fixed unterminated string literal in executor_v2.py")
    print(f" - backup: {bak.name}")
