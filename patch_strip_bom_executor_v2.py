from __future__ import annotations

from pathlib import Path
import time

P = Path(r"C:\flashback\app\bots\executor_v2.py")

raw = P.read_bytes()

# UTF-8 BOM = EF BB BF
BOM = b"\xef\xbb\xbf"

if raw.startswith(BOM):
    bak = P.with_suffix(".py.bak_bom_" + str(int(time.time())))
    bak.write_bytes(raw)
    raw2 = raw[len(BOM):]
    P.write_bytes(raw2)
    print("OK: removed UTF-8 BOM from executor_v2.py")
    print(" - backup:", bak.name)
else:
    print("OK: no UTF-8 BOM found; no changes needed")
