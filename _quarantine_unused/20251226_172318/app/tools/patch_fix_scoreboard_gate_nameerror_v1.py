from __future__ import annotations

import time
from pathlib import Path

TARGET = Path(r"C:\flashback\app\bots\executor_v2.py")

def main() -> None:
    if not TARGET.exists():
        print(f"ERROR: missing {TARGET}")
        return

    src = TARGET.read_text(encoding="utf-8", errors="replace")
    bak = TARGET.with_suffix(f".py.bak_{int(time.time())}")
    bak.write_text(src, encoding="utf-8")

    n1 = src.count("setup_type=str(setup_type)")
    n2 = src.count("bound_log.warning(")

    out = src.replace("setup_type=str(setup_type)", "setup_type=str(setup_type_raw)")
    out = out.replace("bound_log.warning(", "log.warning(")

    TARGET.write_text(out, encoding="utf-8")

    print("OK: patched executor_v2.py")
    print(f" - backups: {bak.name}")
    print(f" - replaced setup_type refs: {n1}")
    print(f" - replaced bound_log.warning refs: {n2}")

if __name__ == "__main__":
    main()
