from __future__ import annotations

import json
import time
from pathlib import Path

IN_PATH = Path(r"C:\flashback\signals\observed.jsonl")

REQUIRED_ANY = [
    ("symbol",),
    ("timeframe","tf"),
    ("setup_type",),
    ("side",),
]

def has_required(d: dict) -> bool:
    for keys in REQUIRED_ANY:
        ok = False
        for k in keys:
            if k in d and d[k] not in (None, "", "null"):
                ok = True
                break
        if not ok:
            return False
    return True

def main() -> int:
    if not IN_PATH.exists():
        print(f"ERROR: missing {IN_PATH}")
        return 2

    raw_lines = IN_PATH.read_text(encoding="utf-8", errors="replace").splitlines()

    kept = []
    dropped = 0
    dropped_tick = 0
    dropped_test = 0
    dropped_bad = 0

    for line in raw_lines:
        s = line.strip()
        if not s:
            continue
        try:
            row = json.loads(s)
        except Exception:
            dropped += 1
            dropped_bad += 1
            continue

        setup_type = str(row.get("setup_type","")).strip().lower()
        source = str(row.get("source","")).strip().lower()

        if setup_type == "tick":
            dropped += 1
            dropped_tick += 1
            continue

        if source == "emit_test_signal":
            dropped += 1
            dropped_test += 1
            continue

        if not isinstance(row, dict) or not has_required(row):
            dropped += 1
            dropped_bad += 1
            continue

        kept.append(json.dumps(row, ensure_ascii=False))

    ts = int(time.time())
    bak = IN_PATH.with_suffix(f".jsonl.bak_sanitize_{ts}")
    bak.write_text("\n".join(raw_lines) + "\n", encoding="utf-8")

    IN_PATH.write_text("\n".join(kept) + ("\n" if kept else ""), encoding="utf-8", newline="\n")

    print("OK: sanitized observed.jsonl")
    print(f" - backup: {bak.name}")
    print(f" - kept: {len(kept)}")
    print(f" - dropped_total: {dropped}")
    print(f"   - dropped_tick: {dropped_tick}")
    print(f"   - dropped_emit_test_signal: {dropped_test}")
    print(f"   - dropped_bad_rows: {dropped_bad}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
