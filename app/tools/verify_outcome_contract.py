from __future__ import annotations

import json
from pathlib import Path

from app.ai.outcome_contract import assert_outcome_row_ok

PATH = Path(r"state\ai_events\outcomes.v1.jsonl")

def main() -> int:
    if not PATH.exists():
        print(f"FAIL: missing {PATH}")
        return 2

    rows = 0
    bad = 0

    for i, line in enumerate(PATH.read_text(encoding="utf-8", errors="ignore").splitlines(), start=1):
        s = line.strip()
        if not s:
            continue
        try:
            obj = json.loads(s)
        except Exception as e:
            bad += 1
            print(f"BAD row#{i}: JSONDecodeError: {e}")
            continue

        rows += 1
        try:
            assert_outcome_row_ok(obj)
        except Exception as e:
            bad += 1
            print(f"BAD row#{i}: {type(e).__name__}: {e}")

    print("=== OUTCOME CONTRACT CHECK (v1) ===")
    print("path=", str(PATH))
    print("rows=", rows)
    print("PASS" if bad == 0 else f"FAIL bad_rows={bad}")

    return 0 if bad == 0 else 1

if __name__ == "__main__":
    raise SystemExit(main())
