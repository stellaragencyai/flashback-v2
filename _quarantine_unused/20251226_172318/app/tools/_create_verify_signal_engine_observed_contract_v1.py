from pathlib import Path

p = Path(r"app\tools\verify_signal_engine_observed_contract.py")
if p.exists():
    raise SystemExit("REFUSE: verify_signal_engine_observed_contract.py already exists")

code = r"""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path


OBSERVED = Path(r"C:\Flashback\signals\observed.jsonl")


def main() -> None:
    if not OBSERVED.exists():
        raise SystemExit("FAIL: missing observed.jsonl")

    total = 0
    bad_debug = 0
    c = Counter()

    for line in OBSERVED.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
        except Exception:
            continue

        total += 1

        # Uniqueness key (same one you tested manually)
        k = (
            r.get("sub_uid"),
            r.get("symbol"),
            r.get("timeframe"),
            r.get("ts_ms"),
            r.get("side"),
            r.get("reason"),
        )
        c[k] += 1

        dbg = r.get("debug") or {}
        if dbg.get("last_close") is None or dbg.get("prev_close") is None or dbg.get("ma") is None:
            bad_debug += 1

    dupes = [(k, v) for k, v in c.items() if v > 1]
    dupe_keys = len(dupes)
    max_dupe = max([v for _, v in dupes], default=1)

    print("=== OBSERVED CONTRACT CHECK ===")
    print("path=", str(OBSERVED))
    print("rows=", total)
    print("bad_debug_rows=", bad_debug)
    print("unique_keys=", len(c))
    print("dupe_keys=", dupe_keys)
    print("max_dupe_count=", max_dupe)

    if total == 0:
        raise SystemExit("FAIL: observed.jsonl has 0 parsed rows")
    if bad_debug > 0:
        raise SystemExit("FAIL: debug contains nulls")
    if dupe_keys > 0:
        raise SystemExit("FAIL: duplicate observed rows detected")

    print("PASS")


if __name__ == "__main__":
    main()
"""
p.write_text(code.strip() + "\n", encoding="utf-8", newline="\n")
print("OK: created verify_signal_engine_observed_contract.py")
