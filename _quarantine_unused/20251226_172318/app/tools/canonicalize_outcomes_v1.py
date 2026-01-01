from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Tuple

IN_PATH = Path(r"state\ai_events\outcomes.jsonl")
OUT_PATH = Path(r"state\ai_events\outcomes.v1.jsonl")
REJECT_PATH = Path(r"state\ai_events\outcomes.rejects.jsonl")

REQUIRED = [
    "schema_version",
    "trade_id",
    "sub_uid",
    "symbol",
    "side",
    "entry_ts_ms",
    "entry_px",
    "entry_qty",
    "exit_side",
    "exit_ts_ms",
    "exit_px",
    "exit_qty",
    "closed_ts_ms",
    "pnl_usd",
    "fees_usd",
]

def _read_jsonl(p: Path):
    if not p.exists():
        return
    for i, line in enumerate(p.read_text(encoding="utf-8", errors="ignore").splitlines(), start=1):
        s = line.strip()
        if not s:
            continue
        try:
            obj = json.loads(s)
        except Exception:
            continue
        if isinstance(obj, dict):
            yield i, obj

def _is_v1(row: Dict[str, Any]) -> bool:
    sv = str(row.get("schema_version") or "")
    return sv == "outcome.v1"

def _missing_required(row: Dict[str, Any]) -> Tuple[bool, list]:
    missing = [k for k in REQUIRED if row.get(k) in (None, "", [])]
    return (len(missing) > 0), missing

def main() -> int:
    if not IN_PATH.exists():
        print(f"FAIL: missing {IN_PATH}")
        return 2

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    total = 0
    kept = 0
    rejected = 0
    legacy_skipped = 0
    bad_json = 0

    with OUT_PATH.open("w", encoding="utf-8", newline="\n") as out_f, REJECT_PATH.open("w", encoding="utf-8", newline="\n") as rej_f:
        for line_no, row in _read_jsonl(IN_PATH):
            total += 1

            # Only canonicalize v1 rows; legacy rows are quarantined (not “fixed” here).
            if not _is_v1(row):
                legacy_skipped += 1
                rej_f.write(json.dumps({"reason":"legacy_schema","source":{"line_no":line_no},"row":row}, ensure_ascii=False) + "\n")
                rejected += 1
                continue

            is_bad, missing = _missing_required(row)
            if is_bad:
                rej_f.write(json.dumps({"reason":"missing_required","missing":missing,"source":{"line_no":line_no},"row":row}, ensure_ascii=False) + "\n")
                rejected += 1
                continue

            out_f.write(json.dumps(row, ensure_ascii=False) + "\n")
            kept += 1

    print("=== OUTCOME V1 CANONICALIZE ===")
    print("in_path=", str(IN_PATH))
    print("out_path=", str(OUT_PATH))
    print("reject_path=", str(REJECT_PATH))
    print("rows_in=", total)
    print("rows_kept_v1=", kept)
    print("rows_rejected=", rejected)
    print("legacy_skipped=", legacy_skipped)
    print("PASS" if kept > 0 else "WARN: no v1 rows kept")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
