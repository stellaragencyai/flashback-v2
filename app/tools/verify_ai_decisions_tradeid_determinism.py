from __future__ import annotations

import json
import hashlib
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple

IN_PATH = Path("state/ai_decisions.jsonl")
OUT_PATH = Path("state/ai_decisions.deduped.jsonl")
REPORT_PATH = Path("state/ai_decisions.dedup_report.json")

def _key(row: Dict[str, Any]) -> Tuple[str, str]:
    return (str(row.get("account_label") or ""), str(row.get("trade_id") or ""))

# Policy signature = what matters for determinism of learning.
# We intentionally IGNORE snapshot_fp + snapshot_* because those can vary while policy stays identical.
POLICY_SIGNATURE_FIELDS = [
    "account_label",
    "trade_id",
    "client_trade_id",
    "event_type",
    "decision",
    "decision_code",
    "allow",
    "size_multiplier",
    "symbol",
    "timeframe",
    "policy_hash",
    "schema_version",
]

# Snapshot signature = tracked for reporting only (not determinism failure).
SNAPSHOT_FIELDS = [
    "snapshot_fp",
    "snapshot_mode",
    "snapshot_schema_version",
    "ts_ms",
    "ts",
]

def _hash_obj(obj: Dict[str, Any]) -> str:
    raw = json.dumps(obj, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def _policy_sig(row: Dict[str, Any]) -> str:
    return _hash_obj({k: row.get(k) for k in POLICY_SIGNATURE_FIELDS})

def _snapshot_sig(row: Dict[str, Any]) -> str:
    return _hash_obj({k: row.get(k) for k in SNAPSHOT_FIELDS})

def _ts_ms(row: Dict[str, Any]) -> int:
    v = row.get("ts_ms", row.get("ts"))
    try:
        return int(v)
    except Exception:
        return 0

def _read_jsonl(path: Path):
    for line_no, line in enumerate(path.read_text(encoding="utf-8", errors="ignore").splitlines(), start=1):
        s = line.strip()
        if not s:
            continue
        try:
            obj = json.loads(s)
        except Exception:
            continue
        if isinstance(obj, dict):
            yield line_no, obj

def main() -> int:
    if not IN_PATH.exists():
        print(f"FAIL: missing {IN_PATH}")
        return 2

    buckets: Dict[Tuple[str, str], List[Tuple[int, Dict[str, Any]]]] = defaultdict(list)
    total = 0
    missing_key = 0

    for line_no, row in _read_jsonl(IN_PATH):
        total += 1
        k = _key(row)
        if (not k[0]) or (not k[1]):
            missing_key += 1
            continue
        buckets[k].append((line_no, row))

    dupe_keys = [k for k, rows in buckets.items() if len(rows) > 1]

    true_conflicts = []
    benign_snapshot_variants = []
    pure_dupes = []
    kept: Dict[Tuple[str, str], Tuple[int, Dict[str, Any]]] = {}

    for k, rows in buckets.items():
        if len(rows) == 1:
            kept[k] = rows[0]
            continue

        # group by POLICY signature first
        pol_groups: Dict[str, List[Tuple[int, Dict[str, Any]]]] = defaultdict(list)
        for ln, r in rows:
            pol_groups[_policy_sig(r)].append((ln, r))

        if len(pol_groups) == 1:
            # Policy identical across rows. Now check if they are pure duplicates or snapshot variants.
            # If snapshot differs, it's benign.
            snap_sigs = set(_snapshot_sig(r) for _, r in rows)
            latest = max(rows, key=lambda t: (_ts_ms(t[1]), t[0]))
            kept[k] = latest

            if len(snap_sigs) == 1:
                pure_dupes.append({"account_label": k[0], "trade_id": k[1], "count": len(rows), "kept_line": latest[0]})
            else:
                benign_snapshot_variants.append({
                    "account_label": k[0],
                    "trade_id": k[1],
                    "count": len(rows),
                    "kept_line": latest[0],
                    "unique_snapshots": len(snap_sigs),
                    "example_snapshot_fp": latest[1].get("snapshot_fp"),
                })
        else:
            # Policy differs within same (account_label, trade_id) -> TRUE conflict (hard fail)
            latest = max(rows, key=lambda t: (_ts_ms(t[1]), t[0]))
            kept[k] = latest
            ex = []
            for pol_sig, entries in list(pol_groups.items())[:3]:
                ln, r = entries[0]
                ex.append({
                    "line": ln,
                    "ts_ms": _ts_ms(r),
                    "allow": r.get("allow"),
                    "decision_code": r.get("decision_code"),
                    "size_multiplier": r.get("size_multiplier"),
                    "snapshot_fp": r.get("snapshot_fp"),
                })
            true_conflicts.append({
                "account_label": k[0],
                "trade_id": k[1],
                "count": len(rows),
                "unique_policy_signatures": len(pol_groups),
                "kept_line": latest[0],
                "examples": ex,
            })

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    ordered = sorted(kept.values(), key=lambda t: (_ts_ms(t[1]), t[0]))
    with OUT_PATH.open("w", encoding="utf-8", newline="\n") as f:
        for _, row in ordered:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    report = {
        "in_path": str(IN_PATH),
        "out_path": str(OUT_PATH),
        "rows_in": total,
        "missing_key_rows": missing_key,
        "unique_keys": len(buckets),
        "dupe_keys": len(dupe_keys),
        "pure_dupe_keys": len(pure_dupes),
        "benign_snapshot_variant_keys": len(benign_snapshot_variants),
        "true_conflict_keys": len(true_conflicts),
        "max_dupe_count": max((len(buckets[k]) for k in dupe_keys), default=1),
        "true_conflicts_sample": true_conflicts[:10],
        "benign_variants_sample": benign_snapshot_variants[:10],
    }
    REPORT_PATH.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    print("=== AI DECISIONS DETERMINISM REPORT (policy-based) ===")
    for k2 in [
        "rows_in","missing_key_rows","unique_keys","dupe_keys","pure_dupe_keys",
        "benign_snapshot_variant_keys","true_conflict_keys","max_dupe_count"
    ]:
        print(f"{k2}=", report[k2])
    print("deduped_out=", str(OUT_PATH))
    print("report_json=", str(REPORT_PATH))

    if report["true_conflict_keys"] == 0:
        print("PASS")
        return 0
    else:
        print("FAIL: true_conflict_keys > 0 (learning must NOT consume decisions yet)")
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
