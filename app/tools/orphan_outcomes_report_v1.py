from __future__ import annotations
from pathlib import Path
import json
import os
from typing import Any, Dict, List, Tuple

def _iter_jsonl(path: Path):
    if not path.exists():
        return
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line=line.strip()
            if not line:
                continue
            try:
                obj=json.loads(line)
            except Exception:
                continue
            if isinstance(obj, dict):
                yield obj

def _find_first(paths: List[Path], patterns: List[str]) -> List[Path]:
    out=[]
    for p in paths:
        if not p.exists():
            continue
        for f in p.rglob("*"):
            if not f.is_file():
                continue
            name=f.name.lower()
            if any(pat in name for pat in patterns) and name.endswith(".jsonl"):
                out.append(f)
    return sorted(out, key=lambda x: x.stat().st_mtime, reverse=True)

ROOT = Path(os.getenv("FLASHBACK_ROOT", Path.cwd()))
STATE = ROOT / "state"

outcome_files = _find_first([STATE], ["outcome", "outcomes"])
# Prefer v1 if present
outcome_files_sorted = sorted(outcome_files, key=lambda p: (("v1" not in p.name.lower()), -p.stat().st_mtime))
outcome_path = outcome_files_sorted[0] if outcome_files_sorted else None

# Context/decision candidates (best effort)
ctx_files = _find_first([STATE], ["context", "setup_context", "setup"])
dec_files = _find_first([STATE], ["decision", "decisions"])

print("=== ORPHAN OUTCOMES REPORT v1 ===")
print("ROOT:", ROOT)
print("STATE:", STATE)
print("OUTCOME_FILE:", str(outcome_path) if outcome_path else "NONE")
print("CTX_FILES:", len(ctx_files))
print("DEC_FILES:", len(dec_files))

if not outcome_path:
    raise SystemExit("STOP: No outcomes file found under .\\state")

# Build known trade_id sets (best effort)
known_trade_ids=set()

def _extract_trade_id(obj: Dict[str, Any]) -> str:
    for k in ("trade_id","effective_trade_id","id"):
        v=obj.get(k)
        if isinstance(v,str) and v.strip():
            return v.strip()
    return ""

for fp in ctx_files[:5]:
    for o in _iter_jsonl(fp):
        tid=_extract_trade_id(o)
        if tid:
            known_trade_ids.add(tid)

for fp in dec_files[:5]:
    for o in _iter_jsonl(fp):
        tid=_extract_trade_id(o)
        if tid:
            known_trade_ids.add(tid)

total=0
orphans=[]
missing_trade_id=0
missing_label=0
missing_setup=0
unjoinable=0

for row in _iter_jsonl(outcome_path):
    total += 1
    tid = row.get("trade_id")
    label = row.get("account_label") or row.get("label")
    setup = row.get("setup_type") or row.get("setup") or row.get("strategy")

    reasons=[]
    if not (isinstance(tid,str) and tid.strip()):
        missing_trade_id += 1
        reasons.append("MISSING_TRADE_ID")
        tid=""
    else:
        tid=tid.strip()

    if not (isinstance(label,str) and label.strip()):
        missing_label += 1
        reasons.append("MISSING_ACCOUNT_LABEL")

    if not (isinstance(setup,str) and setup.strip()):
        missing_setup += 1
        reasons.append("MISSING_SETUP_TYPE")

    if tid and known_trade_ids and (tid not in known_trade_ids):
        unjoinable += 1
        reasons.append("TRADE_ID_NOT_FOUND_IN_CTX_DEC")

    if reasons:
        orphans.append({
            "trade_id": tid,
            "account_label": label,
            "setup_type": setup,
            "reasons": reasons,
            "sample": {k: row.get(k) for k in ("schema_version","ts_ms","symbol","pnl_usd","close_reason","entry_ts_ms","exit_ts_ms") if k in row}
        })

report = {
    "outcome_file": str(outcome_path),
    "total_outcomes": total,
    "orphans_total": len(orphans),
    "missing_trade_id": missing_trade_id,
    "missing_account_label": missing_label,
    "missing_setup_type": missing_setup,
    "unjoinable_trade_id": unjoinable,
    "known_trade_ids_loaded": len(known_trade_ids),
    "orphans_sample": orphans[:50],
}

out_dir = STATE / "reports"
out_dir.mkdir(parents=True, exist_ok=True)
out_path = out_dir / "orphan_outcomes_report.v1.json"
out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

print("REPORT_WRITTEN:", out_path)
print("TOTAL_OUTCOMES:", total)
print("ORPHANS:", len(orphans))
print("MISSING_TRADE_ID:", missing_trade_id)
print("MISSING_ACCOUNT_LABEL:", missing_label)
print("MISSING_SETUP_TYPE:", missing_setup)
print("UNJOINABLE_TRADE_ID:", unjoinable)
