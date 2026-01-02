from __future__ import annotations
from pathlib import Path
import json, os
from typing import Any, Dict, List, Set

ROOT = Path(os.getenv("FLASHBACK_ROOT", Path.cwd()))
STATE = ROOT / "state"
SETUPS = STATE / "ai_events" / "setups.jsonl"
OUTCOMES = STATE / "ai_events" / "outcomes.v1.jsonl"

def iter_jsonl(path: Path):
    if not path.exists(): return
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

def get_tid(o: Dict[str, Any]) -> str:
    v=o.get("trade_id")
    return v.strip() if isinstance(v,str) and v.strip() else ""

print("=== ORPHAN OUTCOMES REPORT v3 (trade_id join vs setups.jsonl) ===")
print("ROOT:", ROOT)
print("SETUPS:", SETUPS, "EXISTS=", SETUPS.exists(), "SIZE=", (SETUPS.stat().st_size if SETUPS.exists() else 0))
print("OUTCOMES:", OUTCOMES, "EXISTS=", OUTCOMES.exists(), "SIZE=", (OUTCOMES.stat().st_size if OUTCOMES.exists() else 0))
if not SETUPS.exists(): raise SystemExit("STOP: setups.jsonl missing")
if not OUTCOMES.exists(): raise SystemExit("STOP: outcomes.v1.jsonl missing")

known: Set[str] = set()
setup_rows=0
for row in iter_jsonl(SETUPS):
    setup_rows += 1
    tid=get_tid(row)
    if tid: known.add(tid)

total=0
unjoinable=0
orphans=[]
for row in iter_jsonl(OUTCOMES):
    total += 1
    tid=get_tid(row)
    if not tid:
        orphans.append({"trade_id":"","reason":"MISSING_TRADE_ID","sample":{k:row.get(k) for k in ("account_label","symbol","setup_type","ts_ms")}})
        continue
    if tid not in known:
        unjoinable += 1
        orphans.append({"trade_id":tid,"reason":"NO_SETUP_CONTEXT","sample":{k:row.get(k) for k in ("account_label","symbol","setup_type","timeframe","ts_ms")}})

out_dir = STATE / "reports"
out_dir.mkdir(parents=True, exist_ok=True)
out_path = out_dir / "orphan_outcomes_report.v3.json"
report = {
  "setup_rows": setup_rows,
  "known_trade_ids": len(known),
  "outcomes_total": total,
  "orphans_total": len(orphans),
  "unjoinable_trade_id": unjoinable,
  "orphans_sample": orphans[:50],
}
out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

print("REPORT_WRITTEN:", out_path)
print("SETUP_ROWS:", setup_rows)
print("KNOWN_TRADE_IDS:", len(known))
print("OUTCOMES_TOTAL:", total)
print("ORPHANS_TOTAL:", len(orphans))
print("UNJOINABLE_TRADE_ID:", unjoinable)
