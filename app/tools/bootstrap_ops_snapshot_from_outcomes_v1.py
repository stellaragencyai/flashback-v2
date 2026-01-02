from __future__ import annotations
import json, time
from pathlib import Path

STATE = Path("state")
OUT = STATE / "ops_snapshot.json"
OUTCOMES = STATE / "ai_events" / "outcomes.v1.jsonl"

def iter_jsonl(p: Path):
    if not p.exists(): return
    with p.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            try:
                o=json.loads(line)
            except Exception:
                continue
            if isinstance(o, dict):
                yield o

def main():
    accounts={}
    for row in iter_jsonl(OUTCOMES):
        a=row.get("account_label")
        if not isinstance(a,str) or not a.strip(): continue
        a=a.strip()
        pnl=float(row.get("pnl_usd") or 0.0)
        acct=accounts.get(a) or {"trades":{"total":0,"wins":0,"losses":0,"open_trade":False},
                                "performance":{"avg_return_pct":0.0,"cumulative_return_pct":0.0},
                                "ai":{"confidence":0.0,"buckets":0,"regime":"unknown","ml_ready":False},
                                "risk":{"state":"ok","error_count":0,"last_error":None}}
        acct["trades"]["total"] += 1
        if pnl > 0: acct["trades"]["wins"] += 1
        elif pnl < 0: acct["trades"]["losses"] += 1
        accounts[a]=acct

    snap={"ts_ms":int(time.time()*1000),"accounts":accounts,"schema_version":1,"source":"outcomes_bootstrap"}
    STATE.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(snap, indent=2), encoding="utf-8")
    print("OK: wrote", OUT, "accounts=", len(accounts))

if __name__=="__main__":
    main()
