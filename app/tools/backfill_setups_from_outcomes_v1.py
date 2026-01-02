from __future__ import annotations
from pathlib import Path
import json, os, time
from typing import Any, Dict, Set

ROOT = Path(os.getenv("FLASHBACK_ROOT", Path.cwd()))
STATE = ROOT / "state"
SETUPS = STATE / "ai_events" / "setups.jsonl"
OUTCOMES = STATE / "ai_events" / "outcomes.v1.jsonl"
OUT_SYN = STATE / "ai_events" / "setups.synthetic_from_outcomes.jsonl"

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

def tid(o: Dict[str, Any]) -> str:
    v=o.get("trade_id")
    return v.strip() if isinstance(v,str) and v.strip() else ""

known: Set[str] = set()
for row in iter_jsonl(SETUPS):
    t=tid(row)
    if t: known.add(t)

made=0
buf=[]
now_ms=int(time.time()*1000)

for out in iter_jsonl(OUTCOMES):
    t=tid(out)
    if (not t) or (t in known):
        continue

    # minimal synthetic setup_context row
    row={
      "event_type":"setup_context",
      "ts": out.get("opened_ts_ms") or out.get("ts_ms") or now_ms,
      "trade_id": t,
      "account_label": out.get("account_label"),
      "symbol": out.get("symbol"),
      "timeframe": out.get("timeframe"),
      "setup_type": out.get("setup_type") or "unknown",
      "strategy": "SYNTH_FROM_OUTCOME",
      "policy": "synthetic",
      "ai_profile": None,
      "payload": {
        "synthetic": True,
        "source": "outcomes.v1.jsonl",
        "note": "Backfilled setup_context because trade_id missing in setups.jsonl",
      },
    }
    buf.append(json.dumps(row, separators=(",",":"), ensure_ascii=False))
    known.add(t)
    made += 1

OUT_SYN.parent.mkdir(parents=True, exist_ok=True)
OUT_SYN.write_text("\n".join(buf) + ("\n" if buf else ""), encoding="utf-8")

print("SYNTH_WRITTEN:", OUT_SYN)
print("SYNTH_ROWS:", made)
