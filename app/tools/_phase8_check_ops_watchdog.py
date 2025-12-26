import json
from pathlib import Path

ops=json.loads(Path(r"state\ops_snapshot.json").read_text(encoding="utf-8", errors="ignore"))
a=(ops.get("accounts") or {})

for lbl in ["flashback01","flashback02","flashback07"]:
    acc=(a.get(lbl) or {})
    print(lbl, "watchdog=", acc.get("watchdog"))
