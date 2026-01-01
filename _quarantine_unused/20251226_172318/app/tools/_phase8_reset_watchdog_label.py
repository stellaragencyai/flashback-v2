import json
from pathlib import Path

lbl = "flashback01"
p = Path(r"state\orchestrator_watchdog.json")

d = json.loads(p.read_text(encoding="utf-8", errors="ignore")) if p.exists() else {}
labels = (d.get("labels") or {})
w = (labels.get(lbl) or {})

# Reset
w["restart_count"] = 0
w["backoff_sec"] = 2.0
w["blocked"] = False
w["blocked_reason"] = None
w["next_restart_allowed_ts_ms"] = 0
w["restart_history_ms"] = []
labels[lbl] = w
d["labels"] = labels

p.write_text(json.dumps(d, indent=2), encoding="utf-8")
print("OK: reset watchdog state for", lbl)
