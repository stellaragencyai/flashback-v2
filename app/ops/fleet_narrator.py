from pathlib import Path
import json, time, sys
from app.ops.ops_state import load_ops_snapshot
from app.core.flashback_common import send_tg
from app.ops.alert_rules import SEVERITY, ESCALATION, EMOJI

STATE = Path("C:/flashback/state")
LAST = STATE / "fleet_last_snapshot.json"
COUNTERS = STATE / "fleet_counters.json"

def load(p):
    if p.exists():
        try: return json.loads(p.read_text())
        except Exception: pass
    return {}

def save(p, d):
    p.write_text(json.dumps(d, indent=2))

def narrate():
    ops = load_ops_snapshot()
    comps = ops.get("components", {})
    last = load(LAST)
    counters = load(COUNTERS)
    now = {}

    accounts = sorted(set(k.split(":")[-1] for k in comps))

    for acc in accounts:
        running, dead = [], []
        for name, c in comps.items():
            if acc not in name: continue
            (running if c.get("ok") else dead).append(name)

        status = "ONLINE"
        if dead and running: status = "DEGRADED"
        if dead and not running: status = "OFFLINE"

        prev = last.get(acc, {})
        now[acc] = {"status": status}

        if status != prev.get("status"):
            sev = SEVERITY.get(status, "INFO")
            msg = f"{EMOJI[sev]} FLASHBACK ALERT\nAccount: {acc}\nStatus: {status}"
            send_tg(acc, msg)

        # restart escalation
        for d in dead:
            counters[d] = counters.get(d, 0) + 1
            n = counters[d]
            if n in ESCALATION.values():
                sev = "WARN" if n == ESCALATION["restart_warn"] else \
                      "ERROR" if n == ESCALATION["restart_error"] else \
                      "CRITICAL"
                send_tg(acc, f"{EMOJI[sev]} RESTART ESCALATION\n{d}\nCount: {n}")

        for r in running:
            counters[r] = 0

    save(LAST, now)
    save(COUNTERS, counters)

if __name__ == "__main__":
    narrate()
