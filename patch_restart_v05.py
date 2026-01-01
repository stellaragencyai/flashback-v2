from pathlib import Path
import time, sys

p = Path("app/ops/fleet_narrator.py")
t = p.read_text(encoding="utf-8")

if "RESTART_V0_5" in t:
    print("? Restart detection already installed")
    sys.exit(0)

inject = """
# --- RESTART_V0_5 ---
def detect_restarts(account, comps, prev):
    alerts = []
    now = time.time()
    for name, c in comps.items():
        if account not in name:
            continue
        boot = c.get("details", {}).get("boot_ts") or c.get("ts")
        last = prev.get(name, {})
        if last and boot and boot != last.get("boot"):
            alerts.append((name, last.get("uptime", "?")))
        prev[name] = {"boot": boot, "uptime": int(now - (boot or now))}
    return alerts
# --- END RESTART_V0_5 ---
"""

t = t.replace("prev = last.get(acc)", "prev = last.get(acc, {})")
t = t.replace(
    'now[acc] = {"status": status, "running": running, "dead": dead}',
    'alerts = detect_restarts(acc, comps, prev)\n'
    '        for name, up in alerts:\n'
    '            send_tg(acc, f"?? FLASHBACK RESTART DETECTED\\nAccount: {acc}\\nComponent: {name}\\nPrevious uptime: {up}s")\n'
    '        now[acc] = {"status": status, "running": running, "dead": dead}'
)

t += inject
p.write_text(t, encoding="utf-8")
print("? Restart detection v0.5 installed")
