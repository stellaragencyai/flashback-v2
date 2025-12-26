from pathlib import Path

p = Path(r"app\ops\fleet_degraded.py")
s = p.read_text(encoding="utf-8", errors="ignore")

if "ignore_ws_heartbeat" in s and "ignore_memory" in s and "ignore_decisions" in s:
    print("OK: ignore flags already present")
    raise SystemExit(0)

needle = 'age_unit="ms",'
if needle not in s:
    raise SystemExit('FATAL: age_unit="ms", not found')

insert = needle + "\n            ignore_ws_heartbeat=True,\n            ignore_memory=True,\n            ignore_decisions=True,"

s = s.replace(needle, insert, 1)
p.write_text(s, encoding="utf-8")
print("OK: injected ignore flags")
