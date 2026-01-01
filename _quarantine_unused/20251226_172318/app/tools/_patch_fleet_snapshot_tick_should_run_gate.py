from pathlib import Path

p = Path(r"app\ops\fleet_snapshot_tick.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# Idempotent: if already has mode-gated should_run, exit
if "mode_upper" in s and "should_run_mode_ok" in s:
    print("OK: should_run already mode-gated")
    raise SystemExit(0)

old = '        subs[label] = {\n            "enabled": enabled,\n            "enable_ai_stack": enable_ai_stack,\n            "automation_mode": manifest_mode,\n            "is_canary": bool(is_canary),\n            "should_run": bool(enabled and enable_ai_stack),\n            "alive": alive,\n            "pid": pid,\n        }'
if old not in s:
    raise SystemExit("FATAL: could not find expected subs[label] block (file drift). Paste the subs[label] block.")

new = '''        mode_upper = str(manifest_mode or "UNKNOWN").strip().upper()
        should_run_mode_ok = (mode_upper not in ("OFF", "DISABLED", "NONE"))

        subs[label] = {
            "enabled": enabled,
            "enable_ai_stack": enable_ai_stack,
            "automation_mode": manifest_mode,
            "is_canary": bool(is_canary),
            "should_run": bool(enabled and enable_ai_stack and should_run_mode_ok),
            "alive": alive,
            "pid": pid,
        }'''
s2 = s.replace(old, new, 1)

p.write_text(s2, encoding="utf-8")
print("OK: patched fleet_snapshot_tick.py (should_run now gated by automation_mode != OFF)")
