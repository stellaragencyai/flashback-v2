from pathlib import Path
import re

p = Path(r"app\ops\fleet_degraded.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# Idempotent: if it already uses snapshot fleet_mode, exit
if "snapshot.get(\"fleet_mode\")" in s or "snap.get(\"fleet_mode\")" in s:
    print("OK: fleet_degraded.py already uses snapshot fleet_mode")
    raise SystemExit(0)

# Find where fleet_mode is set (best-effort). If not found, we inject near snap load.
# We'll inject right after: snap = _load_json(FLEET_SNAPSHOT)
needle = "    snap = _load_json(FLEET_SNAPSHOT)"
if needle not in s:
    raise SystemExit("FATAL: could not find snap load line")

inject = '''
    # Phase8: derive fleet_mode from snapshot (single source of truth)
    # Priority: env override > snapshot.fleet_mode > derive from subs > fallback OFF
    env_mode = (os.getenv("FLEET_AUTOMATION_MODE") or "").strip()
    snap_mode = ""
    try:
        snap_mode = str(snap.get("fleet_mode") or "").strip()
    except Exception:
        snap_mode = ""

    derived_mode = ""
    try:
        subs = (snap.get("fleet", {}) or {}).get("subs", {}) if isinstance(snap, dict) else {}
        active = []
        if isinstance(subs, dict):
            for _lbl, info in subs.items():
                if isinstance(info, dict) and bool(info.get("should_run")):
                    m = str(info.get("automation_mode") or "UNKNOWN").strip().upper()
                    active.append(m)
        if "LIVE" in active:
            derived_mode = "LIVE"
        elif "LIVE_CANARY" in active:
            derived_mode = "LIVE_CANARY"
        elif "LEARN_DRY" in active:
            derived_mode = "LEARN_DRY"
        else:
            derived_mode = "OFF"
    except Exception:
        derived_mode = "OFF"

    fleet_mode = (env_mode or snap_mode or derived_mode or "OFF").strip().upper()
'''
s = s.replace(needle, needle + "\n" + inject, 1)

# Now ensure the validate_runtime_contract call uses fleet_mode variable already present in file.
# If it uses something else, you’ll see it immediately in next run output.
p.write_text(s, encoding="utf-8")
print("OK: patched fleet_degraded.py to use snapshot fleet_mode (env override supported)")
