from pathlib import Path

p = Path(r"app\core\ai_state_bus.py")
s = p.read_text(encoding="utf-8", errors="ignore")

needle = '        if pos_age_sec is not None and pos_age_sec > thresholds["positions"]:\n'
n = s.count(needle)
if n != 2:
    # Try alternate common style
    needle2 = '        if pos_age_sec is not None and pos_age_sec > thresholds_sec["positions"]:\n'
    n2 = s.count(needle2)
    if n2 != 2:
        raise SystemExit(f"PATCH_FAIL ai_state_bus.py: expected 2 matches for positions stale check, got {n} / alt {n2}")
    needle = needle2

replace = (
    '        # DRY_RUN: positions may be intentionally empty + no WS/REST buses running.\n'
    '        # Do not block safety on positions freshness in paper mode.\n'
    '        if EXEC_DRY_RUN:\n'
    '            pos_age_sec = None\n'
    + needle
)

s = s.replace(needle, replace, 2)

p.write_text(s, encoding="utf-8")
print("PATCH_OK ai_state_bus.py DRY_RUN ignores positions bus freshness")
