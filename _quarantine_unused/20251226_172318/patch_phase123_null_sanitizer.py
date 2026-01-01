from pathlib import Path
import re

p = Path("app/core/ai_action_router.py")
s = p.read_text(encoding="utf-8", errors="ignore")

pat = (
    r"(def normalize_ai_action\(raw_action: Dict\[str, Any\]\) -> Dict\[str, Any\]:"
    r"\s*\n[\s\S]*?"
    r"if not isinstance\(raw_action, dict\):\s*\n"
    r"\s*raise ValueError\(\"AI action must be a dict\"\)\s*\n)"
)

m = re.search(pat, s)
if not m:
    raise SystemExit("PATCH_FAIL: normalize_ai_action dict-check block not found")

sanitizer = m.group(1) + '''
    # Sanitize explicit nulls so fallback sizing logic can run
    if raw_action.get("risk_pct_notional", "__MISSING__") is None:
        raw_action = dict(raw_action)
        raw_action.pop("risk_pct_notional", None)

    if raw_action.get("max_spread_bps", "__MISSING__") is None:
        raw_action = dict(raw_action)
        raw_action.pop("max_spread_bps", None)

    if raw_action.get("leverage_override", "__MISSING__") is None:
        raw_action = dict(raw_action)
        raw_action.pop("leverage_override", None)
'''

s = s[:m.start()] + sanitizer + s[m.end():]
p.write_text(s, encoding="utf-8")

print("PATCH_OK: normalize_ai_action null-field sanitizer installed")
