from pathlib import Path
import re

p = Path("app/core/ai_action_router.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# Replace the SINGLE brittle line:
#   risk_pct_notional = _to_decimal(payload.get("risk_pct_notional"), "risk_pct_notional")
#
# with a safe fallback sizing block based on size_fraction + profile.max_notional_pct.

needle = r'risk_pct_notional\s*=\s*_to_decimal\(\s*payload\.get\("risk_pct_notional"\)\s*,\s*"risk_pct_notional"\s*\)\s*'
m = re.search(needle, s)
if not m:
    raise SystemExit("PATCH_FAIL: could not find brittle risk_pct_notional parsing line")

replacement = r'''# risk_pct_notional may be missing/null in JSONL, compute fallback if needed
    rpn_raw = payload.get("risk_pct_notional", None)
    if rpn_raw is None:
        # Compute from size_fraction * profile.max_notional_pct if present
        max_pct = profile.get("max_notional_pct")
        try:
            max_pct = Decimal(str(max_pct))
        except Exception:
            max_pct = Decimal("40")  # safe fallback

        sf_raw = payload.get("size_fraction", None)
        if sf_raw is not None:
            try:
                sf = Decimal(str(sf_raw))
            except Exception:
                sf = Decimal("0")
            if sf <= 0:
                raise ValueError("size_fraction must be > 0 when used")
            if sf > 1:
                sf = Decimal("1")
            risk_pct_notional = max_pct * sf
        else:
            # Conservative fallback: min(max_pct, 1%)
            risk_pct_notional = max_pct if max_pct < Decimal("1.0") else Decimal("1.0")
    else:
        risk_pct_notional = _to_decimal(rpn_raw, "risk_pct_notional")
'''

s = s[:m.start()] + replacement + s[m.end():]
p.write_text(s, encoding="utf-8")
print("PATCH_OK: risk_pct_notional now computed when missing/null (size_fraction fallback enabled)")
