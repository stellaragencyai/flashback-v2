from pathlib import Path

p = Path(r"app\core\ai_state_bus.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# Step 10 patch:
# If include_orderbook=False, do NOT require orderbook freshness.
# Apply in BOTH build_symbol_state() and build_ai_snapshot() blocks.

needle1 = "    ob_age_sec, tr_age_sec = _market_bus_ages()\n    if not include_trades:\n        tr_age_sec = None\n"
replace1 = "    ob_age_sec, tr_age_sec = _market_bus_ages()\n    if not include_trades:\n        tr_age_sec = None\n    if not include_orderbook:\n        ob_age_sec = None\n"

n1 = s.count(needle1)
if n1 != 2:
    raise SystemExit(f"PATCH_FAIL ai_state_bus.py: expected 2 matches for needle1, got {n1}")

s = s.replace(needle1, replace1)

p.write_text(s, encoding="utf-8")
print("PATCH_OK ai_state_bus.py Step10 include_orderbook gating")
