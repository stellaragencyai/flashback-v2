from pathlib import Path

p = Path(r"app\tools\ws_health_check.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# Anchor: mem_required block we inserted earlier
anchor = "    # Memory requirement is mode-aware (OFF => WARN only)\n"
if anchor not in s:
    raise SystemExit("PATCH FAIL: could not find mem_required anchor (did prior patch apply?)")

# Insert ws_required right after mem_required block (so mode exists already)
if "ws_required = True" not in s:
    insert = anchor + (
        "    # WS requirement is mode-aware (OFF => WARN only)\n"
        "    ws_required = True\n"
        "    try:\n"
        "        m = str(mode).strip().upper()\n"
        "        if m in (\"OFF\",):\n"
        "            ws_required = False\n"
        "    except Exception:\n"
        "        ws_required = True\n\n"
    )
    s = s.replace(anchor, insert, 1)

# Gate the early WS failures block:
# Find the first occurrence of "if failures:" which is the WS freshness guardrails section
needle = "    if failures:\n"
idx = s.find(needle)
if idx < 0:
    raise SystemExit("PATCH FAIL: could not find WS failures block (if failures:)")

# Replace ONLY the first occurrence (WS block), not any later usage
s = s.replace(needle, "    if failures and ws_required:\n", 1)

# After the WS return code 2, add WARN-only branch
target = "        return 2\n"
pos = s.find(target)
if pos < 0:
    raise SystemExit("PATCH FAIL: could not find 'return 2' for WS failures block")

warn_block = (
    "        return 2\n\n"
    "    if failures and (not ws_required):\n"
    "        print(\"\\nWARN:\")\n"
    "        for f in failures:\n"
    "            print(f\" - {f}\")\n"
)
s = s.replace(target, warn_block, 1)

p.write_text(s, encoding="utf-8")
print("OK: patched ws_health_check.py (WS mode-aware; OFF => WARN for WS staleness)")
