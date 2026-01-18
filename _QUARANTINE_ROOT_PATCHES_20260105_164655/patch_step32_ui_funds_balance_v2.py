from pathlib import Path
import re

p = Path(r"app/dashboard/templates/dashboard.html")
s = p.read_text(encoding="utf-8", errors="ignore")

# 1) Replace FUNDS chip (match any existing FUNDS badge)
# We keep the other colors the same, just swap the logic.
funds_pat = r"\<span class=\"badge[^\"]*\"\>\s*FUNDS:\s*\{\{\s*funds_source\s*\}\}\s*\<\/span\>"
if re.search(funds_pat, s):
    s = re.sub(
        funds_pat,
        '<span class="badge {% if r.funds_source == \\"REAL\\" %}green{% else %}gray{% endif %}">FUNDS: {{ r.funds_source if r.funds_source else funds_source }}</span>',
        s,
        count=1
    )
    print("OK Step 32a: FUNDS chip replaced")
else:
    print("WARN Step 32a: FUNDS chip pattern not found (skipping)")

# 2) Add Balance chip inside <div class="chips"> ... </div> within <details>
if "Balance: — (SIM)" not in s:
    # Find the first details chips block
    chips_pat = r"(\<div class=\"chips\"\>\s*)(.*?)(\s*\<\/div\>)"
    m = re.search(chips_pat, s, flags=re.DOTALL)
    if not m:
        raise SystemExit("PATCH_FAIL: could not find <div class=\"chips\"> block")

    add = """
            {% if r.funds_source == "REAL" %}
              <span class="chip">Balance: ${{ "%.2f"|format(r.balance or 0.0) }}</span>
            {% else %}
              <span class="chip">Balance: — (SIM)</span>
            {% endif %}
"""
    # Insert near end of chips content (before closing </div>)
    new_mid = m.group(2) + add
    s = s[:m.start()] + m.group(1) + new_mid + m.group(3) + s[m.end():]
    print("OK Step 32b: Balance chip inserted in Details")
else:
    print("OK Step 32b: Balance chip already present")

p.write_text(s, encoding="utf-8")
print("OK Step 32 applied (v2)")
