from pathlib import Path
import re

p = Path(r"app/dashboard/templates/dashboard.html")
s = p.read_text(encoding="utf-8", errors="ignore")

# 1) Add a small FUNDS chip that reflects backend funds_source (instead of guessing from mode)
# Find existing "FUNDS: {{ funds_source }}" chip and replace it
s2 = s.replace(
    '<span class="badge {% if is_live %}green{% else %}red{% endif %}">FUNDS: {{ funds_source }}</span>',
    '<span class="badge {% if r.funds_source == "REAL" %}green{% else %}gray{% endif %}">FUNDS: {{ r.funds_source if r.funds_source else funds_source }}</span>',
    1
)

# 2) Hide balance when SIM (prevent confusing fake $4k numbers in DRY)
# Replace the balance line: "balance": float(...) is backend; here we just avoid showing it.
# We'll add a small "Balance" chip in Details only if REAL.
# Find the "Balance" chip area in details and adjust.
needle = '<span class="chip">Workers Running: {{ r.workers_running if r.workers_running is not none else 0 }}</span>'
if needle not in s2:
    raise SystemExit("PATCH_FAIL: could not find details chips needle")

insert = needle + """
            {% if r.funds_source == "REAL" %}
              <span class="chip">Balance: ${{ "%.2f"|format(r.balance or 0.0) }}</span>
            {% else %}
              <span class="chip">Balance: — (SIM)</span>
            {% endif %}
"""
s2 = s2.replace(needle, insert, 1)

p.write_text(s2, encoding="utf-8")
print("OK Step 32 applied: UI uses funds_source + hides SIM balance")
