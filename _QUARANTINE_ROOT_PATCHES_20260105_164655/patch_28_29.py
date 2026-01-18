from pathlib import Path
import re

p = Path(r"app/dashboard/templates/dashboard.html")
s = p.read_text(encoding="utf-8", errors="ignore")

# -----------------------------
# Fix 28: Use backend status_tier truth-first
# Replace card color assignment block with direct tier usage
# -----------------------------
# Find: "{% set color = "gray" %} ... if status_norm == ..."
# Replace with: "{% set color = (r.status_tier or 'gray') %}"
s2 = re.sub(
    r"{%\s*set\s*color\s*=\s*\"gray\"\s*%}[\s\S]*?{%\s*endif\s*%}\s*",
    "{% set color = (r.status_tier or 'gray') %}\n",
    s,
    count=1
)

# -----------------------------
# Fix 29: Improve outcomes display on card (timestamp + human age)
# - Rename Freshness stat label
# - Show human duration instead of raw seconds if present
# -----------------------------
# Rename the stat label "Freshness" -> "Outcomes Age"
s2 = s2.replace("<div class=\"statK\">Freshness</div>", "<div class=\"statK\">Outcomes Age</div>", 1)

# Replace the freshness body to prefer humanSec rendering
# We keep your .freshHuman mechanism but show more readable fallback.
pattern = r"""<div class="statV mono">\s*{% if fresh is not none %}\s*<span class="freshHuman" data-sec="{{ fresh\|float }}">{{ "%.0f"\|format\(fresh\) }}s</span>\s*{% else %}\s*—\s*{% endif %}\s*</div>"""
replacement = """<div class="statV mono">
              {% if fresh is not none %}
                <span class="freshHuman" data-sec="{{ fresh|float }}">—</span>
              {% else %}
                —
              {% endif %}
            </div>"""
s2 = re.sub(pattern, replacement, s2, count=1)

# Also change Last Outcome block to show ISO more clearly
s2 = s2.replace(
    "<div class=\"miniK\">Last Outcome</div>",
    "<div class=\"miniK\">Last Outcome (UTC)</div>",
    1
)

if s2 == s:
    raise SystemExit("PATCH_WARN: no changes applied (pattern mismatch)")
p.write_text(s2, encoding="utf-8")
print("OK applied Fix 28 + Fix 29")
