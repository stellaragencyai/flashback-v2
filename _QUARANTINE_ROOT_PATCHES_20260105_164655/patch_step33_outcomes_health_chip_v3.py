from pathlib import Path
import re

p = Path(r"app/dashboard/templates/dashboard.html")
s = p.read_text(encoding="utf-8", errors="ignore")

if "Outcomes:" in s and "outcomes_freshness_sec" in s:
    print("OK Step 33 already applied")
    raise SystemExit(0)

# Find the lastRefresh span itself (super stable)
pat = r"(\<span\s+id=\"lastRefresh\"\>.*?\<\/span\>)"
m = re.search(pat, s, flags=re.DOTALL)
if not m:
    raise SystemExit("PATCH_FAIL: could not find <span id=\"lastRefresh\">")

inject = m.group(1) + """
        {% set of = (meta.outcomes_freshness_sec if (meta and meta.outcomes_freshness_sec is not none) else none) %}
        {% set ostate = "?" %}
        {% if of is not none %}
          {% if of|float <= 3600 %}{% set ostate = "OK" %}
          {% elif of|float <= 86400 %}{% set ostate = "STALE" %}
          {% else %}{% set ostate = "DEAD" %}
          {% endif %}
        {% endif %}
        <span class="chip">Outcomes: {{ ostate }}{% if meta and meta.outcomes_last_ts_iso %} ({{ meta.outcomes_last_ts_iso }}){% endif %}</span>
"""

s2 = s.replace(m.group(1), inject, 1)
p.write_text(s2, encoding="utf-8")
print("OK Step 33 applied: outcomes chip injected after lastRefresh span")
