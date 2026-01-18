from pathlib import Path

p = Path(r"app/dashboard/templates/dashboard.html")
s = p.read_text(encoding="utf-8", errors="ignore")

needle = '<span class="chip">Last refresh: <span id="lastRefresh">—</span></span>'
if needle not in s:
    raise SystemExit("PATCH_FAIL: topbar needle not found")

addon = needle + """
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

s2 = s.replace(needle, addon, 1)
p.write_text(s2, encoding="utf-8")
print("OK Step 33 applied: global outcomes feed health chip")
