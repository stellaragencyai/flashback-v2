from pathlib import Path

p = Path(r"app/dashboard/templates/dashboard.html")
s = p.read_text(encoding="utf-8", errors="ignore")
orig = s

# -------------------------------------------------
# 1) Remove per-card Outcomes DEAD / STALE alerts
# -------------------------------------------------
# We replace the whole conditional block with a spacer to preserve alignment.

marker_start = "{% if dead_outcomes %}"
marker_end   = "{% endif %}"

while marker_start in s:
    start = s.find(marker_start)
    end = s.find(marker_end, start)
    if end == -1:
        break
    end = end + len(marker_end)
    s = s[:start] + '\n        <div class="alertSpacer"></div>\n' + s[end:]

# -------------------------------------------------
# 2) Simplify Last Outcome mini box
# -------------------------------------------------
old_block = '''
            <div class="mini">
            <div class="miniK">Last Outcome (UTC)</div>
            <div class="miniV js-lastOutcome" style="font-size:13px; margin-top:10px;">{{ last_iso }}</div>
            <div class="miniK" style="margin-top:6px;">
              Updated: <span class="mono updatedAgo">—</span>
            </div>
          </div>
'''

new_block = '''
            <div class="mini">
            <div class="miniK">Last Outcome (UTC)</div>
            <div class="miniV js-lastOutcome" style="font-size:13px; margin-top:10px;">{{ last_iso }}</div>
            <div class="miniK" style="margin-top:6px;">
              Age:
              {% if trades|int > 0 and fresh is not none %}
                <span class="mono freshHuman js-freshHuman" data-sec="{{ fresh|float }}">—</span>
              {% else %}
                —
              {% endif %}
            </div>
          </div>
'''

if old_block in s:
    s = s.replace(old_block, new_block, 1)

# -------------------------------------------------
# 3) Remove raw seconds display (leave JS humanizer)
# -------------------------------------------------
s = s.replace(
    '{{ "%.0f"|format(fresh) }}s',
    '—'
)

# -------------------------------------------------
# 4) Calm the topbar Subset chip (remove red)
# -------------------------------------------------
s = s.replace(
    'class="truthPill red">Subset:',
    'class="truthPill">Subset:',
    1
)

if s == orig:
    raise SystemExit("PATCH_FAIL: no changes applied (template structure drifted)")

p.write_text(s, encoding="utf-8")
print("OK Group 1 applied successfully")
