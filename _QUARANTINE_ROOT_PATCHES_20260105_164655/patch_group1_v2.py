from pathlib import Path
import re

p = Path(r"app/dashboard/templates/dashboard.html")
s = p.read_text(encoding="utf-8", errors="ignore")
orig = s

hits = {}

# -------------------------------------------------
# 1) Kill per-card Outcomes DEAD/STALE blocks
#    Replace the entire Jinja conditional with a spacer
# -------------------------------------------------
pat_alert_block = r"\{%\s*if\s+dead_outcomes\s*%\}.*?\{%\s*endif\s*%\}"
s, n = re.subn(pat_alert_block, '\n        <div class="alertSpacer"></div>\n', s, flags=re.DOTALL)
hits["alert_block_removed"] = n

# Fallback: if the Jinja block wasn't found but the text exists, blunt-remove alertBox sections
if hits["alert_block_removed"] == 0:
    pat_alertbox = r"\<div class=\"alertBox\"\>.*?\<\/div\>\s*\<\/div\>"
    s, n2 = re.subn(pat_alertbox, '\n        <div class="alertSpacer"></div>\n', s, flags=re.DOTALL)
    hits["alertbox_removed_fallback"] = n2
else:
    hits["alertbox_removed_fallback"] = 0

# -------------------------------------------------
# 2) Simplify Last Outcome box: replace "Updated: ...updatedAgo..."
#    with an "Age:" line driven by freshHuman
# -------------------------------------------------
pat_updated_line = r"Updated:\s*<span class=\"mono updatedAgo\">—<\/span>"
replace_updated = """Age:
              {% if trades|int > 0 and fresh is not none %}
                <span class="mono freshHuman js-freshHuman" data-sec="{{ fresh|float }}">—</span>
              {% else %}
                —
              {% endif %}"""
s, n3 = re.subn(pat_updated_line, replace_updated, s)
hits["last_outcome_updated_to_age"] = n3

# -------------------------------------------------
# 3) Remove raw seconds placeholder inside freshHuman span (leave JS to humanize)
# -------------------------------------------------
s, n4 = re.subn(r"\{\{\s*\"%\.0f\"\|format\(fresh\)\s*\}\}s", "—", s)
hits["raw_secs_removed"] = n4

# -------------------------------------------------
# 4) Calm topbar Subset chip (remove forced red there)
# -------------------------------------------------
s, n5 = re.subn(r"class=\"truthPill\s+red\"\>Subset:", 'class="truthPill">Subset:', s, count=1)
hits["subset_red_removed"] = n5

# Safety: ensure we changed something
if s == orig:
    print("PATCH_FAIL: no changes applied")
    print("HITS:", hits)
    raise SystemExit(2)

p.write_text(s, encoding="utf-8")
print("OK Group 1 v2 applied")
print("HITS:", hits)
