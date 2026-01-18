from pathlib import Path
import re

p = Path(r"app/dashboard/templates/dashboard.html")
s = p.read_text(encoding="utf-8", errors="ignore")

# We insert a small line under the account name, inside acctMeta area.
# Only shows when meta.orch_only_labels exists and acct not in it.
needle = "{% if role %}<span class=\"chip\">Role: {{ role }}</span>{% endif %}"

insert = needle + """
                {% if meta and meta.orch_only_labels and (acct not in meta.orch_only_labels) %}
                  <span class="chip" style="opacity:.75;">Not scheduled (subset)</span>
                {% endif %}
"""

if needle not in s:
    raise SystemExit("PATCH_FAIL: insertion needle not found")

s2 = s.replace(needle, insert, 1)
p.write_text(s2, encoding="utf-8")
print("OK Step 30 applied: per-card subset clarity")
