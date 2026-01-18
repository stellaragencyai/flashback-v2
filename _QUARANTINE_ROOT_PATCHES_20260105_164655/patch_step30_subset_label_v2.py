from pathlib import Path
import re

p = Path(r"app/dashboard/templates/dashboard.html")
s = p.read_text(encoding="utf-8", errors="ignore")

# Insert after the Role chip IF it exists, otherwise after Strategy chip.
subset_chip = r"""
                {% if meta and meta.orch_only_labels and (acct not in meta.orch_only_labels) %}
                  <span class="chip" style="opacity:.75;">Not scheduled (subset)</span>
                {% endif %}
"""

# 1) Try after Role chip block
pat_role = r"(\{%\s*if\s*role\s*%\}.*?Role:\s*\{\{\s*role\s*\}\}.*?\{%\s*endif\s*%\})"
m = re.search(pat_role, s, flags=re.DOTALL)
if m and "Not scheduled (subset)" not in s:
    s2 = s[:m.end()] + subset_chip + s[m.end():]
    p.write_text(s2, encoding="utf-8")
    print("OK Step 30 applied (after Role chip)")
    raise SystemExit(0)

# 2) Fallback: insert after Strategy chip
pat_strat = r"(\<span class=\"chip\"\>\s*Strategy:\s*\{\{\s*strat\s*\}\}\s*\<\/span\>)"
m2 = re.search(pat_strat, s, flags=re.DOTALL)
if m2 and "Not scheduled (subset)" not in s:
    s2 = s[:m2.end()] + subset_chip + s[m2.end():]
    p.write_text(s2, encoding="utf-8")
    print("OK Step 30 applied (after Strategy chip fallback)")
    raise SystemExit(0)

raise SystemExit("PATCH_FAIL: could not find Role chip block OR Strategy chip span to insert after")
