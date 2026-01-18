from pathlib import Path
import re

p = Path(r"app/dashboard/templates/dashboard.html")
s = p.read_text(encoding="utf-8", errors="ignore")

orig = s

# 1) Fix the broken CSS comment header that can break everything after it.
# Replace the unclosed "GROUP A PATCH" line with a proper closed comment.
s = s.replace(
    "/* ==========================\n   GROUP A PATCH (UI polish)\n",
    "/* ==========================\n   GROUP A PATCH (UI polish)\n   ========================== */\n"
)

# 2) Remove the messy/duplicated legacy "A1/A2/A3/A4" block that conflicts with your newer rules.
# (That block starts at "A1: Remove vertical accent strip entirely" and includes grid/min-width + centering.)
s = re.sub(
    r"\n/\* A1: Remove vertical accent strip entirely \*/.*?@media \(max-width: 1180px\)\{\n  \.grid\{ grid-template-columns: repeat\(2, minmax\(316px, 1fr\)\) !important; \}\n\}\n",
    "\n",
    s,
    flags=re.S
)

# 3) Ensure OFFLINE maps to red (not gray) in the server-rendered template logic.
# Find the block that sets 'color' based on status_norm and fix OFFLINE.
s = re.sub(
    r"elif status_norm == \"OFFLINE\" %\}\{% set color = \"gray\" %\}",
    r"elif status_norm == \"OFFLINE\" %}{% set color = \"red\" %}",
    s
)

# 4) Add missing js hooks so softRefresh can update Strategy/Role live.
# Strategy chip: add js-strategy
s = s.replace(
    '<span class="chip truncate" title="Strategy: {{ strat }}">Strategy: {{ strat }}</span>',
    '<span class="chip truncate js-strategy" title="Strategy: {{ strat }}">Strategy: {{ strat }}</span>'
)

# Role chip: add js-role (only if role exists)
s = s.replace(
    '{% if role %}<span class="chip truncate" title="Role: {{ role }}">Role: {{ role }}</span>{% endif %}',
    '{% if role %}<span class="chip truncate js-role" title="Role: {{ role }}">Role: {{ role }}</span>{% endif %}'
)

# 5) PREMIUM UI OVERRIDES v1 (single source of truth overrides)
marker = "/* PREMIUM UI OVERRIDES v1 */"
if marker not in s:
    # Inject near the end of <style> before </style>
    insert = r"""
/* PREMIUM UI OVERRIDES v1
   - unify geometry
   - left-align content
   - shrink inner tile usable width ~15%
   - status badge gets the only loud color
*/
.card::before{ display:none !important; } /* no vertical strips */

.twoCols, .triple{
  padding-left: 22px !important;
  padding-right: 22px !important;
}

.mini, .stat{
  text-align:left !important;
  align-items:flex-start !important;
}

.card .js-status{
  border: 1px solid rgba(148,163,184,.22) !important;
  background: rgba(148,163,184,.10) !important;
  color: var(--text) !important;
  font-weight: 950 !important;
  letter-spacing: .35px !important;
}
.card[data-status="ONLINE"] .js-status{
  border-color: rgba(22,163,74,.28) !important;
  background: rgba(22,163,74,.10) !important;
  color: var(--green) !important;
}
.card[data-status="OFFLINE"] .js-status{
  border-color: rgba(220,38,38,.30) !important;
  background: rgba(220,38,38,.10) !important;
  color: var(--red) !important;
}
.card[data-status="DEGRADED"] .js-status{
  border-color: rgba(245,158,11,.38) !important;
  background: rgba(245,158,11,.12) !important;
  color: #92400e !important;
}
.card[data-status="STOPPED"] .js-status,
.card[data-status="UNKNOWN"] .js-status{
  border-color: rgba(100,116,139,.28) !important;
  background: rgba(100,116,139,.10) !important;
  color: var(--gray) !important;
}
"""
    s = s.replace("</style>", f"\n{marker}\n{insert}\n</style>")

if s == orig:
    print("PATCH_NOOP: nothing changed (already applied or structure drifted)")
else:
    p.write_text(s, encoding="utf-8")
    print("OK: Part 1 applied (CSS stabilized + OFFLINE=red + js hooks + premium overrides v1)")
