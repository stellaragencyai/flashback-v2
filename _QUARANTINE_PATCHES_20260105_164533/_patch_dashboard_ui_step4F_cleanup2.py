#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Patch: Dashboard UI Step 4F (Breakpoint normalize + leftover cleanup)

Fixes:
- Add/normalize grid breakpoints compatible with minmax(460px)
- Remove actionRow CSS (buttons removed but CSS/JS remained)
- Remove /api/action JS block (no-op + confusing)
- Optionally collapse duplicate PREMIUM OVERRIDES v1 blocks so v2 wins cleanly
"""

from __future__ import annotations
import re
from pathlib import Path

P = Path("app/dashboard/templates/dashboard.html")
s = P.read_text(encoding="utf-8", errors="ignore")
orig = s

def _strip_block(regex: str, text: str, flags=0):
    return re.subn(regex, "", text, flags=flags)

# -----------------------------
# 1) Ensure grid breakpoints exist (based on 460px min)
# We'll force these rules near the main .grid definition if possible:
# - <= 1350px => 2 cols-ish
# - <= 900px  => 1 col
# -----------------------------
bp_css = r"""
@media (max-width: 1350px){
  .grid{ grid-template-columns: repeat(auto-fit, minmax(420px, 1fr)); }
}
@media (max-width: 900px){
  .grid{ grid-template-columns: 1fr; }
}
""".strip()

# Insert breakpoints right after the first ".grid{...}" block if not already present
if "max-width: 1350px" not in s and "max-width: 900px" not in s:
    m = re.search(r"\.grid\{[^}]*\}\s*", s, flags=re.S)
    if m:
        insert_at = m.end()
        s = s[:insert_at] + "\n\n" + bp_css + "\n\n" + s[insert_at:]
        n_bp_insert = 1
    else:
        # Fail-soft: append before </style>
        s, n_bp_insert = re.subn(r"</style>", bp_css + "\n\n</style>", s, count=1)
else:
    n_bp_insert = 0

# -----------------------------
# 2) Remove actionRow CSS block entirely
# -----------------------------
s, n_action_css = _strip_block(
    r"/\*\s*ACTION ROW v1\s*\*/.*?(?=\n/\*|\n</style>)",
    s,
    flags=re.S
)

# Also strip any stray .actionRow / .actBtn rules if they survived
s, n_action_css2 = _strip_block(
    r"\.actionRow\{.*?\}\s*\.actBtn\{.*?\}.*?(?=\n\.)",
    s,
    flags=re.S
)

# -----------------------------
# 3) Remove /api/action JS section + bindActionButtons call
# -----------------------------
s, n_action_js = _strip_block(
    r"// PART 3: Action buttons.*?bindActionButtons\(\);\s*",
    s,
    flags=re.S
)

# If it's not labeled, kill the direct fetch('/api/action') chunk
s, n_action_js2 = _strip_block(
    r"async function bindActionButtons\(\)\{.*?\}\s*bindActionButtons\(\);\s*",
    s,
    flags=re.S
)

# -----------------------------
# 4) Reduce duplicate PREMIUM OVERRIDES v1 blocks (keep v2)
# Remove the first big "PREMIUM UI OVERRIDES v1" section if present,
# but do NOT touch v2.
# -----------------------------
# Remove the earlier v1 block that starts with the big header and ends at the comment footer
s, n_v1 = _strip_block(
    r"/\*\s*==========================\s*\n\s*PREMIUM UI OVERRIDES v1.*?\*/\s*",
    s,
    flags=re.S
)

# Also remove the later "PREMIUM UI OVERRIDES v1" duplicate chunk that redefines the same things
# (but keep anything that is explicitly v2)
s, n_v1b = _strip_block(
    r"/\*\s*PREMIUM UI OVERRIDES v1\s*\*/.*?(?=/\*\s*PREMIUM UI OVERRIDES v2|\n</style>)",
    s,
    flags=re.S
)

if s != orig:
    P.write_text(s, encoding="utf-8")
    print("OK: Step 4F applied -> breakpoints + cleanup complete")
    print("breakpoints inserted:", n_bp_insert)
    print("actionRow CSS removed:", n_action_css, n_action_css2)
    print("actionRow JS removed:", n_action_js, n_action_js2)
    print("premium v1 blocks removed:", n_v1, n_v1b)
else:
    print("OK: Step 4F no changes needed (patterns not found)")
