#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Patch: Dashboard UI Step 4B (Cleanup + Restore Layout)

Fixes:
- Remove legacy metric blocks that still render after the new .quad row
- Remove stray actionRow inside the global sysPanel (acct not defined there)
- Remove redundant metaRow banner after sysPanel
- Restore compact grid feel (auto-fit columns)
"""

from __future__ import annotations

import re
from pathlib import Path

P = Path("app/dashboard/templates/dashboard.html")

def fatal(msg: str) -> None:
    raise SystemExit(f"FATAL: {msg}")

if not P.exists():
    fatal(f"Missing file: {P}")

s = P.read_text(encoding="utf-8", errors="ignore")
orig = s

# -------------------------
# 1) Remove actionRow inside sysPanel (global panel has no {{ acct }})
# -------------------------
s, n_action = re.subn(
    r"\n\s*<div class=\"actionRow\"[^>]*>\s*.*?\s*</div>\s*\n",
    "\n",
    s,
    flags=re.DOTALL,
)

# -------------------------
# 2) Remove redundant metaRow banner after sysPanel
#    (keep sysPanel, kill the old pill banner)
# -------------------------
s, n_meta = re.subn(
    r"(</details>\s*)<div class=\"metaRow\">.*?\n\s*</div>\s*</div>\s*",
    r"\1",
    s,
    flags=re.DOTALL,
)

# -------------------------
# 3) Remove legacy metric blocks that still render after .quad
#    We delete everything from the first leftover legacy stat block
#    up to the alert spacer.
# -------------------------
s, n_legacy = re.subn(
    r"\n\s*<div class=\"statV js-trades\".*?\n\s*<div class=\"alertSpacer\"></div>\s*\n",
    "\n\n        <div class=\"alertSpacer\"></div>\n",
    s,
    flags=re.DOTALL,
)

# -------------------------
# 4) Restore compact grid layout (auto-fit)
#    Replace the fixed repeat(3...) grid if present.
# -------------------------
s, n_grid = re.subn(
    r"\.grid\{\s*display:grid;\s*grid-template-columns:repeat\(3,\s*minmax\(330px,\s*1fr\)\);\s*gap:\s*14px;\s*\}",
    ".grid{\n      display:grid;\n      grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));\n      gap: 14px;\n      align-items: start;\n    }",
    s,
    flags=re.DOTALL,
)

# Safety: only write if changed
if s == orig:
    print("OK: Step 4B no changes needed (file already clean)")
else:
    P.write_text(s, encoding="utf-8")
    print("OK: Step 4B applied -> dashboard.html updated")
    print(f"Removed actionRow blocks: {n_action}")
    print(f"Removed metaRow banners: {n_meta}")
    print(f"Removed legacy metric blocks: {n_legacy}")
    print(f"Patched grid rule: {n_grid}")
