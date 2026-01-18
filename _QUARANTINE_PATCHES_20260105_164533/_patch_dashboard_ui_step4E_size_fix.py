#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Patch: Dashboard UI Step 4E (Size + Density Fix)

Goals:
- Make cards wider (increase grid min width)
- Make container padding + metric tiles feel normal again
- Keep the v2 layout, just stop compressing it into postage stamps
"""

from __future__ import annotations
import re
from pathlib import Path

P = Path("app/dashboard/templates/dashboard.html")
s = P.read_text(encoding="utf-8", errors="ignore")
orig = s

# 1) Widen the card min width in the main grid rule
# We previously patched grid-template-columns to auto-fit minmax(360px,...)
# Raise min width to 460px (desktop) so you get fewer, larger cards.
s, n_grid1 = re.subn(
    r"grid-template-columns:\s*repeat\(auto-fit,\s*minmax\(\s*360px\s*,\s*1fr\s*\)\s*\)\s*;",
    "grid-template-columns: repeat(auto-fit, minmax(460px, 1fr));",
    s,
)

# If grid rule is still in old format (3 columns minmax 330), bump it too.
s, n_grid2 = re.subn(
    r"grid-template-columns:\s*repeat\(\s*3\s*,\s*minmax\(\s*330px\s*,\s*1fr\s*\)\s*\)\s*;",
    "grid-template-columns: repeat(auto-fit, minmax(460px, 1fr));",
    s,
)

# 2) Undo the v2 "shrink card padding" if present
# Replace: .card{ padding: 12px !important; }
s, n_pad = re.subn(
    r"\.card\{\s*padding:\s*12px\s*!important;\s*\}",
    ".card{ padding: 16px !important; }",
    s,
)

# 3) Make quad metrics less cramped
# Increase metric min-height and font sizes slightly
s, n_metric_h = re.subn(
    r"min-height:\s*88px\s*;",
    "min-height: 104px;",
    s,
)

s, n_metricV = re.subn(
    r"\.metricV\{\s*[^}]*?font-size:\s*16px\s*;",
    lambda m: m.group(0).replace("font-size: 16px", "font-size: 18px"),
    s,
    flags=re.S,
)

# 4) Make quad spacing a bit roomier
s, n_quad_gap = re.subn(
    r"\.quad\{\s*display:\s*grid;\s*grid-template-columns:\s*repeat\(4,\s*1fr\);\s*gap:\s*10px\s*;",
    ".quad{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px;",
    s,
)

# 5) Keep collapse breakpoints, but make them match the new width
# 980 -> 1100 so it goes 2-col earlier (since cards are wider now)
s, n_bp1 = re.subn(
    r"@media\s*\(max-width:\s*980px\)",
    "@media (max-width: 1100px)",
    s,
)

# 640 -> 760 for 1-col collapse
s, n_bp2 = re.subn(
    r"@media\s*\(max-width:\s*640px\)",
    "@media (max-width: 760px)",
    s,
)

if s != orig:
    P.write_text(s, encoding="utf-8")
    print("OK: Step 4E applied -> dashboard sizing restored")
    print("patched grid auto-fit (360->460):", n_grid1)
    print("patched grid legacy (330->460):", n_grid2)
    print("patched card padding:", n_pad)
    print("patched metric min-height:", n_metric_h)
    print("patched metricV font-size:", n_metricV)
    print("patched quad gap:", n_quad_gap)
    print("patched breakpoints:", n_bp1, n_bp2)
else:
    print("OK: Step 4E no changes needed (patterns not found)")
