from pathlib import Path
import re

p = Path(r"app/dashboard/templates/dashboard.html")
s = p.read_text(encoding="utf-8", errors="ignore")
orig = s
hits = {}

# --------------------------------------------------
# A) REMOVE LEFT STATUS ACCENT STRIP
# --------------------------------------------------
# Neutralize ::before entirely
s, n1 = re.subn(
    r"\.card::before\s*\{[^}]*\}",
    ".card::before{ display:none; }",
    s,
    flags=re.DOTALL
)
hits["card_accent_removed"] = n1

# --------------------------------------------------
# B) STATUS BADGE: TEXT COLOR ONLY
# --------------------------------------------------
# Replace badge color rules with text-only semantics
badge_css_pat = r"\.badge\.green\{[^}]*\}.*?\.badge\.gray\{[^}]*\}"
badge_css_repl = """
    .badge.green{ color: var(--green); background: transparent; border-color: var(--border); }
    .badge.red{ color: var(--red); background: transparent; border-color: var(--border); }
    .badge.yellow{ color: var(--yellow); background: transparent; border-color: var(--border); }
    .badge.gray{ color: var(--muted); background: transparent; border-color: var(--border); }
"""
s, n2 = re.subn(badge_css_pat, badge_css_repl, s, flags=re.DOTALL)
hits["badge_colors_simplified"] = n2

# --------------------------------------------------
# C) CALM GLOBAL TRUTH BANNER COLORS
# --------------------------------------------------
# Force all pills to neutral style
s, n3 = re.subn(
    r"\.pill\.(red|green|yellow|blue)\{[^}]*\}",
    "",
    s,
    flags=re.DOTALL
)
hits["truth_pill_color_classes_removed"] = n3

# Add subtle dot indicator instead
if ".truthDot" not in s:
    s = s.replace(
        ".truthPill{",
        """.truthDot{
      width:8px;height:8px;border-radius:50%;
      background: var(--muted);
      display:inline-block;
    }
    .truthDot.online{ background: var(--green); }
    .truthDot.offline{ background: var(--red); }
    .truthDot.degraded{ background: var(--yellow); }

    .truthPill{""",
        1
    )
    hits["truth_dot_added"] = 1
else:
    hits["truth_dot_added"] = 0

# --------------------------------------------------
# D) REDUCE CARD WIDTH ~8%
# --------------------------------------------------
# Grid tightening
s, n4 = re.subn(
    r"grid-template-columns:repeat\(3,\s*minmax\(\d+px,\s*1fr\)\)",
    "grid-template-columns:repeat(3, minmax(330px, 1fr))",
    s
)
hits["grid_width_reduced"] = n4

# Slight padding reduction
s, n5 = re.subn(
    r"\.card\s*\{\s*background:",
    ".card{ padding: 12px; background:",
    s
)
hits["card_padding_reduced"] = n5

if s == orig:
    print("PATCH_FAIL: no changes applied")
    print("HITS:", hits)
    raise SystemExit(2)

p.write_text(s, encoding="utf-8")
print("OK Group 2 applied")
print("HITS:", hits)
