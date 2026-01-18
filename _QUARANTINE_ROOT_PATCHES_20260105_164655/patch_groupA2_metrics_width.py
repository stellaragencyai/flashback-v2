from pathlib import Path
import re

p = Path(r"app/dashboard/templates/dashboard.html")
s = p.read_text(encoding="utf-8", errors="ignore")
orig = s

marker = "GROUP A PATCH (UI polish)"
if marker not in s:
    raise SystemExit("PATCH_FAIL: Group A marker not found. Did Group A apply to this template?")

# If already applied, do nothing
if "GROUP A2 PATCH (metrics width + left align)" in s:
    print("OK: Group A2 already present (no changes).")
    raise SystemExit(0)

css = r'''
/* ==========================
   GROUP A2 PATCH (metrics width + left align)
   ========================== */

/* Revert the centering that was added earlier */
.mini, .stat{
  align-items: flex-start !important;
  text-align: left !important;
  justify-content: flex-start !important;
}

/* Restore typical left spacing inside tiles */
.mini, .stat{
  padding: 10px 12px !important;
}

/* Make metric blocks visually shorter (width-wise) by adding gutters
   This reduces the usable width by ~15% without changing card/grid structure */
.twoCols, .triple{
  padding-left: 22px !important;
  padding-right: 22px !important;
}

/* Keep label/value spacing tidy */
.miniK, .statK{
  margin: 0 !important;
}
.miniV, .statV{
  margin-top: 6px !important;
}

/* Slightly tighten the inner gap so it feels compact and premium */
.twoCols{ gap: 10px !important; }
.triple{ gap: 10px !important; }
'''

# Insert CSS right after the Group A CSS block so it overrides it
# We inject before the closing "GROUP A PATCH" section ends by placing right after marker line.
idx = s.find(marker)
if idx == -1:
    raise SystemExit("PATCH_FAIL: marker index not found")

# Insert after the marker line occurrence (safe)
insert_at = s.find("\n", idx)
if insert_at == -1:
    raise SystemExit("PATCH_FAIL: marker line newline not found")

s2 = s[:insert_at+1] + css + s[insert_at+1:]

if s2 == orig:
    raise SystemExit("PATCH_FAIL: no changes applied")

p.write_text(s2, encoding="utf-8")
print("OK Group A2 applied: 15% narrower metric tiles via gutters + left alignment restored")
