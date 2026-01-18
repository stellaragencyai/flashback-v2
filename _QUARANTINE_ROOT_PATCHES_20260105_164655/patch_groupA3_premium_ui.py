from pathlib import Path
import re

p = Path(r"app/dashboard/templates/dashboard.html")
s = p.read_text(encoding="utf-8", errors="ignore")
orig = s

marker = "GROUP A PATCH (UI polish)"
if marker not in s:
    raise SystemExit("PATCH_FAIL: Group A marker not found. This patch expects the template you’ve been modifying.")

if "GROUP A3 PATCH (premium typography + radius + topbar declutter)" in s:
    print("OK: Group A3 already present (no changes).")
    raise SystemExit(0)

# -----------------------------
# 1) TOPBAR CHIP DECLUTTER
# -----------------------------
# We keep only a few pills by marking them with a 'keep' class.
# Then CSS hides the rest. This avoids fragile HTML surgery.
def add_keep_class(pattern: str) -> None:
    global s
    # add "keep" into class="truthPill ..."
    s = re.sub(
        rf'(<span\s+class="truthPill)([^"]*)(">\s*{pattern})',
        r'\1 keep\2\3',
        s,
        count=1,
        flags=re.IGNORECASE
    )

add_keep_class("Cards:")
add_keep_class("Orchestrator:")
add_keep_class("Subset:")

# -----------------------------
# 2) PREMIUM CSS (typography + radius + declutter)
# -----------------------------
css = r'''
/* ==========================
   GROUP A3 PATCH (premium typography + radius + topbar declutter)
   ========================== */

:root{
  --r-lg: 16px;
  --r-md: 14px;
  --r-sm: 12px;
}

/* Unify radius across the whole UI */
.card{ border-radius: var(--r-lg) !important; }
.sCard, .metaBanner{ border-radius: var(--r-md) !important; }
.mini, .stat{ border-radius: var(--r-md) !important; }
.btn, .smallBtn, .input, .select{ border-radius: 12px !important; }
.chip, .badge, .pill, .truthPill{ border-radius: 999px !important; }

/* Typography hierarchy: labels softer, values stronger */
.miniK, .statK{
  font-size: 11px !important;
  letter-spacing: .15px !important;
  color: var(--muted) !important;
  font-weight: 700 !important;
}
.miniV{
  font-size: 18px !important;
  letter-spacing: .15px !important;
  font-weight: 900 !important;
  line-height: 1.15 !important;
}
.statV{
  font-size: 13px !important;
  letter-spacing: .12px !important;
  font-weight: 900 !important;
  line-height: 1.2 !important;
}

/* Tighten internal spacing so it feels deliberate */
.twoCols, .triple{
  margin-top: 8px !important;
}
.miniV{ margin-top: 6px !important; }
.statV{ margin-top: 6px !important; }

/* Subhead declutter: hide most pills unless explicitly marked keep */
.subhead .truthPill{ display: none !important; }
.subhead .truthPill.keep{ display: inline-flex !important; }

/* Keep the "Last refresh" block readable but calmer */
.subhead span[id="lastRefresh"],
.subhead #lastRefresh{
  font-weight: 800 !important;
}

/* Make the top row breathe less (less “dashboard vomit”) */
.subhead{
  gap: 8px !important;
  margin-top: 6px !important;
}

/* Slightly reduce summary tile visual weight */
.sK{ font-size: 11px !important; font-weight: 750 !important; }
.sV{ font-size: 18px !important; font-weight: 900 !important; }

/* Make borders softer in light mode */
html:not([data-theme="dark"]) .card,
html:not([data-theme="dark"]) .sCard,
html:not([data-theme="dark"]) .metaBanner,
html:not([data-theme="dark"]) .mini,
html:not([data-theme="dark"]) .stat{
  border-color: rgba(226,232,240,.85) !important;
}
'''

# Insert CSS right after the Group A marker line so it overrides earlier CSS cleanly
idx = s.find(marker)
if idx == -1:
    raise SystemExit("PATCH_FAIL: marker index not found")
insert_at = s.find("\n", idx)
if insert_at == -1:
    raise SystemExit("PATCH_FAIL: marker newline not found")

s2 = s[:insert_at+1] + css + s[insert_at+1:]

if s2 == orig:
    raise SystemExit("PATCH_FAIL: no changes applied")

p.write_text(s2, encoding="utf-8")
print("OK Group A3 applied: premium typography + unified radius + topbar declutter")
