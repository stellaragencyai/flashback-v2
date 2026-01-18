from pathlib import Path
import re

p = Path(r"app/dashboard/templates/dashboard.html")
s = p.read_text(encoding="utf-8", errors="ignore")
orig = s

# Safety: we expect Group A work to be present
if "GROUP A PATCH" not in s:
    raise SystemExit("PATCH_FAIL: Group A marker not found. You're patching the wrong template or it drifted again.")

if "GROUP A4 PATCH (no outline strip + status badge colors + calmer global truth + left align)" in s:
    print("OK: Group A4 already present (no changes).")
    raise SystemExit(0)

css = r'''
/* ==========================
   GROUP A4 PATCH (no outline strip + status badge colors + calmer global truth + left align)
   ========================== */

/* 1) REMOVE the left accent strip entirely (the vertical outline you hate) */
.card::before{ display: none !important; }

/* 2) LEFT-ALIGN content inside metric tiles (no weird centered vibe) */
.mini, .stat{
  text-align: left !important;
  align-items: flex-start !important;
}
.miniK, .miniV, .statK, .statV{
  text-align: left !important;
}
.twoCols, .triple{ justify-items: stretch !important; }

/* 3) STATUS BADGE: color ONLY the ONLINE/OFFLINE/DEGRADED badge (top-right) */
.card .js-status{
  border: 1px solid rgba(148,163,184,.22) !important;
  background: rgba(148,163,184,.10) !important;
  color: var(--text) !important;
  font-weight: 950 !important;
  letter-spacing: .35px !important;
}

/* ONLINE */
.card[data-status="ONLINE"] .js-status{
  border-color: rgba(22,163,74,.28) !important;
  background: rgba(22,163,74,.10) !important;
  color: var(--green) !important;
}

/* OFFLINE */
.card[data-status="OFFLINE"] .js-status{
  border-color: rgba(220,38,38,.30) !important;
  background: rgba(220,38,38,.10) !important;
  color: var(--red) !important;
}

/* DEGRADED */
.card[data-status="DEGRADED"] .js-status{
  border-color: rgba(245,158,11,.38) !important;
  background: rgba(245,158,11,.12) !important;
  color: #92400e !important;
}

/* STOPPED / UNKNOWN fall back */
.card[data-status="STOPPED"] .js-status,
.card[data-status="UNKNOWN"] .js-status{
  border-color: rgba(100,116,139,.28) !important;
  background: rgba(100,116,139,.10) !important;
  color: var(--gray) !important;
}

/* 4) CALM THE GLOBAL TRUTH BANNER (make it neutral, keep warnings subtle) */
.metaBanner{
  background: rgba(148,163,184,.04) !important;
  border-color: rgba(148,163,184,.16) !important;
  box-shadow: none !important;
}

/* Neutralize the colorful pills inside Global Truth */
.metaBanner .pill,
.metaBanner .pill.blue,
.metaBanner .pill.green,
.metaBanner .pill.yellow,
.metaBanner .pill.red,
.metaBanner .pill.gray{
  background: rgba(148,163,184,.08) !important;
  border-color: rgba(148,163,184,.18) !important;
  color: var(--text) !important;
}

/* Keep the "RUNNING SUBSET" warning readable but not clown-colored */
.metaBanner .pill.red{
  background: rgba(245,158,11,.10) !important;
  border-color: rgba(245,158,11,.22) !important;
}

/* 5) OPTIONAL: tone down random badge colors elsewhere (keep status badge as the one that matters) */
.badge.green, .badge.red, .badge.yellow, .badge.gray{
  background: var(--chip) !important;
  border-color: rgba(148,163,184,.18) !important;
  color: var(--chipText) !important;
}
'''

# Inject CSS right after the GROUP A4 marker anchor (we’ll use the Group A3 marker if present, else Group A marker)
anchor = "GROUP A3 PATCH (premium typography + unified radius + topbar declutter)"
if anchor in s:
    insert_pos = s.find(anchor)
elif "GROUP A PATCH" in s:
    insert_pos = s.find("GROUP A PATCH")
else:
    raise SystemExit("PATCH_FAIL: could not locate a stable injection anchor (Group A/A3 markers missing)")

# Put it right AFTER the line containing the anchor for predictable override ordering
line_end = s.find("\n", insert_pos)
if line_end == -1:
    raise SystemExit("PATCH_FAIL: could not locate newline after anchor")

s2 = s[:line_end+1] + css + s[line_end+1:]

if s2 == orig:
    raise SystemExit("PATCH_FAIL: no changes applied")

p.write_text(s2, encoding="utf-8")
print("OK Group A4 applied: removed outline strip; status badge colors only; global truth calmed; left alignment enforced")
