from pathlib import Path
import re

p = Path(r"app/dashboard/templates/dashboard.html")
s = p.read_text(encoding="utf-8", errors="ignore")
orig = s

# ---------------------------
# Inject CSS (Group A)
# ---------------------------
css_inject = r'''
/* ==========================
   GROUP A PATCH (UI polish)
   ========================== */

/* A1: Remove vertical accent strip entirely */
.card::before{ width: 0 !important; }

/* A2: Status pill becomes text-only with semantic color */
.badge.js-status{
  background: transparent !important;
  border: none !important;
  box-shadow: none !important;
  padding: 0 !important;
  border-radius: 0 !important;
  letter-spacing: .35px;
}

/* Status text colors (requested) */
.badge.js-status.online,
.badge.js-status.ONLINE{ color: var(--green) !important; }

.badge.js-status.offline,
.badge.js-status.OFFLINE{ color: var(--red) !important; }

.badge.js-status.degraded,
.badge.js-status.DEGRADED{ color: var(--yellow) !important; }

/* Fallback: status color by data-status attribute */
.card[data-status="ONLINE"] .badge.js-status{ color: var(--green) !important; }
.card[data-status="OFFLINE"] .badge.js-status{ color: var(--red) !important; }
.card[data-status="DEGRADED"] .badge.js-status{ color: var(--yellow) !important; }

/* A3: Make inner metric blocks ~33% shorter + centered */
.twoCols{ gap: 10px; }
.mini, .stat{
  min-height: 0 !important;
  padding: 8px 10px !important;
  display: flex !important;
  flex-direction: column !important;
  justify-content: center !important;
  align-items: center !important;
  text-align: center !important;
}
.miniK, .statK{
  font-size: 11px !important;
  margin: 0 !important;
}
.miniV{
  font-size: 16px !important;
  margin-top: 6px !important;
}
.statV{
  font-size: 13px !important;
  margin-top: 6px !important;
}
.mini .miniK, .stat .statK{ line-height: 1.15 !important; }
.mini .miniV, .stat .statV{ line-height: 1.1 !important; }

/* A4: Narrow cards ~8% by tightening the grid min width */
.grid{
  grid-template-columns: repeat(3, minmax(332px, 1fr)) !important;
}
@media (max-width: 1180px){
  .grid{ grid-template-columns: repeat(2, minmax(316px, 1fr)) !important; }
}
'''

if "</style>" not in s:
    raise SystemExit("PATCH_FAIL: no </style> found")

if "GROUP A PATCH (UI polish)" not in s:
    s = s.replace("</style>", css_inject + "\n</style>", 1)

# ---------------------------
# Ensure status badge has js-status class
# ---------------------------
if "js-status" not in s:
    # Patch the badge that outputs status_norm
    s_new = re.sub(
        r'(<div\s+class="badge\s+)([^"]*)(">\s*\{\{\s*status_norm\s*\}\}\s*</div>)',
        lambda m: m.group(1) + (m.group(2) + " js-status").strip() + m.group(3),
        s,
        count=1
    )
    s = s_new

# ---------------------------
# Inject JS to tag statuses for color-only behavior
# ---------------------------
if "<script>" not in s:
    raise SystemExit("PATCH_FAIL: no <script> tag found")

if "function tagStatusBadges()" not in s:
    js_inject = r'''
    // GROUP A: tag status badges with semantic classes for color-only status
    function tagStatusBadges(){
      document.querySelectorAll(".badge.js-status").forEach(el=>{
        const t = (el.textContent || "").trim().toUpperCase();
        el.classList.remove("ONLINE","OFFLINE","DEGRADED","online","offline","degraded");
        if(t === "ONLINE"){ el.classList.add("ONLINE","online"); }
        else if(t === "OFFLINE"){ el.classList.add("OFFLINE","offline"); }
        else if(t === "DEGRADED"){ el.classList.add("DEGRADED","degraded"); }
      });
    }
'''
    # Insert after <script> tag
    s = s.replace("<script>", "<script>\n" + js_inject, 1)

# Call tagStatusBadges() after initial layout and after refreshes
if "tagStatusBadges();" not in s:
    s = re.sub(r"(applySearchSort\(\);\s*)", r"\1tagStatusBadges();\n    ", s, count=1)
    s = re.sub(r"(tickUpdated\(\);\s*)", r"\1tagStatusBadges();\n        ", s, count=1)

if s == orig:
    raise SystemExit("PATCH_FAIL: no changes applied (template structure drifted)")

p.write_text(s, encoding="utf-8")
print("OK Group A applied: removed accent strip, status text colors, compact+center metrics, narrower cards")
