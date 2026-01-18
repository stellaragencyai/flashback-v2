from pathlib import Path
import re

p = Path(r"app/dashboard/templates/dashboard.html")
s = p.read_text(encoding="utf-8", errors="ignore")

# ----------------------------
# STEP 25: Fleet Mode Banner
# ----------------------------
# Insert banner right after the topbar block (after </div> that closes .topbar)
banner_html = r'''
  <div class="fleetBanner">
    <div class="fleetLeft">
      <span class="fleetChip"><span class="dim">Orchestrator</span>: <span class="mono">{{ meta.orch_mode if meta else "?" }}</span></span>
      <span class="fleetChip"><span class="dim">Subset</span>: <span class="mono">{{ (meta.orch_only_labels|join(", ")) if (meta and meta.orch_only_labels) else "ALL" }}</span></span>
      <span class="fleetChip"><span class="dim">Expected</span>: <span class="mono">{{ (meta.expected_accounts|length) if (meta and meta.expected_accounts) else "?" }}</span></span>
      <span class="fleetChip"><span class="dim">Started</span>: <span class="mono">{{ (meta.orch_started|length) if (meta and meta.orch_started) else 0 }}</span></span>
      <span class="fleetChip"><span class="dim">Skipped</span>: <span class="mono">{{ (meta.orch_skipped|length) if (meta and meta.orch_skipped) else 0 }}</span></span>
    </div>
    <div class="fleetRight">
      <span class="fleetChip"><span class="dim">Outcomes</span>:
        {% if meta and meta.outcomes_exists %}
          <span class="mono">{{ meta.outcomes_last_ts_iso or "—" }}</span>
          <span class="dim">·</span>
          <span class="mono">{{ "%.0f"|format(meta.outcomes_freshness_sec or 0) }}s</span>
        {% else %}
          <span class="mono">MISSING</span>
        {% endif %}
      </span>
    </div>
  </div>
'''

# Add CSS for banner + alignment lock + dark mode safety (minimal and non-breaking)
css_inject = r'''
    /* ----------------------------
       Step 25: Fleet Mode Banner
       ---------------------------- */
    .fleetBanner{
      margin: 10px 0 16px 0;
      padding: 10px 12px;
      border: 1px solid var(--border);
      border-radius: 14px;
      background: var(--card);
      box-shadow: var(--shadow);
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap: 12px;
      flex-wrap: wrap;
    }
    .fleetLeft, .fleetRight{ display:flex; gap: 8px; flex-wrap: wrap; align-items:center; }
    .fleetChip{
      display:inline-flex;
      gap: 6px;
      align-items:baseline;
      padding: 6px 10px;
      border-radius: 999px;
      border: 1px solid var(--border);
      background: var(--chip);
      color: var(--chipText);
      font-size: 12px;
      font-weight: 800;
      white-space: nowrap;
    }

    /* ----------------------------
       Step 26: Alignment Lock
       ---------------------------- */
    .header{ min-height: 64px; align-items:flex-start; }
    .twoCols{ min-height: 124px; }
    .triple{ min-height: 86px; align-items:stretch; }
    .mini, .stat{ height: 100%; display:flex; flex-direction:column; justify-content:space-between; }
    .acctMeta{ min-height: 22px; }
    .rightPills{ min-height: 26px; align-content:flex-start; }

    /* Tighten spacing just a bit */
    .card{ padding: 14px; }
    .chips{ margin-top: 8px; }
'''

# 1) Inject CSS: place it near the end of :root/styles block (before </style>)
if "</style>" not in s:
    raise SystemExit("PATCH_FAIL: </style> not found")

if ".fleetBanner" not in s:
    s = s.replace("</style>", css_inject + "\n  </style>", 1)

# 2) Inject banner under topbar (only once)
if "fleetBanner" not in s:
    # Find the first occurrence of the closing topbar div
    # We anchor on '<div class="topbar">' block end: the next line after it ends is ' {# Summary tiles... #}'
    marker = "{# ------------------------------------------------------------ #}\n  {# Summary tiles computed from rows (truth-first, fail-soft).     #}"
    if marker not in s:
        raise SystemExit("PATCH_FAIL: summary marker not found for banner injection")
    s = s.replace(marker, banner_html + "\n\n  " + marker, 1)

# ----------------------------
# STEP 27: UI Noise Gate
# ----------------------------
# A) Remove OUTCOMES STALE/DEAD badge in the card header pills
# Remove the whole block:
# {% if stale %} ... badge ... {% endif %}
s = re.sub(
    r"\n\s*{% if stale %}\s*\n\s*<div class=\"badge[^>]*>[\s\S]*?<\/div>\s*\n\s*{% endif %}\s*\n",
    "\n",
    s,
    count=1
)

# B) Remove alertBox blocks inside card (both dead_outcomes and stale)
# Remove the entire conditional alertBox area
s = re.sub(
    r"\n\s*{% if dead_outcomes %}[\s\S]*?{% endif %}\s*\n",
    "\n",
    s,
    count=1
)

# C) Ensure Details remains the only place where noisy ops/debug chips live (already true)

p.write_text(s, encoding="utf-8")
print("OK patched dashboard.html (Steps 25/26/27 applied)")
