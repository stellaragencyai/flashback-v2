from __future__ import annotations

import re
from pathlib import Path

P = Path("app/dashboard/templates/dashboard.html")

s = P.read_text(encoding="utf-8", errors="ignore")

# ------------------------------------------------------------
# 1) sysPanel: stop blasting the page open by default
# ------------------------------------------------------------
s2 = s.replace('<details class="sysPanel" open>', '<details class="sysPanel">')

# ------------------------------------------------------------
# 2) Replace tall metrics layout (twoCols + triple) -> quad row
#    - Keep JS hooks: js-pnlTotal, js-trades, js-winRate, js-freshHuman
# ------------------------------------------------------------
quad_block = r'''
        <div class="quad">
          <div class="metric">
            <div class="metricK">PnL ({{ window|default("all") }})</div>
            <div class="metricV {% if pnl >= 0 %}pos{% else %}neg{% endif %} js-pnlTotal">${{ "%.2f"|format(pnl) }}</div>
            <div class="metricSub js-pnlAvg">Avg/trade: ${{ "%.2f"|format(pnl_avg) }}</div>
          </div>

          <div class="metric">
            <div class="metricK">Trades</div>
            <div class="metricV js-trades">{{ trades }}</div>
            <div class="metricSub">24h LTM: {{ r.ltm_trades_24h if r.ltm_trades_24h is not none else 0 }}</div>
          </div>

          <div class="metric">
            <div class="metricK">Win %</div>
            <div class="metricV js-winRate">{{ "%.2f"|format(win_pct) }}%</div>
            <div class="metricSub">Mode: {{ mU }}</div>
          </div>

          <div class="metric">
            <div class="metricK">Age</div>
            <div class="metricV mono">
              {% if trades|int > 0 and fresh is not none %}
                <span class="freshHuman js-freshHuman" data-sec="{{ fresh|float }}">—</span>
              {% else %}
                —
              {% endif %}
            </div>
            <div class="metricSub">Last: <span class="js-lastOutcome">{{ last_iso }}</span></div>
          </div>
        </div>
'''.strip("\n")

# Find and replace the first metrics region inside each card
pattern = re.compile(
    r'\n\s*<div class="twoCols">.*?</div>\s*\n\s*<div class="triple">.*?</div>\s*\n',
    re.DOTALL
)

s3, n = pattern.subn("\n" + quad_block + "\n\n", s2, count=1)

if n != 1:
    raise SystemExit(f"PATCH FAILED: expected to replace 1 metrics block, replaced {n}. Template drifted.")

# ------------------------------------------------------------
# 3) Inject PREMIUM UI OVERRIDES v2 (Step 4A)
# ------------------------------------------------------------
marker = "/* PREMIUM UI OVERRIDES v2 (Step 4A) */"
if marker not in s3:
    insert = r'''

/* PREMIUM UI OVERRIDES v2 (Step 4A)
   Goal:
   - Cards: less noise, more hierarchy
   - Metrics: one premium row
   - Header: no pill spam
   - sysPanel: collapsed by default + calmer
*/
.card{ padding: 12px !important; }
.header{ margin-bottom: 8px !important; }
.avatar{ width: 32px !important; height: 32px !important; border-radius: 10px !important; }
.acctName{ font-size: 14px !important; font-weight: 900 !important; }
.acctMeta{ gap: 6px !important; }

/* Kill header chip clutter (keep ONLY Mode badge, everything else goes to Details) */
.acctMeta .chip{ display: none !important; }
.acctMeta .badge{ display: none !important; }
.acctMeta .badge.js-mode{ display: inline-flex !important; }

/* Status badge stays the hero (already styled earlier via data-status rules) */
.rightPills{ gap: 8px !important; }
.statusReason{ max-width: 220px !important; }

/* Premium metrics row */
.quad{
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 10px;
  margin: 10px 0 8px 0;
  padding-left: 6px;
  position: relative;
  z-index: 2;
}
@media (max-width: 1180px){
  .quad{ grid-template-columns: repeat(2, 1fr); }
}
@media (max-width: 820px){
  .quad{ grid-template-columns: 1fr; }
}

.metric{
  border: 1px solid rgba(148,163,184,.18);
  border-radius: 14px;
  padding: 10px 12px;
  background: var(--surface);
  display:flex;
  flex-direction: column;
  justify-content: space-between;
  min-height: 88px;
}

.metricK{
  font-size: 11px;
  color: var(--muted);
  font-weight: 750;
  letter-spacing: .15px;
  white-space: nowrap;
  overflow:hidden;
  text-overflow: ellipsis;
}
.metricV{
  margin-top: 6px;
  font-size: 16px;
  font-weight: 950;
  letter-spacing: .15px;
  line-height: 1.1;
}
.metricSub{
  margin-top: 6px;
  font-size: 11px;
  color: var(--muted);
  font-weight: 750;
  white-space: nowrap;
  overflow:hidden;
  text-overflow: ellipsis;
}

/* Remove the now-unused spacer height padding so cards shrink */
.alertSpacer{ min-height: 12px !important; margin-top: 6px !important; }

/* sysPanel: calm + not screaming */
details.sysPanel{
  border: 1px solid rgba(148,163,184,.18) !important;
  background: rgba(148,163,184,.05) !important;
  border-radius: 14px !important;
  padding: 10px 12px !important;
}
details.sysPanel summary{
  font-size: 11px !important;
  font-weight: 900 !important;
  letter-spacing: .25px !important;
}
'''.strip("\n")

    # Insert before </style>
    s3 = s3.replace("</style>", "\n" + insert + "\n\n</style>")

P.write_text(s3, encoding="utf-8")
print("OK: Step 4A applied -> dashboard.html updated")
