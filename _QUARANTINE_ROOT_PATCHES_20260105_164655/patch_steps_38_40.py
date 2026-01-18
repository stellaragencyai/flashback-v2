from pathlib import Path
import re

p = Path(r"app/dashboard/templates/dashboard.html")
s = p.read_text(encoding="utf-8", errors="ignore")
orig = s

# -----------------------------
# Step 38: PnL label REAL vs SIM
# -----------------------------
old_pnl = '<div class="miniK">PnL Total ({{ window|default("all") }})</div>'
new_pnl = '<div class="miniK">PnL Total ({% if r.funds_source == "REAL" %}REAL{% else %}SIM{% endif %}, {{ window|default("all") }})</div>'
if old_pnl in s and new_pnl not in s:
    s = s.replace(old_pnl, new_pnl, 1)

# ---------------------------------------------------------
# Fix: ensure trades is defined BEFORE stale/dead uses trades
# ---------------------------------------------------------
# In your file, stale/dead_outcomes appears before trades assignment.
# We'll move the trades line up by inserting it just before the stale calc
# (only if we detect the pattern).
pat_stale = r'(\{% set stale = .*?%\}\s*\n\s*\{% set dead_outcomes = .*?%\})'
m_stale = re.search(pat_stale, s, flags=re.DOTALL)
if m_stale:
    # If trades already defined above stale, skip. Otherwise insert trades line above stale.
    before = s[:m_stale.start()]
    if 'set trades =' not in before[-1500:]:
        trades_line = '      {% set trades = (r.total_trades if r.total_trades is not none else 0) %}\n\n'
        s = s[:m_stale.start()] + trades_line + s[m_stale.start():]

# --------------------------------------------
# Step 39: color derived from status_norm only
# --------------------------------------------
# Replace: {% set color = (r.status_tier or 'gray') %}
color_old = "{% set color = (r.status_tier or 'gray') %}"
if color_old in s:
    color_new = """{% set color = "gray" %}
      {% if status_norm == "ONLINE" %}{% set color = "green" %}
      {% elif status_norm == "DEGRADED" %}{% set color = "yellow" %}
      {% elif status_norm == "OFFLINE" %}{% set color = "gray" %}
      {% elif status_norm == "STOPPED" %}{% set color = "gray" %}
      {% endif %}"""
    s = s.replace(color_old, color_new, 1)

# ------------------------------------------------
# Step 40: add a micro "reason" line under status
# ------------------------------------------------
# Add CSS once (statusReason)
if ".statusReason" not in s:
    css_inject_point = "</style>"
    css_add = """
    .statusReason{
      font-size: 11px;
      color: var(--muted);
      font-weight: 800;
      text-align: right;
      max-width: 260px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      opacity: .9;
    }
"""
    s = s.replace(css_inject_point, css_add + "\n" + css_inject_point, 1)

# Insert reason line right after the status badge inside rightPills
# We target the exact snippet present in your pasted file.
badge_snip = '<div class="badge {{ color }} js-status">{{ status_norm }}</div>'
if badge_snip in s and "statusReason" not in s[s.find(badge_snip):s.find(badge_snip)+600]:
    reason_block = badge_snip + """
            {% if status_norm != "ONLINE" %}
              {% set why = "" %}
              {% if r.status_reason %}{% set why = r.status_reason %}
              {% elif r.orch_reason %}{% set why = r.orch_reason %}
              {% elif r.orch_status and (r.orch_status|string|upper) == "SKIPPED" %}{% set why = "SKIPPED: " ~ (r.orch_reason or "not scheduled") %}
              {% elif status_norm == "STOPPED" %}{% set why = "Disabled" %}
              {% elif status_norm == "OFFLINE" %}{% set why = "Supervisor not running" %}
              {% elif status_norm == "DEGRADED" %}{% set why = "Degraded workers / supervisor" %}
              {% else %}{% set why = "Unknown" %}
              {% endif %}
              {% if why %}
                <div class="statusReason" title="{{ why }}">{{ why }}</div>
              {% endif %}
            {% endif %}
"""
    s = s.replace(badge_snip, reason_block, 1)

if s == orig:
    raise SystemExit("PATCH_NOOP: no changes applied (patterns not found or already applied)")

p.write_text(s, encoding="utf-8")
print("OK Steps 38-40 applied safely (patch-only).")
