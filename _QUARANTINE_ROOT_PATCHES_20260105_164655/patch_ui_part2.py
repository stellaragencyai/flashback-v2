from pathlib import Path
import re

p = Path(r"app/dashboard/templates/dashboard.html")
s = p.read_text(encoding="utf-8", errors="ignore")
orig = s

# -------------------------------
# A) Clean up TOP SUBHEAD clutter
# -------------------------------
# Replace the whole <div class="subhead"> ... </div> with a compact version.
subhead_pat = r'<div class="subhead">.*?</div>\s*\n'
subhead_new = r'''<div class="subhead">
        <span>Auto-refresh every 4s.</span>
        <span class="truthPill keep">Cards: {{ rows|length }}</span>
        <span class="truthPill keep">Mode: {{ (meta.orch_mode if meta else "?")|upper }}</span>
        <span class="truthPill keep">Subset: {{ (meta.orch_only_labels|join(", ")) if (meta and meta.orch_only_labels) else "ALL" }}</span>
        <span class="truthPill keep">Last refresh: <span id="lastRefresh">—</span></span>
      </div>
'''
s, n1 = re.subn(subhead_pat, subhead_new, s, flags=re.S)

# ------------------------------------------
# B) Replace GLOBAL TRUTH banner with compact
# ------------------------------------------
# Replace <div class="metaBanner"> ... </div> with a calmer "sysPanel"
meta_pat = r'<div class="metaBanner">.*?</div>\s*\n\s*{# ------------------------------------------------------------ #}\s*\n\s*{# Summary tiles computed from rows.*?#}\s*\n'
# We'll reinsert the summary-section comment header right after.
meta_new = r'''<div class="sysPanel">
    <div class="sysLeft">
      <div class="sysTitle">System Status</div>
      <div class="sysMeta">
        <span class="sysPill"><span class="sysK">Window</span><span class="mono">{{ window|default("all") }}</span></span>

        {% if orch_mode %}
          {% set om = (orch_mode|string)|upper %}
          <span class="sysPill"><span class="sysK">Orch</span><span class="mono">{{ om }}</span></span>
        {% else %}
          <span class="sysPill"><span class="sysK">Orch</span><span class="mono">UNKNOWN</span></span>
        {% endif %}

        <span class="sysPill">
          <span class="sysK">Fleet</span>
          <span class="mono">{{ present_n }}</span>/<span class="mono">{{ expected_n }}</span>
          {% if missing_n > 0 %}<span class="sysWarn">(+{{ missing_n }} missing)</span>{% endif %}
        </span>

        <span class="sysPill">
          <span class="sysK">Subset</span>
          <span class="mono">
            {% if orch_only is iterable and only_n > 0 %}
              {{ orch_only|join(", ") }}
            {% else %}
              ALL
            {% endif %}
          </span>
          {% if running_subset %}<span class="sysWarn">(subset)</span>{% endif %}
        </span>

        {# Outcomes freshness state #}
        {% set of = (m.get('outcomes_freshness_sec') if m else none) %}
        {% set ostate = "?" %}
        {% if of is not none %}
          {% if of|float <= 3600 %}{% set ostate = "OK" %}
          {% elif of|float <= 86400 %}{% set ostate = "STALE" %}
          {% else %}{% set ostate = "DEAD" %}
          {% endif %}
        {% endif %}
        <span class="sysPill">
          <span class="sysK">Outcomes</span>
          <span class="mono">{{ ostate }}</span>
          {% if m and m.get('outcomes_last_ts_iso') %}
            <span class="sysDim mono">({{ m.get('outcomes_last_ts_iso') }})</span>
          {% endif %}
        </span>
      </div>
    </div>

    <div class="sysRight">
      {% if running_subset %}
        <div class="sysNote">You’re running a subset. Dashboard shows all accounts, orchestrator is scheduling only some.</div>
      {% else %}
        <div class="sysNote">Truth-first UI: minimal noise, maximum signal.</div>
      {% endif %}
    </div>
  </div>

  {# ------------------------------------------------------------ #}
  {# Summary tiles computed from rows (truth-first, fail-soft).     #}
  {# ------------------------------------------------------------ #}
'''
s, n2 = re.subn(meta_pat, meta_new, s, flags=re.S)

# ------------------------------------------
# C) Add System Status panel CSS (quiet style)
# ------------------------------------------
css_marker = "/* PREMIUM UI OVERRIDES v1 */"
if css_marker in s and "/* SYSTEM STATUS PANEL v1 */" not in s:
    inject = r"""
/* SYSTEM STATUS PANEL v1 (quiet, premium, minimal color) */
.sysPanel{
  border: 1px solid rgba(148,163,184,.18);
  background: rgba(148,163,184,.06);
  border-radius: 14px;
  padding: 12px 14px;
  margin: 0 0 16px 0;
  display:flex;
  justify-content:space-between;
  gap: 14px;
  flex-wrap: wrap;
}
html[data-theme="dark"] .sysPanel{
  border-color: rgba(148,163,184,.14);
  background: rgba(148,163,184,.06);
}

.sysTitle{
  font-size: 11px;
  font-weight: 950;
  letter-spacing: .35px;
  text-transform: uppercase;
  color: var(--muted);
  margin-bottom: 8px;
}

.sysMeta{
  display:flex;
  flex-wrap: wrap;
  gap: 8px;
  align-items:center;
}

.sysPill{
  display:inline-flex;
  gap: 8px;
  align-items:center;
  padding: 6px 10px;
  border-radius: 999px;
  border: 1px solid rgba(148,163,184,.18);
  background: rgba(148,163,184,.08);
  color: var(--text);
  font-size: 11px;
  font-weight: 850;
  white-space: nowrap;
}

.sysK{
  color: var(--muted);
  font-weight: 900;
  letter-spacing: .2px;
}

.sysWarn{
  color: var(--yellow);
  font-weight: 950;
}

.sysDim{ color: var(--muted); font-weight: 850; }

.sysRight{
  display:flex;
  align-items:flex-end;
  justify-content:flex-end;
  flex: 1 1 auto;
  min-width: 240px;
}

.sysNote{
  color: var(--muted);
  font-size: 12px;
  font-weight: 750;
  text-align: right;
  max-width: 520px;
  line-height: 1.25;
}
"""
    s = s.replace(css_marker, css_marker + "\n" + inject)

# ------------------------------------------
# D) Remove old metaBanner CSS if present
# (We keep it if other parts still reference, but it’s now unused.)
# ------------------------------------------
# Optional: no deletion here to avoid accidental drift.

if s == orig:
    print("PATCH_NOOP: Part 2 not applied (structure drifted or already applied)")
    print(f"DEBUG: subhead_replaced={n1}, meta_replaced={n2}")
else:
    p.write_text(s, encoding="utf-8")
    print("OK: Part 2 applied (topbar declutter + System Status strip)")

