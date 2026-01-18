from pathlib import Path
import re

p = Path(r"app/dashboard/templates/dashboard.html")
s = p.read_text(encoding="utf-8", errors="ignore")

# -----------------------------
# 1) CSS: add 'historical' class styling + chip spacing normalization (Step 36/37)
# -----------------------------
if ".card.historical" not in s:
    css_pat = r"(\.dim\{[^}]*\}\s*)"
    m = re.search(css_pat, s, flags=re.DOTALL)
    if not m:
        raise SystemExit("PATCH_FAIL: could not find CSS anchor near .dim{}")

    css_add = m.group(1) + """

    /* Step 36: de-emphasize historical (not running / skipped) cards without adding red noise */
    .card.historical .twoCols,
    .card.historical .triple,
    .card.historical .chips,
    .card.historical .footer{
      opacity: .78;
    }

    /* Step 37: align meta/chips visually across cards */
    .acctMeta{ line-height: 1.25; }
    .chip, .badge{ line-height: 1.2; }
    .acctMeta .chip, .acctMeta .badge{ margin-top: 2px; }
"""
    s = s.replace(m.group(1), css_add, 1)
    print("OK Step 36/37: CSS added")
else:
    print("OK Step 36/37: CSS already present")

# -----------------------------
# 2) Add MODE MISMATCH chip in acctMeta (Step 35)
# -----------------------------
if "MODE MISMATCH" not in s:
    # insert after Strategy chip span
    pat_strat = r"(\<span class=\"chip\"\>\s*Strategy:\s*\{\{\s*strat\s*\}\}\s*\<\/span\>)"
    m = re.search(pat_strat, s, flags=re.DOTALL)
    if not m:
        raise SystemExit("PATCH_FAIL: could not find Strategy chip span for Step 35 insertion")

    add = m.group(1) + """
                {% if r.mode_mismatch %}
                  <span class="chip" style="opacity:.75;">MODE MISMATCH</span>
                {% endif %}
"""
    s = s.replace(m.group(1), add, 1)
    print("OK Step 35: MODE MISMATCH chip inserted")
else:
    print("OK Step 35: already present")

# -----------------------------
# 3) Add HISTORICAL label under Last Outcome when not running/skipped (Step 34)
# -----------------------------
if "Historical (not running)" not in s:
    # Find the "Last Outcome" mini card block and insert label under Updated line.
    # Anchor on 'Updated:' span to keep stable.
    pat_updated = r"(Updated:\s*\<span class=\"mono updatedAgo\"\>—\<\/span\>)"
    m = re.search(pat_updated, s, flags=re.DOTALL)
    if not m:
        raise SystemExit("PATCH_FAIL: could not find Updated: ... updatedAgo anchor")

    add = m.group(1) + """
            {% if (r.orch_status == "SKIPPED") or (not is_online) %}
              <div class="miniK" style="margin-top:6px; opacity:.75;">Historical (not running)</div>
            {% endif %}
"""
    s = s.replace(m.group(1), add, 1)
    print("OK Step 34: Historical label inserted")
else:
    print("OK Step 34: already present")

# -----------------------------
# 4) Add 'historical' card flag class when skipped/offline (Step 36)
# -----------------------------
# You already build cardFlags string. We append " historical" when SKIPPED or not online.
if " historical" not in s:
    # Find where cardFlags is defined and append conditional
    # Anchor on: {% set cardFlags = "" %}
    anchor = '{% set cardFlags = "" %}'
    if anchor not in s:
        raise SystemExit("PATCH_FAIL: could not find cardFlags anchor")

    # Inject after SIM/stale/dead flags (best-effort): place after the stale/dead block
    # We'll add a new conditional after existing dead/stale append block if present.
    pat_flags = r"(\{% if dead_outcomes %\}\{% set cardFlags = cardFlags ~ \" dead\" %\}\{% elif stale %\}\{% set cardFlags = cardFlags ~ \" stale\" %\}\{% endif %\})"
    m = re.search(pat_flags, s)
    if not m:
        # fallback: append right after cardFlags init
        s = s.replace(anchor, anchor + "\n      {% if (r.orch_status == \"SKIPPED\") or (not is_online) %}{% set cardFlags = cardFlags ~ \" historical\" %}{% endif %}", 1)
        print("OK Step 36: historical flag added (fallback placement)")
    else:
        inject = m.group(1) + "\n      {% if (r.orch_status == \"SKIPPED\") or (not is_online) %}{% set cardFlags = cardFlags ~ \" historical\" %}{% endif %}"
        s = s.replace(m.group(1), inject, 1)
        print("OK Step 36: historical flag added")
else:
    print("OK Step 36: historical flag likely already present (skipping)")

p.write_text(s, encoding="utf-8")
print("OK Steps 34–37 applied to dashboard.html")
