from pathlib import Path
import re

# -----------------------------
# Patch dashboard.html (Steps 22/23/24 UI)
# -----------------------------
tpl = Path(r"app/dashboard/templates/dashboard.html")
s = tpl.read_text(encoding="utf-8", errors="ignore")

orig = s

# (A) Force badge color variants to render neutral (Step 22)
# Replace the colored badge CSS definitions with neutral equivalents
s = re.sub(
    r"\.badge\.green\{[^}]*\}\s*\.badge\.red\{[^}]*\}\s*\.badge\.yellow\{[^}]*\}\s*\.badge\.gray\{[^}]*\}",
    """.badge.green{ background: var(--chip); border-color: var(--border); color: var(--chipText); }
    .badge.red{ background: var(--chip); border-color: var(--border); color: var(--chipText); }
    .badge.yellow{ background: var(--chip); border-color: var(--border); color: var(--chipText); }
    .badge.gray{ background: var(--chip); border-color: var(--border); color: var(--chipText); }""",
    s,
    flags=re.S
)

# (B) Make the right-side status badge neutral (Step 22)
s = s.replace('<div class="badge {{ color }}">{{ status_norm }}</div>', '<div class="badge">{{ status_norm }}</div>')

# (C) Make FUNDS badge neutral (Step 22)
s = re.sub(r'<span class="badge \{\% if is_live \%\}green\{\% else \%\}red\{\% endif \%\}">FUNDS: \{\{ funds_source \}\}</span>',
           '<span class="badge">FUNDS: {{ funds_source }}</span>', s)

# (D) Make MODE badge neutral (Step 22)
s = re.sub(r'<span class="badge \{\% if is_live \%\}green\{\% else \%\}gray\{\% endif \%\}">\{\{ mU \}\}</span>',
           '<span class="badge">{{ mU }}</span>', s)

# (E) Remove OUTCOMES STALE/DEAD badge in the card header (Step 22/23)
# Deletes the whole block: {% if stale %} ... {% endif %} inside rightPills
s = re.sub(
    r'\s*\{\%\s*if\s*stale\s*\%\}.*?\{\%\s*endif\s*\%\}\s*',
    "\n",
    s,
    flags=re.S
)

# (F) Fix stale/dead logic so it only triggers when there are trades (Step 23)
s = s.replace(
    '{% set stale = (true if (fresh is not none and fresh|float > 3600) else false) %}',
    '{% set stale = (true if (trades|int > 0 and fresh is not none and fresh|float > 3600) else false) %}'
)
s = s.replace(
    '{% set dead_outcomes = (true if (fresh is not none and fresh|float > 86400) else false) %}',
    '{% set dead_outcomes = (true if (trades|int > 0 and fresh is not none and fresh|float > 86400) else false) %}'
)

# (G) Freshness display becomes — when no trades (Step 23)
# Replace the Freshness cell condition
s = re.sub(
    r'\{\%\s*if\s*fresh\s*is\s*not\s*none\s*\%\}',
    '{% if trades|int > 0 and fresh is not none %}',
    s
)

# (H) Add Present/Missing chips in the topbar (Step 24)
# We look for the meta chips block inserted earlier and extend it safely.
needle = '<span class="chip">Expected: {{ (meta.expected_accounts|length) if (meta and meta.expected_accounts) else "?" }}</span>'
if needle in s:
    inject = needle + '\n        <span class="chip">Present: {{ (meta.present_count) if (meta and (meta.present_count is not none)) else "?" }}</span>\n        <span class="chip">Missing: {{ (meta.missing_count) if (meta and (meta.missing_count is not none)) else "?" }}</span>'
    s = s.replace(needle, inject, 1)

if s == orig:
    raise SystemExit("PATCH_FAIL: dashboard.html did not change (unexpected).")

tpl.write_text(s, encoding="utf-8")
print("OK patched dashboard.html (steps 22/23/24 UI)")

# -----------------------------
# Patch data_hydrator_v1.py (Step 24 meta truth)
# -----------------------------
py = Path(r"app/dashboard/data_hydrator_v1.py")
t = py.read_text(encoding="utf-8", errors="ignore")
t0 = t

# Inject fleet presence fields right before 'return meta' inside hydrate_dashboard_meta
# This is structure-safe: no signature changes, no route shape changes.
if "def hydrate_dashboard_meta" not in t:
    raise SystemExit("PATCH_FAIL: hydrate_dashboard_meta not found in data_hydrator_v1.py")

if "present_accounts" not in t:
    t = re.sub(
        r"(\n\s*return\s+meta\s*\n)",
        r"""
    # Fleet presence truth (expected vs present vs missing)
    try:
        _subs = _load_subaccounts_state()
        present_accounts = []
        for _sa in (_subs or []):
            if isinstance(_sa, dict):
                _lab = _sa.get("account_label") or _sa.get("account") or _sa.get("subaccount_name")
                if isinstance(_lab, str) and _lab.strip():
                    present_accounts.append(_lab.strip())
        present_accounts = sorted(set(present_accounts), key=lambda x: (x == "main", x))
    except Exception:
        present_accounts = []

    _exp = meta.get("expected_accounts") or []
    exp_set = {str(x).strip() for x in _exp if str(x).strip()}
    pres_set = {str(x).strip() for x in present_accounts if str(x).strip()}

    missing_accounts = sorted(list(exp_set - pres_set), key=lambda x: (x == "main", x))

    meta["present_accounts"] = present_accounts
    meta["present_count"] = len(pres_set)
    meta["missing_accounts"] = missing_accounts
    meta["missing_count"] = len(missing_accounts)
\1
""",
        t,
        flags=re.S
    )

if t == t0:
    raise SystemExit("PATCH_FAIL: data_hydrator_v1.py did not change (unexpected).")

py.write_text(t, encoding="utf-8")
print("OK patched data_hydrator_v1.py (step 24 meta truth)")
