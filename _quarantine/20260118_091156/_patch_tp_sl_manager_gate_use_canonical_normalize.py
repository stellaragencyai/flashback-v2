import pathlib, re

p = pathlib.Path(r"app\bots\tp_sl_manager.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# 1) Ensure we import normalize_trade_id alongside enforce_decision
# Try a few import patterns safely.
if "normalize_trade_id" not in s:
    # Case A: from app.ai.ai_decision_enforcer import enforce_decision
    s2 = re.sub(
        r"from\s+app\.ai\.ai_decision_enforcer\s+import\s+enforce_decision\s*",
        "from app.ai.ai_decision_enforcer import enforce_decision, normalize_trade_id\n",
        s,
        count=1,
    )
    if s2 == s:
        # Case B: enforce_decision imported in a tuple or multi-import line
        s2 = re.sub(
            r"from\s+app\.ai\.ai_decision_enforcer\s+import\s+([^\n]+)",
            lambda m: (m.group(0) if "normalize_trade_id" in m.group(1) else f"from app.ai.ai_decision_enforcer import {m.group(1).strip()}, normalize_trade_id"),
            s,
            count=1,
        )
    s = s2

# 2) Patch _gate_allows_trade to normalize using normalize_trade_id()
# We look for the function and the enforce_decision call. Then replace the block that sets lab/tid_norm and calls enforce_decision.
m = re.search(r"def\s+_gate_allows_trade\s*\(.*?\):", s)
if not m:
    raise SystemExit("PATCH_FAILED: could not find def _gate_allows_trade")

# Replace any manual tid_norm construction + enforce_decision(...) with canonical normalize_trade_id + enforce_decision
# This is intentionally conservative: we only rewrite inside the function around enforce_decision usage.
pattern = r"(def\s+_gate_allows_trade\s*\(.*?\):)(.*?)(\n\s*return\s+\(True,\s*[^\)]*\)\s*)"
mm = re.search(pattern, s, flags=re.DOTALL)
if not mm:
    raise SystemExit("PATCH_FAILED: could not bracket _gate_allows_trade body for patching")

head, body, tailret = mm.group(1), mm.group(2), mm.group(3)

# Find the enforce_decision call in the body
if "enforce_decision" not in body:
    raise SystemExit("PATCH_FAILED: _gate_allows_trade does not call enforce_decision (unexpected layout)")

# Inject a canonical normalization block right before the enforce_decision call.
# We will:
# - derive lab from env OR from trade_id prefix
# - compute tid_norm = normalize_trade_id(trade_id, account_label=lab)
# - call enforce_decision(tid_norm, account_label=lab)
# - keep existing verdict handling logic as-is (we only swap the inputs)
inject = r"""
        # --- CANONICAL TRADE_ID NORMALIZATION (GATE) ---
        tid_in = str(trade_id or "").strip()
        lab = os.getenv("ACCOUNT_LABEL", "").strip()
        if not lab:
            if ":" in tid_in:
                lab = tid_in.split(":", 1)[0].strip()
            elif "-" in tid_in:
                lab = tid_in.split("-", 1)[0].strip()
        tid_norm = normalize_trade_id(tid_in, account_label=lab)
        # ------------------------------------------------
"""

# Remove any previous local definitions of tid_in/tid_norm/lab right before enforce_decision to avoid conflicts.
# We do a light cleanup: drop lines that assign to tid_in / tid_norm / lab in the 80 lines before enforce_decision.
lines = body.splitlines(True)
idx = None
for i, ln in enumerate(lines):
    if "enforce_decision" in ln:
        idx = i
        break
if idx is None:
    raise SystemExit("PATCH_FAILED: could not locate enforce_decision line index")

start = max(0, idx - 120)
clean = []
for i, ln in enumerate(lines):
    if start <= i < idx:
        if re.search(r"\b(tid_in|tid_norm|lab)\s*=", ln):
            continue
    clean.append(ln)
body_clean = "".join(clean)

# Now ensure the enforce_decision line uses tid_norm and lab (account_label)
body_clean = re.sub(
    r"enforce_decision\s*\(\s*([^\),]+)\s*(,\s*account_label\s*=\s*([^\)]+))?\s*\)",
    "enforce_decision(tid_norm, account_label=lab)",
    body_clean,
    count=1,
)

# Insert inject block right before the enforce_decision(...) call (same indentation as that line)
body_lines = body_clean.splitlines(True)
out = []
inserted = False
for ln in body_lines:
    if (not inserted) and ("enforce_decision" in ln):
        indent = re.match(r"^(\s*)", ln).group(1)
        inj = "\n".join(indent + x if x.strip() else x for x in inject.splitlines())
        out.append(inj + "\n")
        inserted = True
    out.append(ln)

if not inserted:
    raise SystemExit("PATCH_FAILED: inject point not found")

new_body = "".join(out)
s_new = s[:mm.start()] + head + new_body + tailret + s[mm.end():]

p.write_text(s_new, encoding="utf-8")
print("PATCH_OK:", str(p))
