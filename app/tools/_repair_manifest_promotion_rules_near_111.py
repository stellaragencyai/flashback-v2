from pathlib import Path
import re

p = Path(r"config\fleet_manifest.yaml")
L = p.read_text(encoding="utf-8", errors="replace").splitlines(True)

# Convert tabs to spaces (tabs in YAML are poison)
L = [ln.replace("\t", "  ") for ln in L]

target_line = 111  # the reported failure line
i = max(0, target_line - 1)

# Walk upward to find the nearest "promotion_rules" or the start of the current fleet item
start = None
promo = None
for j in range(i, -1, -1):
    s = L[j].rstrip("\r\n")
    if re.match(r"^\s*promotion_rules\s*:\s*$", s):
        promo = j
        break
    if re.match(r"^\s*-\s*account_label\s*:\s*", s):
        start = j
        break

fixed = 0

def indent_of(s: str) -> int:
    return len(s) - len(s.lstrip(" "))

if promo is None:
    # promotion_rules header missing: insert it just before the enabled:false line (or nearest enabled line)
    # find nearest enabled: false within +-10 lines
    k = None
    for j in range(max(0, i-10), min(len(L), i+10)):
        if re.match(r"^\s*enabled\s*:\s*(true|false)\s*$", L[j].rstrip("\r\n")):
            k = j
            break
    if k is None:
        raise SystemExit("FATAL: Could not find an 'enabled: true/false' line near the reported error to anchor the fix.")
    # Insert promotion_rules header at indent 2
    L.insert(k, "  promotion_rules:\n")
    promo = k
    fixed += 1

# Now enforce indentation rules inside promotion_rules block
# promotion_rules line should be indent 2
if indent_of(L[promo].rstrip("\r\n")) != 2:
    L[promo] = "  promotion_rules:\n"
    fixed += 1

# Children should be indent 4 until block ends
for j in range(promo+1, len(L)):
    s = L[j].rstrip("\r\n")
    if not s.strip():
        continue
    ind = indent_of(s)
    # Block ends if indent <= 2 and it looks like a key or new list item
    if ind <= 2 and (re.match(r"^\s*-\s*", s) or re.match(r"^\s*[A-Za-z_].*:\s*", s)):
        break
    # Fix known keys
    m = re.match(r"^\s*(enabled|min_trades|min_winrate|min_avg_r|min_expectancy_r|max_drawdown_pct)\s*:\s*(.*)$", s)
    if m:
        key = m.group(1)
        rest = m.group(2)
        L[j] = f"    {key}: {rest}\n"
        fixed += 1

p.write_text("".join(L), encoding="utf-8")
print(f"OK: promotion_rules_block_repaired edits={fixed}")
