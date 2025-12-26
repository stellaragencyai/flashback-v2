from pathlib import Path
import re

p = Path(r"config\fleet_manifest.yaml")
lines = p.read_text(encoding="utf-8", errors="replace").splitlines(True)

# 1) Normalize tabs -> 2 spaces (tabs in YAML = death)
lines = [ln.replace("\t", "  ") for ln in lines]

# 2) Repair promotion_rules indentation inside fleet entries:
# We enforce:
#   - account entry fields at indent 2
#   promotion_rules: at indent 2
#   promotion_rules children at indent 4
in_fleet = False
in_promo = False
fixed = 0

for i in range(len(lines)):
    raw = lines[i]
    s = raw.rstrip("\r\n")
    stripped = s.lstrip(" ")
    indent = len(s) - len(stripped)

    # Enter fleet
    if stripped == "fleet:":
        in_fleet = True
        in_promo = False
        continue

    if in_fleet:
        # Exit fleet on new top-level key
        if indent == 0 and re.match(r"^[A-Za-z_][A-Za-z0-9_]*\s*:", stripped) and stripped != "fleet:":
            in_fleet = False
            in_promo = False
            continue

        # Start of a new fleet row
        if stripped.startswith("- account_label:"):
            in_promo = False
            continue

        # Detect promotion_rules header
        if stripped.startswith("promotion_rules:"):
            # force indent to 2 spaces
            if indent != 2:
                lines[i] = "  promotion_rules:\n"
                fixed += 1
            in_promo = True
            continue

        # If inside promotion_rules block, ensure child keys are indent 4
        if in_promo:
            # promotion_rules block ends when we hit another indent<=2 key (or a new list item)
            if (indent <= 2 and stripped) or stripped.startswith("- "):
                in_promo = False
                continue

            # Fix child keys like enabled/min_trades/etc to indent 4
            if re.match(r"^(enabled|min_trades|min_winrate|min_avg_r|min_expectancy_r|max_drawdown_pct)\s*:", stripped):
                if indent != 4:
                    lines[i] = "    " + stripped + "\n"
                    fixed += 1
                continue

# Write back
p.write_text("".join(lines), encoding="utf-8")
print(f"OK: promotion_rules_indent_fixes={fixed}")
