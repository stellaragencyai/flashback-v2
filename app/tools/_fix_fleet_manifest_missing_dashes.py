from pathlib import Path
import re

p = Path(r"config\fleet_manifest.yaml")
s = p.read_text(encoding="utf-8", errors="replace")
lines = s.splitlines(True)

# Heuristic repair:
# If we see an "enabled:" line at indentation level 2+ where YAML expects a new mapping key,
# the usual cause is that the *previous* fleet entry header is missing "- account_label:".
#
# We'll search for any "account_label:" lines inside fleet that do NOT start with "- "
# and fix them.

in_fleet = False
fixed = 0

for i in range(len(lines)):
    raw = lines[i]
    stripped = raw.lstrip()
    indent = len(raw) - len(stripped)

    if stripped.startswith("fleet:"):
        in_fleet = True
        continue

    if in_fleet:
        # If we hit a new top-level key, exit fleet
        if indent == 0 and re.match(r"^[A-Za-z_][A-Za-z0-9_]*\s*:", stripped):
            break

        # Fix missing dash on account_label lines inside fleet list
        if stripped.startswith("account_label:") and not stripped.startswith("- "):
            # We only patch if it looks like it should be a list item header (indent 0 or 2)
            # Typical correct format: "- account_label: xxx" at indent 0 or 2 depending on style.
            # Your file uses "- account_label:" at indent 0, so we enforce that.
            # If this line is indented, we still prepend "- " at same indent.
            prefix = raw[:indent]
            lines[i] = prefix + "- " + stripped
            fixed += 1

# Write patched file
p.write_text("".join(lines), encoding="utf-8")
print(f"OK: patched_missing_dashes={fixed}")
