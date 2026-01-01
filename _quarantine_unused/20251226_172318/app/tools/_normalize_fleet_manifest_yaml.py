from __future__ import annotations

from pathlib import Path
import re

p = Path(r"config\fleet_manifest.yaml")
txt = p.read_text(encoding="utf-8", errors="replace")

# Normalize tabs (tabs in YAML = pain)
txt = txt.replace("\t", "  ")
lines = txt.splitlines(True)

out = []
in_fleet = False
in_item = False

# These are keys that belong at fleet item level (indent 2) not nested (indent 4+)
ITEM_KEYS = {
    "account_label",
    "sub_uid",
    "enabled",
    "enable_ai_stack",
    "strategy_name",
    "role",
    "automation_mode",
    "ai_profile",
    "risk_pct",
    "max_concurrent_positions",
    "exit_profile",
    "timeframes",
    "symbols",
    "setup_types",
    "promotion_rules",
}

# These are known children of promotion_rules (indent 4)
PROMO_KEYS = {
    "enabled",
    "min_trades",
    "min_winrate",
    "min_avg_r",
    "min_expectancy_r",
    "max_drawdown_pct",
}

def indent(s: str) -> int:
    return len(s) - len(s.lstrip(" "))

fixed = 0
promo_indent = None  # None if not currently in promotion_rules block

for i, ln in enumerate(lines):
    raw = ln.rstrip("\r\n")
    s = raw

    # fleet: start
    if re.match(r"^\s*fleet\s*:\s*$", s):
        in_fleet = True
        in_item = False
        promo_indent = None
        out.append(ln)
        continue

    # If we leave fleet section (next top-level key at indent 0)
    if in_fleet and indent(s) == 0 and re.match(r"^[A-Za-z_][A-Za-z0-9_]*\s*:\s*", s) and not s.startswith("fleet:"):
        in_fleet = False
        in_item = False
        promo_indent = None
        out.append(ln)
        continue

    if not in_fleet:
        out.append(ln)
        continue

    # New fleet item marker
    if re.match(r"^\s*-\s*account_label\s*:\s*", s):
        in_item = True
        promo_indent = None
        # Force exact indent style: "- account_label:" at indent 0
        # If someone indented it, normalize it.
        normalized = re.sub(r"^\s*-\s*", "- ", s)
        if normalized != s:
            fixed += 1
            out.append(normalized + "\n")
        else:
            out.append(ln)
        continue

    # Keep blank lines as-is
    if not s.strip():
        out.append(ln)
        continue

    # If we are inside an item, normalize common key indentation
    if in_item:
        # Detect "promotion_rules:" header and normalize to indent 2
        m_pr = re.match(r"^\s*promotion_rules\s*:\s*$", s)
        if m_pr:
            if indent(s) != 2:
                fixed += 1
                out.append("  promotion_rules:\n")
            else:
                out.append(ln)
            promo_indent = 2
            continue

        # Detect keys of form "key: value"
        m = re.match(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*:\s*(.*)$", s)
        if m:
            key = m.group(1)
            rest = m.group(2)

            # Are we currently inside promotion_rules children?
            # We treat promo child keys as indent 4 ONLY if promo_indent is active
            if promo_indent is not None:
                # promotion_rules block ends when we see indent <= 2 AND it's not a promo child list continuation
                if indent(s) <= 2 and key in ITEM_KEYS and key != "promotion_rules":
                    promo_indent = None
                else:
                    # Normalize promo child keys
                    if key in PROMO_KEYS:
                        if indent(s) != 4:
                            fixed += 1
                            out.append(f"    {key}: {rest}\n")
                        else:
                            out.append(ln)
                        continue

            # Normalize item-level keys
            if key in ITEM_KEYS and key != "promotion_rules":
                if indent(s) != 2:
                    fixed += 1
                    out.append(f"  {key}: {rest}\n")
                else:
                    out.append(ln)
                continue

        # Normalize list blocks under known list keys (timeframes/symbols/setup_types)
        # If someone wrote "  timeframes:" then the list items must be "  - '5'" (indent 2) or "    - '5'" (indent 4) depending style.
        # Your file uses:
        #   timeframes:
        #   - '5'
        # So we keep that style. If we see list items with indent 4 under these keys, we fix to indent 2.
        if re.match(r"^\s*-\s*['A-Za-z0-9_]", s):
            # If it's a list item at indent 4, normalize to indent 2 (your style)
            if indent(s) == 4:
                fixed += 1
                out.append("  " + s.lstrip(" ") + "\n")
                continue

    out.append(ln)

new_txt = "".join(out)
p.write_text(new_txt, encoding="utf-8")

print(f"OK: normalized fleet_manifest.yaml fixed_lines={fixed}")

# Hard verify parse (fail loud)
try:
    import yaml  # type: ignore
    d = yaml.safe_load(new_txt)
    if not isinstance(d, dict) or "fleet" not in d or not isinstance(d.get("fleet"), list):
        raise SystemExit("FATAL: YAML parsed but structure is not {fleet: [..]}")
    print(f"OK: YAML_PARSE fleet_len={len(d.get('fleet') or [])}")
except Exception as e:
    raise SystemExit(f"FATAL: YAML still invalid after normalization: {e!r}")
