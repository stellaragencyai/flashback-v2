from __future__ import annotations

from pathlib import Path
import re

p = Path(r"config\fleet_manifest.yaml")
txt = p.read_text(encoding="utf-8", errors="replace").replace("\t", "  ")
lines = txt.splitlines(True)

# Locate fleet block
fleet_start = None
for i, ln in enumerate(lines):
    if re.match(r"^\s*fleet\s*:\s*$", ln.rstrip("\r\n")):
        fleet_start = i
        break

if fleet_start is None:
    raise SystemExit("FATAL: Could not find top-level 'fleet:' in config\\fleet_manifest.yaml")

# End of fleet block: next top-level key (indent 0, looks like key:)
fleet_end = len(lines)
for j in range(fleet_start + 1, len(lines)):
    s = lines[j].rstrip("\r\n")
    if s and (len(s) - len(s.lstrip(" ")) == 0) and re.match(r"^[A-Za-z_][A-Za-z0-9_]*\s*:\s*", s) and not s.startswith("fleet:"):
        fleet_end = j
        break

head = "".join(lines[:fleet_start+1])  # includes 'fleet:' line
tail = "".join(lines[fleet_end:])      # rest of doc after fleet block
body = lines[fleet_start+1:fleet_end]

# Keys that must be at item level (indent 2)
ITEM_KEYS = {
    "account_label","sub_uid","enabled","enable_ai_stack","strategy_name","role","automation_mode",
    "ai_profile","risk_pct","max_concurrent_positions","exit_profile","timeframes","symbols","setup_types","promotion_rules",
}

# promotion_rules children (indent 4)
PROMO_KEYS = {"enabled","min_trades","min_winrate","min_avg_r","min_expectancy_r","max_drawdown_pct"}

# list keys whose items should be indent 4
LIST_KEYS = {"timeframes","symbols","setup_types"}

out = []
in_item = False
in_promo = False
current_list = None

def indent(s: str) -> int:
    return len(s) - len(s.lstrip(" "))

def is_kv(s: str):
    m = re.match(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*:\s*(.*)$", s)
    return (m.group(1), m.group(2)) if m else (None, None)

for ln in body:
    s = ln.rstrip("\r\n")

    if not s.strip():
        continue  # drop blank lines inside fleet to avoid indentation ambiguity

    # new item
    m_item = re.match(r"^\s*-\s*account_label\s*:\s*(.*)$", s)
    if m_item:
        in_item = True
        in_promo = False
        current_list = None
        val = m_item.group(1).strip()
        out.append(f"- account_label: {val}\n")
        continue

    if not in_item:
        # Ignore stray pre-item junk inside fleet, but keep it visible as comment
        out.append(f"# DROPPED_STRAY: {s}\n")
        continue

    # list item line?
    if re.match(r"^\s*-\s+", s):
        if current_list in LIST_KEYS:
            out.append("    " + s.lstrip(" ") + "\n")  # indent 4
        else:
            # If list item appears without a list key context, comment it so YAML stays valid
            out.append(f"  # ORPHAN_LIST_ITEM: {s}\n")
        continue

    key, rest = is_kv(s)
    if key is None:
        # Unknown line shape; comment it out so YAML remains valid and we can inspect later
        out.append(f"  # UNPARSEABLE: {s}\n")
        continue

    # promotion_rules header
    if key == "promotion_rules":
        out.append("  promotion_rules:\n")
        in_promo = True
        current_list = None
        continue

    # if we hit a new item-level key, end promo/list contexts
    if key in ITEM_KEYS and key != "promotion_rules":
        in_promo = False
        current_list = key if key in LIST_KEYS else None
        out.append(f"  {key}: {rest}\n")
        continue

    # promo child
    if in_promo and key in PROMO_KEYS:
        out.append(f"    {key}: {rest}\n")
        continue

    # list context key maybe malformed indentation; still treat if known
    if key in LIST_KEYS:
        in_promo = False
        current_list = key
        out.append(f"  {key}: {rest}\n")
        continue

    # Unknown key inside item:
    # keep it but at indent 2 so YAML is valid and snapshot_tick can still read account_label/enabled/mode/etc.
    in_promo = False
    current_list = None
    out.append(f"  {key}: {rest}\n")

new_txt = head + "".join(out) + tail
p.write_text(new_txt, encoding="utf-8")
print("OK: rebuilt fleet block into valid YAML indentation (fleet section rewritten)")

# Hard parse verify
import yaml  # type: ignore
d = yaml.safe_load(new_txt)
if not isinstance(d, dict) or "fleet" not in d or not isinstance(d.get("fleet"), list):
    raise SystemExit("FATAL: YAML parsed but structure is invalid (missing fleet list)")
print(f"OK: YAML_PARSE fleet_len={len(d.get('fleet') or [])}")
