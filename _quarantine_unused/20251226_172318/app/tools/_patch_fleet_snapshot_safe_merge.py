from pathlib import Path
import re

P = Path(r"app\ops\fleet_snapshot_tick.py")
s = P.read_text(encoding="utf-8", errors="ignore")

# 1) Ensure helper exists (top-level, safe)
if "def _load_ops_snapshot" not in s:
    s = s.replace(
        "STATE = ROOT / \"state\"",
        "STATE = ROOT / \"state\"\n\n"
        "def _load_ops_snapshot() -> dict:\n"
        "    p = STATE / \"ops_snapshot.json\"\n"
        "    if not p.exists():\n"
        "        return {}\n"
        "    try:\n"
        "        import json\n"
        "        return json.loads(p.read_text(encoding=\"utf-8\", errors=\"ignore\") or \"{}\")\n"
        "    except Exception:\n"
        "        return {}\n"
    )

# 2) Patch inside snapshot builder function ONLY
pattern = r"(for\s+label\s+in\s+labels\s*:\s*\n(?:[ \t]+.*\n)+?)"
m = re.search(pattern, s)
if not m:
    raise SystemExit("FATAL: could not find subs build loop")

block = m.group(1)

if "_merge_runtime" not in block:
    inject = (
        "        # Phase8: merge ops runtime truth\n"
        "        ops = _load_ops_snapshot()\n"
        "        acc = (ops.get('accounts') or {}) if isinstance(ops, dict) else {}\n"
        "        a = acc.get(label) if isinstance(acc, dict) else None\n"
        "        if isinstance(a, dict):\n"
        "            sup = a.get('supervisor_ai_stack') if isinstance(a.get('supervisor_ai_stack'), dict) else {}\n"
        "            pid = sup.get('pid') if isinstance(sup, dict) else a.get('pid')\n"
        "            alive = sup.get('alive') if isinstance(sup, dict) else a.get('alive')\n"
        "            if pid is not None:\n"
        "                subs[label]['pid'] = pid\n"
        "            if alive is not None:\n"
        "                subs[label]['alive'] = bool(alive)\n"
    )
    block2 = block + inject
    s = s.replace(block, block2)

P.write_text(s, encoding="utf-8")
print("OK: fleet_snapshot_tick patched safely (function-level injection)")
