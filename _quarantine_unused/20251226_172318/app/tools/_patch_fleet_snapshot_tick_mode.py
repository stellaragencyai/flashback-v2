from pathlib import Path
import re

p = Path(r"app\ops\fleet_snapshot_tick.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# Idempotency: if already patched, exit
if "fleet_mode" in s and "_read_manifest_rows" in s and "automation_mode" in s:
    print("OK: fleet_snapshot_tick.py already patched (fleet_mode + manifest rows)")
    raise SystemExit(0)

# Replace _read_manifest_labels with a real YAML reader
pattern = r"def _read_manifest_labels\(\)\s*->\s*list\[str\]:.*?return out\s*\n"
m = re.search(pattern, s, flags=re.DOTALL)
if not m:
    raise SystemExit("FATAL: could not find _read_manifest_labels() block to replace")

replacement = '''def _read_manifest_rows() -> list[dict[str, Any]]:
    """
    Loads config/fleet_manifest.yaml and returns list of fleet rows.
    Best-effort: requires PyYAML. If missing, falls back to label-only parse.
    """
    if not CFG.exists():
        return []

    # Try YAML first (correct)
    try:
        import yaml  # type: ignore
        d = yaml.safe_load(CFG.read_text(encoding="utf-8", errors="ignore")) or {}
        fleet = d.get("fleet") or []
        if not isinstance(fleet, list):
            return []
        out: list[dict[str, Any]] = []
        for row in fleet:
            if isinstance(row, dict):
                out.append(row)
        return out
    except Exception:
        # Fallback: label-only scan (legacy)
        labels: list[str] = []
        for line in CFG.read_text(encoding="utf-8", errors="ignore").splitlines():
            t = line.strip()
            if t.startswith("account_label:"):
                lbl = t.split(":", 1)[1].strip().strip("'").strip('"')
                if lbl:
                    labels.append(lbl)
        seen = set()
        uniq = []
        for x in labels:
            if x not in seen:
                seen.add(x)
                uniq.append(x)
        return [{"account_label": x} for x in uniq]
'''

s = s[:m.start()] + replacement + s[m.end():]

# Now patch main() to use rows + compute fleet_mode + inject automation_mode into subs
# 1) Replace: labels = _read_manifest_labels()
s = s.replace("    labels = _read_manifest_labels()", "    rows = _read_manifest_rows()")

# 2) Insert labels derivation + subs
needle = "    subs: Dict[str, Any] = {}"
if needle not in s:
    raise SystemExit("FATAL: could not find subs dict init")
s = s.replace(
    needle,
    needle + "\n\n    # Labels from manifest rows (preserve manifest order)\n    labels: list[str] = []\n    for r in rows:\n        if isinstance(r, dict):\n            lbl = str(r.get('account_label') or '').strip()\n            if lbl and lbl not in labels:\n                labels.append(lbl)\n",
    1,
)

# 3) Patch loop: for label in labels:
s = s.replace("    for label in labels:", "    # Merge manifest config with ops_snapshot live signals\n    for r in rows:\n        if not isinstance(r, dict):\n            continue\n        label = str(r.get('account_label') or '').strip()\n        if not label:\n            continue\n\n        manifest_enabled = bool(r.get('enabled', True))\n        manifest_enable_ai_stack = bool(r.get('enable_ai_stack', True))\n        manifest_mode = str(r.get('automation_mode') or 'UNKNOWN').strip()\n        is_canary = (manifest_mode.strip().upper() == 'LIVE_CANARY')\n\n        acc = ops_accounts.get(label) if isinstance(ops_accounts, dict) else None\n        if not isinstance(acc, dict):\n            acc = {}\n\n        sup = acc.get('supervisor_ai_stack') if isinstance(acc.get('supervisor_ai_stack'), dict) else {}\n        alive = bool(sup.get('ok')) if isinstance(sup, dict) and sup else False\n        pid = sup.get('pid') if isinstance(sup, dict) else None\n\n        enabled = bool(manifest_enabled)\n        enable_ai_stack = bool(manifest_enable_ai_stack)\n", 1)

# 4) Patch subs[label] payload to include automation_mode + is_canary
# Find exact subs assignment block
sub_block_old = '''        subs[label] = {
            "enabled": enabled,
            "enable_ai_stack": enable_ai_stack,
            "should_run": bool(enabled and enable_ai_stack),
            "alive": alive,
            "pid": pid,
        }'''
if sub_block_old not in s:
    raise SystemExit("FATAL: could not find expected subs[label] block to replace")
sub_block_new = '''        subs[label] = {
            "enabled": enabled,
            "enable_ai_stack": enable_ai_stack,
            "automation_mode": manifest_mode,
            "is_canary": bool(is_canary),
            "should_run": bool(enabled and enable_ai_stack),
            "alive": alive,
            "pid": pid,
        }'''
s = s.replace(sub_block_old, sub_block_new, 1)

# 5) Inject fleet_mode computation before out = { ... }
out_needle = "    out = {"
if out_needle not in s:
    raise SystemExit("FATAL: could not find out dict creation")
if "fleet_mode =" in s:
    raise SystemExit("FATAL: fleet_mode already exists unexpectedly")
fleet_mode_block = '''
    # Compute fleet_mode deterministically from SHOULD_RUN subs
    # Precedence: LIVE > LIVE_CANARY > LEARN_DRY > OFF
    active_modes = []
    for _lbl, info in subs.items():
        if isinstance(info, dict) and bool(info.get("should_run")):
            m = str(info.get("automation_mode") or "UNKNOWN").strip().upper()
            active_modes.append(m)

    if "LIVE" in active_modes:
        fleet_mode = "LIVE"
    elif "LIVE_CANARY" in active_modes:
        fleet_mode = "LIVE_CANARY"
    elif "LEARN_DRY" in active_modes:
        fleet_mode = "LEARN_DRY"
    else:
        fleet_mode = "OFF"
'''
s = s.replace(out_needle, fleet_mode_block + "\n" + out_needle, 1)

# 6) Add fleet_mode key into out dict (near ts_ms for clarity)
# Insert after "ts_ms": _now_ms(),
s = s.replace('        "ts_ms": _now_ms(),', '        "ts_ms": _now_ms(),\n        "fleet_mode": fleet_mode,', 1)

p.write_text(s, encoding="utf-8")
print("OK: patched fleet_snapshot_tick.py (manifest YAML rows + automation_mode + fleet_mode)")
