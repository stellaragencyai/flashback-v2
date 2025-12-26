from __future__ import annotations

from pathlib import Path
import re

P = Path(r"app\ops\fleet_snapshot_tick.py")
s = P.read_text(encoding="utf-8", errors="ignore")

if "Phase8: merge ops_snapshot runtime truth into fleet snapshot" in s:
    raise SystemExit("FATAL: merge block already present (refusing to double-patch)")

# Ensure helper exists
if "def _load_ops_snapshot" not in s:
    m = re.search(r"(?m)^\s*STATE\s*=\s*.+$", s)
    if not m:
        # fallback: insert after ROOT definition
        m = re.search(r"(?m)^\s*ROOT\s*=\s*.+$", s)
    if not m:
        raise SystemExit("FATAL: Could not find ROOT= or STATE= to anchor helper insertion")

    insert_at = m.end()
    helper = """

def _load_ops_snapshot() -> dict:
    p = STATE / "ops_snapshot.json"
    if not p.exists():
        return {}
    try:
        import json
        return json.loads(p.read_text(encoding="utf-8", errors="ignore") or "{}")
    except Exception:
        return {}
"""
    s = s[:insert_at] + helper + s[insert_at:]


# Find a writer anchor line index.
# We look for any line that (a) references fleet_snapshot OR FLEET_SNAPSHOT and (b) writes/dumps.
lines = s.splitlines(True)

def is_writer_line(l: str) -> bool:
    ll = l.lower()
    if ("fleet_snapshot" not in ll) and ("fleet_snapshot.json" not in ll) and ("fleet_snapshot_json" not in ll) and ("fleet_snapshot_path" not in ll) and ("fleet_snapshot" not in ll) and ("fleet_snapshot" not in ll) and ("fleet_snapshot" not in ll):
        # also allow FLEET_SNAPSHOT constant usage without literal string
        if "fleet_snapshot" not in l and "FLEET_SNAPSHOT" not in l:
            return False
    # writer-ish operations
    return any(x in ll for x in ["write_text", "write_bytes", "open(", "json.dump", "dump(", "write("])

writer_i = None
for i, l in enumerate(lines):
    if is_writer_line(l):
        writer_i = i
        break

# If still not found, try broader: any write/dump line, then backtrack for fleet_snapshot path nearby
if writer_i is None:
    for i, l in enumerate(lines):
        ll = l.lower()
        if any(x in ll for x in ["write_text", "write_bytes", "json.dump", "dump("]):
            # look back 40 lines for 'fleet_snapshot' mention
            back = "".join(lines[max(0, i-40):i+1]).lower()
            if "fleet_snapshot" in back or "fleet_snapshot.json" in back or "fleet_snapshot" in back:
                writer_i = i
                break

if writer_i is None:
    raise SystemExit("FATAL: Could not find any writer-ish line associated with fleet_snapshot in fleet_snapshot_tick.py")

# Determine indent of the writer line
writer_line = lines[writer_i].rstrip("\r\n")
indent = re.match(r"^(\s*)", writer_line).group(1)

# Try to guess the snapshot dict variable name used nearby
# Look backward up to 80 lines for a likely assignment like: out = { ... } OR snap = {...}
var_name = None
for j in range(writer_i, max(-1, writer_i-80), -1):
    m = re.match(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*\{", lines[j].rstrip("\r\n"))
    if m:
        var_name = m.group(1)
        break
    m2 = re.match(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*dict\(", lines[j].rstrip("\r\n"))
    if m2:
        var_name = m2.group(1)
        break

if not var_name:
    # common defaults
    var_name = "out"

merge = f"""
{indent}# Phase8: merge ops_snapshot runtime truth into fleet snapshot (authoritative runtime)
{indent}try:
{indent}    ops = _load_ops_snapshot()
{indent}    accounts = (ops.get("accounts") or {{}}) if isinstance(ops, dict) else {{}}
{indent}    fleet = ({var_name}.get("fleet") or {{}}) if isinstance({var_name}, dict) else {{}}
{indent}    subs = (fleet.get("subs") or {{}}) if isinstance(fleet, dict) else {{}}
{indent}    if isinstance(accounts, dict) and isinstance(subs, dict):
{indent}        for lbl, sub in subs.items():
{indent}            if not isinstance(sub, dict):
{indent}                continue
{indent}            a = accounts.get(lbl)
{indent}            if not isinstance(a, dict):
{indent}                continue
{indent}            sup = a.get("supervisor_ai_stack") if isinstance(a.get("supervisor_ai_stack"), dict) else {{}}
{indent}            pid = sup.get("pid") if isinstance(sup, dict) else None
{indent}            alive = sup.get("alive") if isinstance(sup, dict) else None
{indent}            if pid is None:
{indent}                pid = a.get("pid")
{indent}            if alive is None:
{indent}                alive = a.get("alive")
{indent}            if pid is not None:
{indent}                sub["pid"] = pid
{indent}            if alive is not None:
{indent}                sub["alive"] = bool(alive)
{indent}except Exception:
{indent}    pass
"""

# Insert merge block immediately before writer line
lines.insert(writer_i, merge)
s2 = "".join(lines)

P.write_text(s2, encoding="utf-8")
print(f"OK: patched fleet_snapshot_tick.py (universal writer hook) writer_line={writer_i+1} var={var_name!r}")
