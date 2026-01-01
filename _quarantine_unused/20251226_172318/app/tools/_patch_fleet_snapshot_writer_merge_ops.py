from __future__ import annotations

from pathlib import Path
import re

P = Path(r"app\ops\fleet_snapshot_tick.py")
s = P.read_text(encoding="utf-8", errors="ignore")

# ---------------------------
# 1) Ensure helper exists
# ---------------------------
if "def _load_ops_snapshot" not in s:
    # Insert after STATE definition if present
    m = re.search(r"(?m)^\s*STATE\s*=\s*.+$", s)
    if not m:
        raise SystemExit("FATAL: Could not find STATE = ... line to anchor helper insertion")

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


# ---------------------------
# 2) Find where fleet snapshot is written
# ---------------------------
# We anchor on writing fleet_snapshot.json via FLEET_SNAPSHOT.write_text(...)
w = re.search(r"(?m)^\s*FLEET_SNAPSHOT\.write_text\(", s)
if not w:
    # Some versions might do: (STATE/"fleet_snapshot.json").write_text(...)
    w = re.search(r"(?m)^\s*.*fleet_snapshot\.json.*write_text\(", s)
if not w:
    raise SystemExit("FATAL: Could not find the fleet_snapshot write_text() call to anchor the merge")

# Determine indent of the writer line
writer_line_start = s.rfind("\n", 0, w.start()) + 1
writer_line = s[writer_line_start:s.find("\n", writer_line_start)]
indent = re.match(r"^(\s*)", writer_line).group(1)

# Try to capture the variable passed into json.dumps(<VAR>, ...)
var_name = None
m2 = re.search(r"json\.dumps\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*[,)]", writer_line)
if m2:
    var_name = m2.group(1)
else:
    # fallback common names
    for cand in ("out", "snapshot", "snap", "payload", "d"):
        if re.search(rf"(?m)^\s*{cand}\s*=\s*\{{", s) or re.search(rf"(?m)^\s*{cand}\s*=\s*dict\(", s):
            var_name = cand
            break
if not var_name:
    var_name = "out"  # best-effort default

merge_block = f"""
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
{indent}            # fall back to top-level fields if present
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

# Prevent double-inject
if "merge ops_snapshot runtime truth into fleet snapshot" in s:
    raise SystemExit("FATAL: merge block already present (refusing to double-patch)")

# Insert merge block immediately BEFORE the writer line
s = s[:writer_line_start] + merge_block + s[writer_line_start:]

P.write_text(s, encoding="utf-8")
print(f"OK: patched fleet_snapshot_tick.py (writer-anchored merge) var={var_name!r}")
