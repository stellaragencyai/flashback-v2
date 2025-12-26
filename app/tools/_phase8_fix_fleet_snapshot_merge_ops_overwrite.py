from __future__ import annotations

from pathlib import Path
import re

P = Path(r"app\ops\fleet_snapshot_tick.py")
s = P.read_text(encoding="utf-8", errors="ignore")

# Remove the SECOND overwrite block that re-fetches acc and reassigns enabled/enable_ai_stack/alive/pid.
# We keep:
#   acc = ops_accounts.get(label) ...
#   sup = acc.get('supervisor_ai_stack') ...
#   alive/pid derived ONCE from ops snapshot
#   enabled/enable_ai_stack derived ONCE from manifest
#
# Delete block:
#   acc = ops_accounts.get(label) ...
#   enabled = bool(acc.get("enabled"...))
#   enable_ai_stack = bool(acc.get("enable_ai_stack"...))
#   sup = acc.get("supervisor_ai_stack"...)
#   alive = bool(sup.get("ok"...))
#   pid = sup.get("pid"...)

pattern = r"""
(?P<block>
^[ \t]*acc[ \t]*=[ \t]*ops_accounts\.get\(label\)[^\n]*\n
^[ \t]*if[ \t]+not[ \t]+isinstance\(acc,[ \t]*dict\):[^\n]*\n
^[ \t]*acc[ \t]*=[ \t]*\{\}[ \t]*\n
\ n?
^[ \t]*enabled[ \t]*=[ \t]*bool\(acc\.get\("enabled",[ \t]*True\)\)[^\n]*\n
^[ \t]*enable_ai_stack[ \t]*=[ \t]*bool\(acc\.get\("enable_ai_stack",[ \t]*True\)\)[^\n]*\n
\ n?
^[ \t]*sup[ \t]*=[ \t]*acc\.get\("supervisor_ai_stack"\)[^\n]*\n
^[ \t]*alive[ \t]*=[ \t]*bool\(sup\.get\("ok"\)\)[^\n]*\n
^[ \t]*pid[ \t]*=[ \t]*sup\.get\("pid"\)[^\n]*\n
)
"""

# Regex with DOTALL + MULTILINE via flags
rx = re.compile(pattern, re.MULTILINE | re.VERBOSE)

m = rx.search(s)
if not m:
    raise SystemExit("FATAL: could not find the overwrite acc/enabled/enable_ai_stack/alive/pid block to remove (file drifted).")

s2 = s[:m.start("block")] + s[m.end("block"):]
P.write_text(s2, encoding="utf-8")
print("OK: removed ops overwrite block (manifest authoritative; ops supplies pid/alive)")
