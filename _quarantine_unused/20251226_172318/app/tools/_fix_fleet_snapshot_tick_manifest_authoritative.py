from pathlib import Path
import re

p = Path(r"app\ops\fleet_snapshot_tick.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# Idempotent: if we already removed ops override block, exit
if "PHASE8_FIX: do NOT override manifest enabled/enable_ai_stack from ops_snapshot" in s:
    print("OK: fleet_snapshot_tick.py already fixed (manifest is authoritative)")
    raise SystemExit(0)

# We remove the second "acc = ops_accounts.get(label)" + enabled/enable_ai_stack override block.
# That block starts after:
#   enabled = bool(manifest_enabled)
#   enable_ai_stack = bool(manifest_enable_ai_stack)
# and then repeats acc/enabled/enable_ai_stack/sup again.
marker = "        enabled = bool(manifest_enabled)\n        enable_ai_stack = bool(manifest_enable_ai_stack)\n"
idx = s.find(marker)
if idx == -1:
    raise SystemExit("FATAL: could not find manifest_enabled assignment block")

# Find the duplicate override section that follows
dup_start = s.find("\n        acc = ops_accounts.get(label)", idx + len(marker))
if dup_start == -1:
    raise SystemExit("FATAL: could not find duplicate ops override block start")

# End of the duplicate block: just before 'mode_upper ='
dup_end = s.find("\n        mode_upper =", dup_start)
if dup_end == -1:
    raise SystemExit("FATAL: could not find duplicate ops override block end (mode_upper)")

# Replace duplicate override block with a comment
replacement = "\n        # PHASE8_FIX: do NOT override manifest enabled/enable_ai_stack from ops_snapshot\n        # Ops snapshot is observed reality (alive/pid), manifest is intent (enabled/mode).\n"
s2 = s[:dup_start] + replacement + s[dup_end:]

p.write_text(s2, encoding="utf-8")
print("OK: fixed fleet_snapshot_tick.py (manifest authoritative; ops does not override intent)")
