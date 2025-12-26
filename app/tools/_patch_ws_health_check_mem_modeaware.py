from pathlib import Path

p = Path(r"app\tools\ws_health_check.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# Anchor: we know mode exists in file
needle_mode = '    mode = _get_mode_from_manifest(account_label) or os.getenv("AUTOMATION_MODE") or "UNKNOWN"\n'
if needle_mode not in s:
    raise SystemExit("PATCH FAIL: could not find mode assignment anchor")

# Insert mem_required logic AFTER mode is defined (and before memory checks start)
if "mem_required = True" not in s:
    insert = needle_mode + (
        "    # Memory requirement is mode-aware (OFF => WARN only)\n"
        "    mem_required = True\n"
        "    try:\n"
        "        m = str(mode).strip().upper()\n"
        "        if m in (\"OFF\",):\n"
        "            mem_required = False\n"
        "    except Exception:\n"
        "        mem_required = True\n"
    )
    s = s.replace(needle_mode, insert)

# Gate memory failure block
needle_fail = "    if mem_failures:\n"
if needle_fail not in s:
    raise SystemExit("PATCH FAIL: could not find 'if mem_failures:' block")

s = s.replace(needle_fail, "    if mem_failures and mem_required:\n", 1)

# After FAIL return 4, add WARN-only branch
target = "        return 4\n"
if target not in s:
    raise SystemExit("PATCH FAIL: could not find 'return 4' in memory failure block")

warn_block = (
    "        return 4\n\n"
    "    if mem_failures and (not mem_required):\n"
    "        print(\"\\nWARN (AI MEMORY):\")\n"
    "        for f in mem_failures:\n"
    "            print(f\" - {f}\")\n"
)
s = s.replace(target, warn_block, 1)

p.write_text(s, encoding="utf-8")
print("OK: patched ws_health_check.py (AI memory mode-aware; OFF => WARN)")
