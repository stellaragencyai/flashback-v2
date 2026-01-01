from pathlib import Path
import sys

p = Path("app/tools/manual_allow_decision.py")
s = p.read_text(encoding="utf-8", errors="ignore")

if "manual_allow: hardened logger" in s:
    print("already_patched")
    sys.exit(0)

anchor = 'DECISIONS_PATH.parent.mkdir(parents=True, exist_ok=True)\n'
if anchor not in s:
    print("anchor_missing_DECISIONS_PATH")
    sys.exit(2)

# 1) Insert logger import block after DECISIONS_PATH mkdir
insert_block = anchor + """
# --- manual_allow: hardened logger (lock + dedupe + reject routing) ---
try:
    from app.core.ai_decision_logger import append_decision as _append_decision_logged  # type: ignore
except Exception:
    _append_decision_logged = None  # type: ignore
"""

s = s.replace(anchor, insert_block, 1)

# 2) Replace the raw file-write block with logger-first behavior
needle = """    with DECISIONS_PATH.open("ab") as f:
        f.write(json.dumps(row, ensure_ascii=False).encode("utf-8") + b"\\n")
"""

if needle not in s:
    print("raw_append_block_not_found")
    sys.exit(3)

replacement = """    if _append_decision_logged is not None:
        _append_decision_logged(row)
    else:
        with DECISIONS_PATH.open("ab") as f:
            f.write(json.dumps(row, ensure_ascii=False).encode("utf-8") + b"\\n")
"""

s = s.replace(needle, replacement, 1)

p.write_text(s, encoding="utf-8")
print("patched_ok")
