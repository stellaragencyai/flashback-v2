from pathlib import Path
import time
import re

TARGET = Path(r"C:\flashback\app\bots\executor_v2.py")
if not TARGET.exists():
    raise SystemExit(f"ERROR: missing {TARGET}")

src = TARGET.read_text(encoding="utf-8", errors="replace")
bak = TARGET.with_suffix(f".py.bak_{int(time.time())}")
bak.write_text(src, encoding="utf-8")

lines = src.splitlines()
out = []
fix_count = 0

def is_blank_or_comment(s: str) -> bool:
    t = s.strip()
    return (t == "") or t.startswith("#")

i = 0
while i < len(lines):
    line = lines[i]
    out.append(line)

    # Match: "except Exception as e:" with some indentation
    m = re.match(r"^(\s*)except Exception as e:\s*$", line)
    if m:
        base_indent = m.group(1)
        body_indent = base_indent + (" " * 4)

        # Look ahead to find next non-blank/non-comment line
        j = i + 1
        while j < len(lines) and is_blank_or_comment(lines[j]):
            out.append(lines[j])
            j += 1

        # If file ends right after except, or next meaningful line is not indented as a body,
        # insert a 'pass' so the except block is syntactically valid.
        if j >= len(lines):
            out.append(body_indent + "pass  # auto-fix: empty except block")
            fix_count += 1
            i = j
            continue

        nxt = lines[j]
        # If the next line does NOT start with body indentation (or deeper), then except has no body
        if not nxt.startswith(body_indent) and nxt.strip() != "":
            out.append(body_indent + "pass  # auto-fix: empty except block")
            fix_count += 1

        # Now continue normal processing (do not skip the next line; it will be handled in next loop)
    i += 1

TARGET.write_text("\n".join(out), encoding="utf-8")

print(f"OK: patched empty except blocks: {fix_count}")
print(f" - backup: {bak.name}")
