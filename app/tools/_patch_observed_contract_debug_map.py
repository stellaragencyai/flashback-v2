from __future__ import annotations

import re
from pathlib import Path

TARGET = Path(r"app\core\observed_contract.py")

def main() -> int:
    if not TARGET.exists():
        print(f"FAIL: missing {TARGET}")
        return 2

    s = TARGET.read_text(encoding="utf-8", errors="ignore")

    # Replace the entire debug dict contents safely (between "debug": { and the matching } at same indent)
    # We anchor to the exact keys you showed to avoid accidental wreckage.
    pat = re.compile(
        r'("debug"\s*:\s*\{\s*\n)'
        r'(\s*"engine"\s*:\s*dbg\.get\("engine"\)\s*,\s*\n)'
        r'(?s:.*?)(\n\s*\}\s*,)',
        re.MULTILINE
    )

    m = pat.search(s)
    if not m:
        print("FAIL: could not locate expected debug dict block to patch")
        return 1

    head = m.group(1)
    indent_line = m.group(2)
    tail = m.group(3)

    # Determine indent from the engine line
    indent = re.match(r"^(\s*)", indent_line).group(1)

    new_mid = (
        f'{indent}"engine": dbg.get("engine"),\n'
        f'{indent}"raw_reason": dbg.get("raw_reason"),\n'
        f'{indent}"regime": dbg.get("regime"),\n'
        f'{indent}"last_close": dbg.get("last_close"),\n'
        f'{indent}"prev_close": dbg.get("prev_close"),\n'
        f'{indent}"ma": dbg.get("ma"),\n'
    )

    s2 = s[:m.start()] + head + new_mid + tail + s[m.end():]
    TARGET.write_text(s2, encoding="utf-8", newline="\n")
    print("OK: patched debug mapping in observed_contract.py")

    # Compile check
    try:
        compile(TARGET.read_text(encoding="utf-8", errors="ignore"), str(TARGET), "exec")
        print("PASS: observed_contract.py compiles")
        return 0
    except Exception as e:
        print("FAIL: still does not compile:", e)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
