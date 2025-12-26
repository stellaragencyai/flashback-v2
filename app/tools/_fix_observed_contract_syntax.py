from __future__ import annotations

import re
from pathlib import Path

TARGET = Path(r"app\core\observed_contract.py")

# Matches a line that looks like a dict key but was written like a list item:
#     "raw_reason",
#     'engine',
_KEY_ITEM = re.compile(r"^(\s*)([\"'])([A-Za-z0-9_]+)\2\s*,\s*$")

def main() -> int:
    if not TARGET.exists():
        print(f"FAIL: missing {TARGET}")
        return 2

    src = TARGET.read_text(encoding="utf-8", errors="ignore").splitlines()
    out = []
    fixes = 0

    # We'll do a conservative heuristic:
    # If the file contains obvious dict syntax nearby (a colon in adjacent lines),
    # then treat bare-key-comma lines as broken dict entries and repair to ": None,".
    for i, line in enumerate(src):
        m = _KEY_ITEM.match(line)
        if m:
            # Look at nearby lines to guess whether we're inside a dict
            prev = src[i-1] if i-1 >= 0 else ""
            nxt  = src[i+1] if i+1 < len(src) else ""

            neighborhood = prev + "\n" + line + "\n" + nxt
            looks_like_dict = (":" in prev) or (":" in nxt) or ("{" in prev) or ("}" in nxt)

            if looks_like_dict:
                indent, quote, key = m.group(1), m.group(2), m.group(3)
                out.append(f"{indent}{quote}{key}{quote}: None,  # AUTO-FIX: was missing ':'")
                fixes += 1
                continue

        out.append(line)

    TARGET.write_text("\n".join(out) + "\n", encoding="utf-8", newline="\n")
    print(f"OK: patched {TARGET} fixes={fixes}")

    # Quick compile check
    try:
        compile(TARGET.read_text(encoding="utf-8", errors="ignore"), str(TARGET), "exec")
        print("PASS: observed_contract.py compiles")
        return 0
    except Exception as e:
        print("FAIL: still does not compile:", e)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
