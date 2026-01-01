from __future__ import annotations

from pathlib import Path
import re

TARGET = Path(r"app\ai\ai_decision_enforcer.py")

def main() -> int:
    if not TARGET.exists():
        print(f"ERR: missing {TARGET}")
        return 2

    s = TARGET.read_text(encoding="utf-8", errors="ignore")

    # We replace the broken block:
    #   read_jsonl_tail = None
    #       def safe_str...
    #       def safe_upper...
    #       def normalize_timeframe...
    # with a correct top-level version (no indentation).
    pat = re.compile(
        r"(?ms)^read_jsonl_tail\s*=\s*None\s*#\s*type:\s*ignore\s*\n"
        r"(?:^[ \t]+def\s+safe_str\(.*?\n(?:.*\n)*?)"
        r"(?:^[ \t]+def\s+safe_upper\(.*?\n(?:.*\n)*?)"
        r"(?:^[ \t]+def\s+normalize_timeframe\(.*?\n(?:.*\n)*?)"
        r"(?:^\s*\n)+"
    )

    repl = (
        "read_jsonl_tail = None  # type: ignore\n"
        "\n"
        "def safe_str(x: Any) -> str:  # type: ignore\n"
        "    try:\n"
        "        return ('' if x is None else str(x)).strip()\n"
        "    except Exception:\n"
        "        return ''\n"
        "\n"
        "def safe_upper(x: Any) -> str:  # type: ignore\n"
        "    return safe_str(x).upper()\n"
        "\n"
        "def normalize_timeframe(tf: Any) -> str:  # type: ignore\n"
        "    s = safe_str(tf).lower()\n"
        "    if not s:\n"
        "        return ''\n"
        "    if s.endswith(('m','h','d','w')):\n"
        "        return s\n"
        "    try:\n"
        "        n = int(float(s))\n"
        "        return f\"{n}m\" if n > 0 else ''\n"
        "    except Exception:\n"
        "        return ''\n"
        "\n"
        "\n"
    )

    m = pat.search(s)
    if not m:
        print("ERR: could not find the broken indented helper block to replace.")
        return 3

    s2 = pat.sub(repl, s, count=1)

    # sanity: ensure we did not introduce tabs at start of these defs
    if re.search(r"(?m)^[ \t]+def\s+safe_str\(", s2):
        print("ERR: safe_str still indented after patch (unexpected).")
        return 4

    TARGET.write_text(s2, encoding="utf-8")
    print("OK: patched ai_decision_enforcer.py (fixed unexpected indent helper block)")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
