from __future__ import annotations

from pathlib import Path

TARGET = Path(r"app\ai\outcome_writer.py")

def main():
    if not TARGET.exists():
        print(f"ERROR: missing {TARGET}")
        return 2

    s = TARGET.read_text(encoding="utf-8", errors="replace")

    before = s

    # Fix the most common bug patterns:
    # - appending literal "\\n" instead of "\n"
    # - writing "\\n" after json dumps
    s = s.replace('"\\\\n"', '"\\n"')
    s = s.replace("'\\\\n'", "'\\n'")

    # Also catch concatenations that might be explicitly writing backslash-n as text
    s = s.replace("+ \"\\\\n\"", "+ \"\\n\"")
    s = s.replace("+ '\\\\n'", "+ '\\n'")

    if s == before:
        print("WARN: no changes made (writer may already be correct or uses a different pattern).")
    else:
        TARGET.write_text(s, encoding="utf-8")
        print("OK: patched app/ai/outcome_writer.py to use real newlines")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
