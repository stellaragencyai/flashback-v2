from __future__ import annotations

from pathlib import Path

SRC = Path(r"state\ai_events\outcomes.v1.jsonl")

def main():
    if not SRC.exists():
        print(f"ERROR: missing {SRC}")
        return 2

    raw = SRC.read_text(encoding="utf-8", errors="replace")

    # Replace literal backslash-n sequences that appear after JSON objects
    # e.g. ..."}\n\n" -> ..."}\n" (real newline)
    fixed = raw.replace("\\n\r\n", "\n").replace("\\n\n", "\n").replace("\\n", "")

    # Normalize line endings and strip empty trailing lines
    lines = [ln.rstrip() for ln in fixed.splitlines() if ln.strip()]

    SRC.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print("OK: sanitized outcomes.v1.jsonl (removed literal \\n tokens)")
    print(f"rows_after={len(lines)}")

if __name__ == "__main__":
    raise SystemExit(main())
