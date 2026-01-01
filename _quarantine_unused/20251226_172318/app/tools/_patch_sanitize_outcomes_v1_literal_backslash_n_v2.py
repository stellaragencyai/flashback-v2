from __future__ import annotations

from pathlib import Path

SRC = Path(r"state\ai_events\outcomes.v1.jsonl")

def main():
    if not SRC.exists():
        print(f"ERROR: missing {SRC}")
        return 2

    raw = SRC.read_text(encoding="utf-8", errors="replace")

    # Convert literal backslash-n sequences into real newlines.
    # This is the key difference vs v1: DO NOT delete separators.
    raw = raw.replace("\\r\\n", "\n")
    raw = raw.replace("\\n", "\n")

    # Normalize and keep only non-empty lines
    lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]

    SRC.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print("OK: sanitized outcomes.v1.jsonl v2 (converted literal \\n to real newlines)")
    print(f"rows_after={len(lines)}")

if __name__ == "__main__":
    raise SystemExit(main())
