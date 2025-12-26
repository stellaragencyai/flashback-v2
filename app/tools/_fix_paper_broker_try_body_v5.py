from __future__ import annotations
from pathlib import Path

TARGET = Path(r"app\sim\paper_broker.py")

def leading_spaces(line: str) -> int:
    return len(line) - len(line.lstrip(" "))

def main() -> None:
    if not TARGET.exists():
        raise SystemExit(f"FAIL: missing {TARGET}")

    text = TARGET.read_text(encoding="utf-8", errors="ignore").replace("\t", "    ")
    lines = text.splitlines()

    fixes = 0
    for i, line in enumerate(lines):
        if line.lstrip() == "try:":
            base = leading_spaces(line)
            # Find the next publish call shortly after this try:
            for j in range(i + 1, min(i + 10, len(lines))):
                if "_publish_positions_bus(" in lines[j]:
                    cur = leading_spaces(lines[j])
                    # If it's not indented deeper than the try, force it under the try.
                    if cur <= base:
                        lines[j] = (" " * (base + 4)) + lines[j].lstrip(" ")
                        fixes += 1
                    break

    TARGET.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"OK: force-indented publish under try where needed (fixes={fixes})")

if __name__ == "__main__":
    main()
