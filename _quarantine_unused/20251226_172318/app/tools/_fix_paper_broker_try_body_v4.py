from __future__ import annotations
from pathlib import Path

TARGET = Path(r"app\sim\paper_broker.py")

def leading_spaces(line: str) -> int:
    return len(line) - len(line.lstrip(" "))

def main() -> None:
    if not TARGET.exists():
        raise SystemExit(f"FAIL: missing {TARGET}")

    lines = TARGET.read_text(encoding="utf-8", errors="ignore").replace("\t","    ").splitlines()
    nfix = 0

    i = 0
    while i < len(lines):
        line = lines[i]
        if line.lstrip() == "try:":
            ind = leading_spaces(line)
            # Look ahead a small window for publish call
            for j in range(i+1, min(i+8, len(lines))):
                if "_publish_positions_bus(" in lines[j]:
                    # If publish is same indent as try, indent it into the try block
                    if leading_spaces(lines[j]) == ind:
                        lines[j] = (" " * (ind + 4)) + lines[j].lstrip(" ")
                        nfix += 1
                    break
        i += 1

    TARGET.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"OK: brute-indented publish under try where needed (fixes={nfix})")

if __name__ == "__main__":
    main()
