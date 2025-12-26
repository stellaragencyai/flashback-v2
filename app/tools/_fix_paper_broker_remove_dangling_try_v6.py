from __future__ import annotations
from pathlib import Path

TARGET = Path(r"app\sim\paper_broker.py")

def indent(line: str) -> int:
    return len(line) - len(line.lstrip(" "))

def main() -> None:
    if not TARGET.exists():
        raise SystemExit(f"FAIL: missing {TARGET}")

    text = TARGET.read_text(encoding="utf-8", errors="ignore").replace("\t", "    ")
    lines = text.splitlines()

    removed = 0

    i = 0
    while i < len(lines):
        line = lines[i]
        if line.lstrip() == "try:":
            base = indent(line)
            # Look ahead a bit for an except/finally at the SAME indent level
            found_handler = False
            for j in range(i + 1, min(i + 25, len(lines))):
                lj = lines[j]
                # If we dedent back to base indent (or less) without finding handler, stop
                if indent(lj) <= base and lj.strip() != "":
                    break
                stripped = lj.lstrip()
                if (stripped.startswith("except") or stripped.startswith("finally")) and indent(lj) == base:
                    found_handler = True
                    break

            if not found_handler:
                # Dangling try: remove it.
                lines.pop(i)
                removed += 1
                continue  # don't increment i; re-check current index
        i += 1

    TARGET.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"OK: removed dangling try blocks (removed={removed})")

if __name__ == "__main__":
    main()
