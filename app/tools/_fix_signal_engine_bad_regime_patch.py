from __future__ import annotations

from pathlib import Path

TARGET = Path(r"app\bots\signal_engine.py")

BLOCK_MARKERS = [
    "PATCH: ensure regime indicators are emitted into observed.debug",
    "PATCH: attach regime indicators into debug payload",
]

def main() -> int:
    if not TARGET.exists():
        print(f"FAIL: missing {TARGET}")
        return 2

    lines = TARGET.read_text(encoding="utf-8", errors="ignore").splitlines()
    out = []
    removed = 0
    i = 0

    def _is_marker(line: str) -> bool:
        return any(m in line for m in BLOCK_MARKERS)

    while i < len(lines):
        line = lines[i]

        # If we hit one of our injected patch blocks, remove it.
        if _is_marker(line):
            # Remove the marker line and the following indented try/except/pass block.
            removed += 1
            i += 1

            # Consume subsequent lines that are part of that injected block.
            # Heuristic: keep consuming while line starts with whitespace and contains
            # try/except/pass/_fb_regime_ind/fb_debug/regime_ind, or until we hit a blank line.
            while i < len(lines):
                l2 = lines[i]
                if l2.strip() == "":
                    i += 1
                    break
                if any(tok in l2 for tok in ("try:", "except", "pass", "_fb_regime_ind", "fb_debug", "regime_ind")):
                    i += 1
                    continue
                # If the indentation is deep (still within a block), keep consuming a little more safely
                if l2.startswith(" " * 8) or l2.startswith("\t"):
                    i += 1
                    continue
                break

            continue

        out.append(line)
        i += 1

    TARGET.write_text("\n".join(out) + "\n", encoding="utf-8", newline="\n")
    print(f"OK: cleaned injected regime patch blocks from signal_engine.py removed_blocks={removed}")

    # Compile check
    try:
        compile(TARGET.read_text(encoding="utf-8", errors="ignore"), str(TARGET), "exec")
        print("PASS: signal_engine.py compiles")
        return 0
    except Exception as e:
        print("FAIL: still does not compile:", e)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
