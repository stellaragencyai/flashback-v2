from __future__ import annotations

from pathlib import Path
import re

TARGET = Path(r"app\sim\paper_broker.py")

def main() -> None:
    if not TARGET.exists():
        raise SystemExit(f"FAIL: missing {TARGET}")

    s = TARGET.read_text(encoding="utf-8", errors="ignore").replace("\t", "    ")

    # Fix pattern:
    #   <indent>try:
    #   <same indent>_publish_positions_bus(...)
    #
    # Should be:
    #   <indent>try:
    #   <indent+4>_publish_positions_bus(...)
    #
    # Also handles optional blank lines between try and publish.
    pat = re.compile(
        r"(?m)^(?P<ind>[ ]*)try:\s*\n(?:(?:[ ]*)\n)*^(?P=ind)(?P<call>_publish_positions_bus\s*\(.*\))\s*$"
    )

    def repl(m: re.Match) -> str:
        ind = m.group("ind")
        call = m.group("call")
        return f"{ind}try:\n{ind}    {call}"

    s2, n = pat.subn(repl, s)

    if n == 0:
        # Give a more direct hint if the exact pattern didn't match
        # Try a looser repair: find "try:" followed soon by publish at same indent
        pat2 = re.compile(r"(?m)^(?P<ind>[ ]*)try:\s*\n^(?P=ind)_publish_positions_bus", re.MULTILINE)
        if pat2.search(s2):
            s2 = pat2.sub(r"\g<ind>try:\n\g<ind>    _publish_positions_bus", s2)
            n = 1

    TARGET.write_text(s2, encoding="utf-8")
    print(f"OK: fixed dangling try before _publish_positions_bus (edits={n})")

if __name__ == "__main__":
    main()
