from __future__ import annotations

from pathlib import Path
import re
import sys

TARGET = Path(r"app\tools\ai_scoreboard_snapshot_v1.py")

def die(msg: str, code: int = 1) -> None:
    print(f"ERROR: {msg}")
    raise SystemExit(code)

def main() -> None:
    if not TARGET.exists():
        die(f"Target file missing: {TARGET}")

    txt = TARGET.read_text(encoding="utf-8")

    # 1) Ensure argparse defines --min-conf
    if "--min-conf" not in txt:
        # Try to insert right after --min-n argument
        # This matches a typical argparse add_argument for --min-n (with any spacing)
        pat = r"(ap\.add_argument\(\s*[\"']--min-n[\"'][^\n]*\)\s*\n)"
        m = re.search(pat, txt)
        if not m:
            die("Could not find argparse add_argument('--min-n', ...) to anchor --min-conf insertion.")
        insert = (
            m.group(1)
            + "    ap.add_argument(\"--min-conf\", type=float, default=None)\n"
        )
        txt = txt[:m.start(1)] + insert + txt[m.end(1):]

    # 2) Ensure output JSON includes min_conf
    # We look for the output dict that contains schema_version and min_n, then inject min_conf if missing.
    # This is intentionally tolerant of whitespace and ordering (within reason).
    if '"min_conf"' not in txt and "'min_conf'" not in txt:
        # Find the dict literal passed to json.dump / write (common in this script)
        # Anchor on schema_version + min_n within the same dict block.
        pat = r"(\{\s*(?:.|\n){0,800}?[\"']schema_version[\"']\s*:\s*[\"']scoreboard\.v1[\"']\s*,\s*(?:.|\n){0,800}?[\"']min_n[\"']\s*:\s*[^,\n}]+,\s*)"
        m = re.search(pat, txt)
        if not m:
            die("Could not find the output dict block containing schema_version=scoreboard.v1 and min_n to inject min_conf.")
        # Inject min_conf right after min_n entry area we captured
        injection = m.group(1) + "        \"min_conf\": args.min_conf,\n"
        txt = txt[:m.start(1)] + injection + txt[m.end(1):]

    # 3) Ensure args.min_conf is actually referenced somewhere; if not, that's fine because we inserted it above.
    # 4) Write back
    TARGET.write_text(txt, encoding="utf-8")
    print("OK: patched ai_scoreboard_snapshot_v1.py to support --min-conf and write min_conf into scoreboard.v1.json")

if __name__ == "__main__":
    main()
