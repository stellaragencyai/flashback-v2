from __future__ import annotations

from pathlib import Path

TARGET = Path(r"app\core\observed_contract.py")

def main() -> int:
    if not TARGET.exists():
        print(f"FAIL: missing {TARGET}")
        return 2

    s = TARGET.read_text(encoding="utf-8", errors="ignore").splitlines()

    # We will insert regime into a debug allowlist near raw_reason without altering raw_reason.
    # Works for patterns like:
    #   DEBUG_KEYS = {"engine": ..., "raw_reason": ..., ...}
    # or
    #   DEBUG_KEYS = ["engine", "raw_reason", ...]
    #
    # We insert regime only if not already present.

    if any("regime" in line for line in s):
        print("OK: observed_contract already mentions regime (no-op)")
        return 0

    out = []
    inserted = False

    for i, line in enumerate(s):
        out.append(line)

        # Dict-style: look for a line that contains raw_reason as a key with a colon.
        if (not inserted) and ("raw_reason" in line) and (":" in line):
            indent = line.split('"raw_reason"')[0] if '"raw_reason"' in line else line.split("'raw_reason'")[0]
            # Insert regime key with a safe default value that won't be used, just carried through.
            # We choose None.
            out.append(f"{indent}\"regime\": None,  # PATCH: allow debug.regime")
            inserted = True
            continue

        # List-style: raw_reason as an item
        if (not inserted) and ("raw_reason" in line) and (":" not in line) and (line.strip().rstrip(",") in ('"raw_reason"', "'raw_reason'")):
            indent = line.split('"raw_reason"')[0] if '"raw_reason"' in line else line.split("'raw_reason'")[0]
            out.append(f"{indent}\"regime\",  # PATCH: allow debug.regime")
            inserted = True
            continue

    if not inserted:
        print("FAIL: could not find a safe insertion point near raw_reason in observed_contract.py")
        return 1

    TARGET.write_text("\n".join(out) + "\n", encoding="utf-8", newline="\n")
    print("OK: patched observed_contract.py (added debug.regime safely)")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
