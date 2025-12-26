from __future__ import annotations

from pathlib import Path

TARGET = Path(r"app\ai\ai_decision_enforcer.py")

def main() -> int:
    if not TARGET.exists():
        print(f"ERR: missing {TARGET}")
        return 2

    lines = TARGET.read_text(encoding="utf-8", errors="ignore").splitlines()

    # Find "read_jsonl_tail = None" then unindent subsequent lines until "DECISIONS_PATH"
    start = None
    for i, line in enumerate(lines):
        if line.strip().startswith("read_jsonl_tail") and "None" in line:
            start = i
            break

    if start is None:
        print("ERR: could not find 'read_jsonl_tail = None' in file.")
        return 3

    end = None
    for j in range(start + 1, len(lines)):
        if lines[j].lstrip().startswith("DECISIONS_PATH"):
            end = j
            break

    if end is None:
        print("ERR: could not find 'DECISIONS_PATH' after read_jsonl_tail block.")
        return 4

    changed = 0
    for k in range(start + 1, end):
        # Only adjust lines that are indented at least 4 spaces
        if lines[k].startswith("    "):
            lines[k] = lines[k][4:]
            changed += 1

    TARGET.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"OK: unindented lines={changed} in ai_decision_enforcer.py (block {start+1}->{end})")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
