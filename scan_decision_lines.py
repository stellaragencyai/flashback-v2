from pathlib import Path
import re

files = [
    r"app\ai\ai_decision_outcome_linker.py",
    r"app\ai\ai_memory_contract.py",
    r"app\bots\t.py",
    r"app\tools\ai_backfill_decisions_for_all_outcomes.py",
]

patterns = [
    r"ai_decisions\.jsonl",
    r'open\("ab"\)',
    r'open\("a"\)',
    r"_append_jsonl\(",
]

for fp in files:
    p = Path(fp)
    print("\n====", fp, "====")
    if not p.exists():
        print("FILE NOT FOUND")
        continue

    lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()
    for i, line in enumerate(lines, 1):
        if any(re.search(pt, line) for pt in patterns):
            print(f"{i:04d}: {line}")
