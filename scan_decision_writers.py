from pathlib import Path
import re

p = Path("app")
bad = []

def flags_writer(s: str) -> bool:
    if "ai_decisions.jsonl" not in s:
        return False

    # allow canonical writer
    if "app.core.ai_decision_logger" in s:
        return False

    # Strong evidence of direct writes to ai_decisions.jsonl
    direct_patterns = [
        r'DECISIONS_PATH\.open\(\s*["' + "'" + r']ab["' + "'" + r']\s*\)',
        r'AI_DECISIONS_PATH\.open\(\s*["' + "'" + r']ab["' + "'" + r']\s*\)',
        r'Path\(\s*["' + "'" + r']state/ai_decisions\.jsonl["' + "'" + r']\s*\)\.open\(\s*["' + "'" + r']ab["' + "'" + r']\s*\)',
        r'Path\(\s*["' + "'" + r']state\\ai_decisions\.jsonl["' + "'" + r']\s*\)\.open\(\s*["' + "'" + r']ab["' + "'" + r']\s*\)',
        r'os\.open\([^)]*ai_decisions\.jsonl',
        r'_os\.open\([^)]*ai_decisions\.jsonl',
    ]

    for pat in direct_patterns:
        if re.search(pat, s):
            return True

    return False

for f in p.rglob("*.py"):
    s = f.read_text(encoding="utf-8", errors="ignore")
    if flags_writer(s):
        bad.append(str(f))

print("RAW_DECISION_WRITERS:", len(bad))
print("\n".join(bad))
