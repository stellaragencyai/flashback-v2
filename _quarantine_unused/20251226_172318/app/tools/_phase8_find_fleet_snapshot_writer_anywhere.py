from __future__ import annotations

from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]
APP = ROOT / "app"

targets = [
    "fleet_snapshot.json",
    "fleet_snapshot",
    "FLEET_SNAPSHOT",
    r"state\\fleet_snapshot",
    r"state/fleet_snapshot",
]

writer_tokens = [
    "write_text",
    "write_bytes",
    "open(",
    "json.dump",
    "json.dumps",
    "atomic",
    "tmp",
    "replace(",
    "rename(",
    "os.replace",
]

def hit_score(text: str) -> int:
    t = text.lower()
    score = 0
    for x in targets:
        if x.lower() in t:
            score += 5
    for x in writer_tokens:
        if x.lower() in t:
            score += 2
    return score

hits = []
for p in APP.rglob("*.py"):
    try:
        s = p.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        continue

    if not any(t.lower() in s.lower() for t in targets):
        continue

    lines = s.splitlines()
    for i, l in enumerate(lines, start=1):
        if any(t.lower() in l.lower() for t in targets) or any(w.lower() in l.lower() for w in writer_tokens):
            block = "\n".join(lines[max(0, i-6):min(len(lines), i+6)])
            score = hit_score(block)
            hits.append((score, str(p), i, l.strip(), block))

hits.sort(reverse=True, key=lambda x: (x[0], x[1], x[2]))

print("ROOT=", ROOT)
print("HITS=", len(hits))
print("\n=== TOP HITS (most likely writers) ===")
for score, fp, ln, one, block in hits[:25]:
    print("\n---")
    print(f"score={score} file={fp} line={ln}")
    print("line=", one)
    print("context:\n" + block)
