import json
from pathlib import Path

paths = [
    Path("state/ai_events/setups.jsonl"),
    Path("state/ai_events/outcomes.v1.jsonl"),
    Path("state/ai_events/ws_executions.jsonl"),
    Path("state/ai_events/ai_decisions.jsonl"),
    Path("state/ai_actions.jsonl"),
]

for p in paths:
    if not p.exists():
        print("CHECK", p, ": MISSING")
        continue

    bad = 0
    n = 0
    text = p.read_text(encoding="utf-8", errors="ignore")

    for line in text.splitlines():
        if not line.strip():
            continue
        n += 1
        try:
            json.loads(line)
        except Exception:
            bad += 1

    b = p.read_bytes()
    print(
        "CHECK", p,
        "size=", p.stat().st_size,
        "HAS_NL=", (b"\n" in b),
        "lines=", n,
        "bad=", bad
    )
