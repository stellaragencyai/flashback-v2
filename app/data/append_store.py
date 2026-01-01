import json
from pathlib import Path

PROGRESS = Path('state/features/progress.json')

def load_progress():
    if not PROGRESS.exists():
        return {}
    try:
        return json.load(PROGRESS.open('r', encoding='utf-8'))
    except Exception:
        return {}

def save_progress(p):
    PROGRESS.parent.mkdir(parents=True, exist_ok=True)
    json.dump(p, PROGRESS.open('w', encoding='utf-8'), indent=2)

def append_rows(path, rows, last_ts=None):
    if not rows:
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a', encoding='utf-8') as f:
        for r in rows:
            f.write(json.dumps(r, separators=(',', ':')) + '\n')

    if last_ts is not None:
        prog = load_progress()
        prog['last_ts'] = last_ts
        save_progress(prog)
