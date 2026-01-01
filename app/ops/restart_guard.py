from pathlib import Path
import json
import time

STATE_PATH = Path("state/orchestrator_state.json")
COOLDOWN_SECONDS = 60
MAX_RESTARTS_PER_HOUR = 3

def load_state():
    if not STATE_PATH.exists():
        return {}
    return json.loads(STATE_PATH.read_text())

def save_state(state):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2))

def can_restart(name):
    state = load_state()
    s = state.get(name, {})

    now = time.time()
    last = s.get("last_start_ts", 0)
    restarts = s.get("restart_ts", [])

    # cooldown
    if now - last < COOLDOWN_SECONDS:
        return False

    # rate limit
    restarts = [t for t in restarts if now - t < 3600]
    if len(restarts) >= MAX_RESTARTS_PER_HOUR:
        return False

    return True

def mark_restart(name):
    state = load_state()
    s = state.setdefault(name, {})
    now = time.time()

    s["last_start_ts"] = now
    s.setdefault("restart_ts", []).append(now)

    save_state(state)
