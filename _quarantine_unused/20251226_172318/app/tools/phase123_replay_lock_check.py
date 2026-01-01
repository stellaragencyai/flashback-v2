import os, time, threading, sys
from pathlib import Path
import orjson

# Ensure repo root on path
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

os.environ["ACCOUNT_LABEL"] = "main"
os.environ["AI_ROUTER_ENABLED"] = "true"
os.environ["AI_ROUTER_SEND_TG"] = "false"
os.environ["AI_ROUTER_POLL_SECONDS"] = "1"
os.environ["EXEC_DRY_RUN"] = "true"

import app.bots.ai_action_router as b

actions = Path("state/ai_actions.jsonl")
actions.parent.mkdir(parents=True, exist_ok=True)

offset = Path("state/offsets/ai_action_router_main.offset")
offset.parent.mkdir(parents=True, exist_ok=True)

# Append ONE fresh action
a = {
    "type": "open",
    "account_label": "main",
    "symbol": "BTCUSDT",
    "side": "long",
    "size_fraction": 1.0,
    "dry_run": True,
    "ts_ms": int(time.time() * 1000),
    "source": "phase123_replay_test"
}

with actions.open("ab") as f:
    f.write(orjson.dumps(a))
    f.write(b"\n")

# Reset offset
if offset.exists():
    offset.unlink()

# Run adapter first time
t1 = threading.Thread(target=b.loop, daemon=True)
t1.start()
time.sleep(3)

val1 = offset.read_text("utf-8", errors="ignore").strip() if offset.exists() else ""

# Run adapter second time (no new actions)
t2 = threading.Thread(target=b.loop, daemon=True)
t2.start()
time.sleep(3)

val2 = offset.read_text("utf-8", errors="ignore").strip() if offset.exists() else ""

print("OFFSET1 =", val1)
print("OFFSET2 =", val2)
print("SAME    =", val1 == val2)
