import os, time, threading, sys
from decimal import Decimal
from pathlib import Path
import orjson

# Ensure repo root is on sys.path (so `import app.core...` works when running from app/tools)
REPO_ROOT = Path(__file__).resolve().parents[2]  # .../app/tools/ -> repo root
sys.path.insert(0, str(REPO_ROOT))

# Hard safety
os.environ["EXEC_DRY_RUN"] = "true"
os.environ["ACCOUNT_LABEL"] = os.environ.get("ACCOUNT_LABEL") or "main"
os.environ["AI_ROUTER_ENABLED"] = "true"
os.environ["AI_ROUTER_SEND_TG"] = "false"
os.environ["AI_ROUTER_POLL_SECONDS"] = "1"

print("=== PHASE1_3_LOCK_CHECK ===")
print("REPO_ROOT    =", str(REPO_ROOT))
print("ACCOUNT_LABEL =", os.environ["ACCOUNT_LABEL"])
print("EXEC_DRY_RUN  =", os.environ["EXEC_DRY_RUN"])

# Imports must succeed
import app.core.flashback_common as c
import app.core.execution_ws as e
import app.core.ai_action_router as r
import app.bots.ai_action_router as adapter

print("IMPORTS_OK = True")
print("HAS_qty_from_pct =", hasattr(c, "qty_from_pct"))

# 1) Direct normalize/apply should not crash on schema B-style
a1 = {
    "type":"open",
    "account_label":os.environ["ACCOUNT_LABEL"],
    "symbol":"BTCUSDT",
    "side":"long",
    "size_fraction":1.0,
    "dry_run":True,
    "ts_ms":int(time.time()*1000),
    "source":"phase123_lock_check"
}
res1 = r.apply_ai_action(a1)
print("DIRECT_APPLY_OK =", res1.get("ok"), "ERROR =", res1.get("error"))

# 2) End-to-end adapter tail test: append a new action line, run adapter briefly, ensure offset is written
actions_path = Path(os.getenv("AI_ACTIONS_PATH","state/ai_actions.jsonl"))
actions_path.parent.mkdir(parents=True, exist_ok=True)

a2 = {
    "type":"open",
    "account_label":os.environ["ACCOUNT_LABEL"],
    "symbol":"BTCUSDT",
    "side":"long",
    "size_fraction":1.0,
    "dry_run":True,
    "ts_ms":int(time.time()*1000),
    "source":"phase123_lock_check_adapter"
}
with actions_path.open("ab") as f:
    f.write(orjson.dumps(a2)); f.write(b"\n")

# Reset offset to force tailer to read deterministically
off = Path(f"state/offsets/ai_action_router_{os.environ['ACCOUNT_LABEL']}.offset")
off.parent.mkdir(parents=True, exist_ok=True)
if off.exists():
    off.unlink()

t = threading.Thread(target=adapter.loop, daemon=True)
t.start()
time.sleep(3)

print("OFFSET_EXISTS =", off.exists())
print("OFFSET_VAL =", off.read_text("utf-8", errors="ignore").strip() if off.exists() else None)

# 3) Execution_ws direct dry-run should still skip
ex = e.open_position_ws_first("BTCUSDT","LONG",Decimal("10"))
print("EXEC_WS_OPEN_OK =", ex.get("ok"), "SKIPPED =", ex.get("skipped"))

print("=== DONE ===")
