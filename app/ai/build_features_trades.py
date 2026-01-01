import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STATE = ROOT / "state"

SRC = STATE / "trades_bus.json"
DST = STATE / "features_trades.jsonl"

if not SRC.exists():
    raise FileNotFoundError(SRC)

count = 0
with open(SRC, "r", encoding="utf-8") as f, open(DST, "w", encoding="utf-8") as out:
    trades = json.load(f)
    for t in trades:
        if not t.get("exit_price"):
            continue
        out.write(json.dumps(t) + "\n")
        count += 1

print(f"[build_features_trades] wrote {count} rows -> {DST}")
