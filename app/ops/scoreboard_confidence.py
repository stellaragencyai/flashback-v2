from pathlib import Path
import json, statistics, sys

ROOT = Path(r"C:\flashback")
STATE = ROOT / "state"
SB_DIR = STATE / "scoreboard"

INP = SB_DIR / "scoreboard.v1.json"
OUT = SB_DIR / "scoreboard.v1.confidence.json"

SHRINK_K = 50  # strength of prior

if not INP.exists():
    sys.exit("❌ Missing scoreboard.v1.json")

data = json.loads(INP.read_text(encoding="utf-8"))

rows = data.get("rows") or data.get("scoreboard") or []
if not rows:
    sys.exit("❌ No rows found in scoreboard")

# --- Compute global prior expectancy ---
exps = [r["expectancy"] for r in rows if isinstance(r.get("expectancy"), (int, float))]
prior = statistics.mean(exps) if exps else 0.0

for r in rows:
    n = max(int(r.get("n", 0)), 0)
    exp = r.get("expectancy", 0.0)

    weight = n / (n + SHRINK_K) if n > 0 else 0.0
    adj = weight * exp + (1 - weight) * prior

    r["expectancy_raw"] = exp
    r["expectancy_adj"] = round(adj, 6)
    r["confidence"] = round(weight, 4)
    r["prior_expectancy"] = round(prior, 6)

# --- Write output ---
OUT.write_text(json.dumps({
    "schema": "scoreboard.v1.confidence",
    "generated_from": "scoreboard.v1",
    "shrink_k": SHRINK_K,
    "prior_expectancy": round(prior, 6),
    "rows": rows
}, indent=2), encoding="utf-8")

print(f"✅ Confidence model applied")
print(f"   Prior expectancy: {prior:.6f}")
print(f"   Output: {OUT}")
