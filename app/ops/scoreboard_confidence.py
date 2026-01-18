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

# Support both schemas:
#  - legacy: rows=[...]
#  - current: buckets=[...]
rows = None
mode = None
if isinstance(data.get("buckets"), list):
    rows = data.get("buckets") or []
    mode = "buckets"
elif isinstance(data.get("rows"), list):
    rows = data.get("rows") or []
    mode = "rows"
elif isinstance(data.get("scoreboard"), list):
    rows = data.get("scoreboard") or []
    mode = "scoreboard"
else:
    rows = []
    mode = "unknown"

if not rows:
    sys.exit("❌ No rows found in scoreboard (expected buckets[] or rows[])")

# --- Compute global prior expectancy ---
exps = []
for r in rows:
    if isinstance(r, dict):
        v = r.get("expectancy")
        if isinstance(v, (int, float)):
            exps.append(float(v))
prior = statistics.mean(exps) if exps else 0.0

# --- Apply shrinkage to expectancy and compute confidence_shrink ---
for r in rows:
    if not isinstance(r, dict):
        continue
    n = int(r.get("n", 0) or 0)
    n = max(n, 0)
    exp = float(r.get("expectancy", 0.0) or 0.0)

    weight = (n / (n + SHRINK_K)) if n > 0 else 0.0
    adj = (weight * exp) + ((1 - weight) * prior)

    r["expectancy_raw"] = exp
    r["expectancy_adj"] = round(adj, 6)
    r["confidence_shrink"] = round(weight, 4)
    r["prior_expectancy"] = round(prior, 6)

# --- Write output (do not mutate input schema) ---
OUT.write_text(json.dumps({
    "schema": "scoreboard.v1.confidence",
    "generated_from": "scoreboard.v1",
    "input_mode": mode,
    "shrink_k": SHRINK_K,
    "prior_expectancy": round(prior, 6),
    "rows": rows
}, indent=2), encoding="utf-8")

print("✅ Confidence model applied")
print(f"   Input mode: {mode}")
print(f"   Prior expectancy: {prior:.6f}")
print(f"   Output: {OUT}")
