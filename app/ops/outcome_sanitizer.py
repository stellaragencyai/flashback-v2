from pathlib import Path
import json, math, collections, sys

ROOT = Path(r"C:\flashback")
STATE = ROOT / "state"
STATE.mkdir(exist_ok=True)

INP = STATE / "ai_events" / "outcomes.v1.jsonl"
OUT_OK = STATE / "outcomes.sanitized.v1.jsonl"
OUT_BAD = STATE / "outcomes.sanitized.rejects.jsonl"
OUT_STATS = STATE / "outcomes.sanitized.stats.json"

MIN_SAMPLE = 10
MIN_HOLD = 30

def finite(x):
    return isinstance(x, (int, float)) and math.isfinite(x)

if not INP.exists():
    raise SystemExit(f"❌ Missing input: {INP}")

counts = collections.Counter()
stats = collections.Counter()
sample_map = collections.Counter()

with INP.open(encoding="utf-8") as f:
    for line in f:
        try:
            o = json.loads(line)
        except Exception:
            counts["BAD_JSON"] += 1
            continue
        key = (o.get("account"), o.get("symbol"), o.get("setup"))
        sample_map[key] += 1

with (
    INP.open(encoding="utf-8") as fin,
    OUT_OK.open("w", encoding="utf-8") as ok,
    OUT_BAD.open("w", encoding="utf-8") as bad,
):
    for raw in fin:
        stats["total"] += 1
        try:
            o = json.loads(raw)
        except Exception:
            counts["BAD_JSON"] += 1
            continue

        def reject(reason):
            counts[reason] += 1
            o["reject_reason"] = reason
            bad.write(json.dumps(o) + "\n")

        for f in ("account","symbol","side","entry_ts","exit_ts","pnl"):
            if f not in o:
                reject("MISSING_FIELD"); break
        else:
            if o["exit_ts"] <= o["entry_ts"]:
                reject("INVALID_TS"); continue
            if not finite(o["pnl"]):
                reject("BAD_NUMERIC"); continue
            hold = o["exit_ts"] - o["entry_ts"]
            if hold < MIN_HOLD:
                reject("SHORT_HOLD"); continue
            key = (o.get("account"), o.get("symbol"), o.get("setup"))
            if sample_map[key] < MIN_SAMPLE:
                reject("LOW_SAMPLE"); continue
            exp = o.get("expectancy")
            if exp is not None and finite(exp) and exp < 0:
                reject("NEG_EXPECTANCY"); continue

            ok.write(json.dumps(o) + "\n")
            stats["kept"] += 1

OUT_STATS.write_text(json.dumps({
    "total": stats["total"],
    "kept": stats["kept"],
    "rejected": sum(counts.values()),
    "reasons": counts,
}, indent=2))

print(f"OK: Sanitized {stats['kept']} / {stats['total']} outcomes")
