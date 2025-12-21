from pathlib import Path
import json
import shutil

src = Path(r"state\ai_decisions.jsonl")
if not src.exists():
    print("MISSING:", src)
    raise SystemExit(1)

bak = Path(str(src) + ".bak_before_dedupe")
if not bak.exists():
    shutil.copy2(src, bak)

tmp = Path(str(src) + ".tmp")

seen = set()
kept = 0
dropped = 0
bad = 0

def s(x):
    return str(x) if x is not None else ""

def upper(x):
    return s(x).upper()

with src.open("r", encoding="utf-8", errors="ignore") as f, tmp.open("w", encoding="utf-8", newline="\n") as g:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except Exception:
            bad += 1
            continue

        trade_id = s(row.get("trade_id"))
        acct = s(row.get("account_label"))
        sym = upper(row.get("symbol"))
        tf = s(row.get("timeframe"))
        et = s(row.get("event_type"))

        # infer event_type if missing
        if not et:
            if row.get("decision_code") or row.get("decision") or (isinstance(row.get("payload"), dict) and row["payload"].get("decision")):
                et = "ai_decision"
            row["event_type"] = et

        # normalize decision_code
        dc = s(row.get("decision_code"))
        d = s(row.get("decision"))
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        if not dc and isinstance(payload, dict):
            dc = s(payload.get("decision_code"))
        if not d and isinstance(payload, dict):
            d = s(payload.get("decision"))
        if not dc and d:
            row["decision_code"] = d
            dc = d

        # drop junk placeholders
        if upper(dc) == "NO_DECISION":
            dropped += 1
            continue

        # stage
        extra = row.get("extra") if isinstance(row.get("extra"), dict) else {}
        stage = s(extra.get("stage"))
        if not stage:
            stage = "pilot" if et == "pilot_decision" else "enforced"
            row.setdefault("extra", {})
            if isinstance(row["extra"], dict):
                row["extra"]["stage"] = stage

        # schema_version
        sv = row.get("schema_version", None)
        try:
            svi = int(sv) if sv is not None and sv != "" else 0
        except Exception:
            svi = 0
        if svi <= 0:
            row["schema_version"] = 1 if et == "pilot_decision" else 2

        # canonical key
        key = (trade_id, acct, sym, tf, et, stage)
        if trade_id and acct and sym and key in seen:
            dropped += 1
            continue
        seen.add(key)

        g.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
        kept += 1

# swap in
src.replace(Path(str(src) + ".pre_dedupe"))
tmp.replace(src)

print("DEDUPE_OK")
print("Backup:", bak)
print("Kept:", kept, "Dropped:", dropped, "BadJSON:", bad)
