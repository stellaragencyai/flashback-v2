from pathlib import Path
import orjson

p = Path("state/ai_decisions.jsonl")
if not p.exists():
    raise SystemExit("missing state/ai_decisions.jsonl")

out = []
changed = 0
kept = 0

for b in p.read_bytes().splitlines():
    s = b.strip()
    if not s:
        continue
    if s[:1] != b"{":
        out.append(b)
        continue
    try:
        d = orjson.loads(s)
    except Exception:
        out.append(b)
        continue
    if not isinstance(d, dict):
        out.append(b)
        continue

    # Only tag legacy pilot rows where event_type is missing/None
    if (d.get("event_type") is None) and (d.get("schema_version") == 1) and ("decision" in d):
        d["event_type"] = "pilot_decision"
        changed += 1
        out.append(orjson.dumps(d))
        kept += 1
        continue

    out.append(b)
    kept += 1

backup = p.with_suffix(".jsonl.bak_pre_pilot_tag")
backup.write_bytes(p.read_bytes())
p.write_bytes(b"\n".join(out) + (b"\n" if out else b""))

print("backup", str(backup))
print("changed", changed)
print("lines_written", len(out))
