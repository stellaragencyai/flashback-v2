import json
from pathlib import Path

p = Path(r"C:\Flashback\signals\observed.jsonl")
print("exists=", p.exists())
if not p.exists():
    raise SystemExit("MISSING observed.jsonl. Run signal_engine for ~60s first.")

lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()
rows = []
for l in lines:
    l = l.strip()
    if not l:
        continue
    try:
        rows.append(json.loads(l))
    except Exception:
        pass

print("rows=", len(rows))
fallback_count = sum(1 for r in rows if isinstance(r, dict) and "fallback:" in str(r.get("reason","")))
subuid_none_count = sum(1 for r in rows if isinstance(r, dict) and r.get("sub_uid") == "None")
subuid_null_count = sum(1 for r in rows if isinstance(r, dict) and r.get("sub_uid") is None)

print("fallback_count=", fallback_count)
print('sub_uid == "None" count=', subuid_none_count)
print("sub_uid is None count=", subuid_null_count)

print("sample_first_5=")
for r in rows[:5]:
    print(r)
