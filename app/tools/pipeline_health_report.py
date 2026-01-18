import json
from pathlib import Path
from collections import defaultdict

SETUPS = Path("state/ai_events/setups.jsonl")
OUTS   = Path("state/ai_events/outcomes.v1.jsonl")

def read_jsonl(p: Path):
    if not p.exists():
        return []
    out = []
    for ln in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        ln = ln.strip()
        if not ln:
            continue
        try:
            out.append(json.loads(ln))
        except Exception:
            continue
    return out

setups = read_jsonl(SETUPS)
outs   = read_jsonl(OUTS)

# --- setup detection stats (so we can see what we are actually parsing) ---
setup_detect_event_type = 0
setup_detect_schema_ver = 0
setup_detect_other = 0

setup_by_tid = {}
setup_count_by_acct = defaultdict(int)

def _is_setup_context_row(o: dict) -> bool:
    # Accept both historical formats:
    # 1) event_type: "setup_context"
    # 2) schema_version: "setup_context.v1"
    if o.get("event_type") == "setup_context":
        return True
    if o.get("schema_version") == "setup_context.v1":
        return True
    return False

for o in setups:
    if not isinstance(o, dict):
        continue

    if _is_setup_context_row(o) and o.get("trade_id"):
        # stats
        if o.get("event_type") == "setup_context":
            setup_detect_event_type += 1
        elif o.get("schema_version") == "setup_context.v1":
            setup_detect_schema_ver += 1
        else:
            setup_detect_other += 1

        tid = str(o["trade_id"])
        setup_by_tid[tid] = o
        acct = str(o.get("account_label") or "main")
        setup_count_by_acct[acct] += 1
    else:
        setup_detect_other += 1

out_count_by_acct = defaultdict(int)
join_by_acct = defaultdict(int)
unknown_by_acct = defaultdict(int)
orph_by_acct = defaultdict(int)

for o in outs:
    if not isinstance(o, dict):
        continue

    acct = str(o.get("account_label") or "main")
    out_count_by_acct[acct] += 1

    tid = str(o.get("trade_id") or "")
    st = o.get("setup_type")
    if st in (None, "", "unknown"):
        unknown_by_acct[acct] += 1

    if tid and tid in setup_by_tid:
        join_by_acct[acct] += 1
    else:
        orph_by_acct[acct] += 1

accts = sorted(set(list(out_count_by_acct.keys()) + list(setup_count_by_acct.keys())))

print("=== PIPELINE HEALTH REPORT ===")
print(f"SETUPS_FILE={SETUPS} exists={SETUPS.exists()} size={SETUPS.stat().st_size if SETUPS.exists() else 0}")
print(f"OUTS_FILE  ={OUTS} exists={OUTS.exists()} size={OUTS.stat().st_size if OUTS.exists() else 0}")
print("")
print("SETUP DETECTION:")
print(f"  matched event_type==setup_context     : {setup_detect_event_type}")
print(f"  matched schema_version==setup_context.v1: {setup_detect_schema_ver}")
print(f"  other/ignored rows                    : {setup_detect_other}")
print("")
print("ACCOUNT      setups_sc   outcomes   joined   join_pct   unknown   unk_pct   orphans")
for a in accts:
    sc = setup_count_by_acct.get(a, 0)
    oc = out_count_by_acct.get(a, 0)
    jc = join_by_acct.get(a, 0)
    uc = unknown_by_acct.get(a, 0)
    orc = orph_by_acct.get(a, 0)
    jp = (100.0 * jc / oc) if oc else 0.0
    up = (100.0 * uc / oc) if oc else 0.0
    print(f"{a:<11} {sc:9d} {oc:9d} {jc:8d} {jp:8.2f}% {uc:9d} {up:7.2f}% {orc:8d}")

print("")
print("NOTE: join = outcomes with matching setup_context by trade_id")
