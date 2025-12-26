from pathlib import Path

p = Path(r"app\bots\signal_engine.py")
s = p.read_text(encoding="utf-8", errors="ignore")

if "DEDUPE_EMIT_V1" in s:
    raise SystemExit("REFUSE: DEDUPE_EMIT_V1 already applied")

# 1) Add seen_emit set at the start of each loop (near total_signals_this_loop reset)
needle1 = "total_signals_this_loop = 0"
if needle1 not in s:
    # fallback: try alternate spacing
    needle1 = "total_signals_this_loop=0"
    if needle1 not in s:
        raise SystemExit("PATCH FAILED: could not find total_signals_this_loop reset")

inject1 = (
    needle1 + "\n"
    "        seen_emit = set()  # DEDUPE_EMIT_V1: prevent duplicate observed.jsonl rows per run\n"
)

s2 = s.replace(needle1, inject1, 1)

# 2) Insert dedupe guard right after sub_uid is computed in the matched emit loop
needle2 = '                    sub_uid = str(strat.get("sub_uid"))'
if needle2 not in s2:
    raise SystemExit('PATCH FAILED: could not find sub_uid assignment line for emit loop')

guard = (
    needle2 + "\n"
    "                    _dk = (sub_uid, symbol, tf_label, bar_ts, side, reason)\n"
    "                    if _dk in seen_emit:\n"
    "                        continue\n"
    "                    seen_emit.add(_dk)  # DEDUPE_EMIT_V1\n"
)

s3 = s2.replace(needle2, guard, 1)

p.write_text(s3, encoding="utf-8", newline="\n")
print("OK: patched signal_engine.py (DEDUPE_EMIT_V1)")
