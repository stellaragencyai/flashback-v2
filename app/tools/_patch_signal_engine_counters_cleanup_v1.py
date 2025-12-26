from pathlib import Path
import re

p = Path(r"app\bots\signal_engine.py")
s = p.read_text(encoding="utf-8", errors="ignore")

if "COUNTERS_CLEANUP_V1" in s:
    raise SystemExit("REFUSE: COUNTERS_CLEANUP_V1 already applied")

# 1) Remove duplicate 'if not logic_fn' block (keep the first one that increments counter)
s2 = s.replace(
    "                        if not logic_fn:\n"
    "                            c_missing_logic += 1\n"
    "                            continue\n"
    "                        if not logic_fn:\n"
    "                            continue\n",
    "                        if not logic_fn:\n"
    "                            c_missing_logic += 1\n"
    "                            continue\n"
)

# 2) Remove duplicate 'if not s_side' block (keep the first one that increments counter)
s3 = s2.replace(
    "                        s_side, s_reason = logic_fn(candles)\n"
    "                        if not s_side:\n"
    "                            c_logic_none += 1\n"
    "                            continue\n"
    "                        if not s_side:\n"
    "                            continue\n",
    "                        s_side, s_reason = logic_fn(candles)\n"
    "                        if not s_side:\n"
    "                            c_logic_none += 1\n"
    "                            continue\n"
)

# 3) Increment c_regime_blocked when regime filters fail
needle = "                        if not passes_regime_filters(raw_obj, regime_ind):\n                            continue\n"
if needle not in s3:
    raise SystemExit("PATCH FAILED: could not find regime filter continue block")

s4 = s3.replace(
    needle,
    "                        if not passes_regime_filters(raw_obj, regime_ind):\n"
    "                            c_regime_blocked += 1  # COUNTERS_CLEANUP_V1\n"
    "                            continue\n"
)

# 4) Add WARN line if strategy has setups but matched_added is zero for that loop iteration
# Insert just before the existing DBG summary print at end of loop.
dbg_line = "        print(f\"[DBG] setups_checked={c_setups_checked} missing_logic={c_missing_logic} logic_none={c_logic_none} setup_types_empty={c_setup_types_empty} matched_added={c_matched_added}\", flush=True)"
if dbg_line not in s4:
    raise SystemExit("PATCH FAILED: could not find end-of-loop DBG print line")

warn_block = (
    "        # COUNTERS_CLEANUP_V1: warn if strategies exist but nothing matched (helps catch silent logic death)\n"
    "        if c_setups_checked > 0 and c_matched_added == 0:\n"
    "            print(f\"[WARN] strategies_present_but_no_match setups_checked={c_setups_checked} missing_logic={c_missing_logic} logic_none={c_logic_none} regime_blocked={c_regime_blocked} setup_types_empty={c_setup_types_empty}\", flush=True)\n"
)

s5 = s4.replace(dbg_line, warn_block + dbg_line)

p.write_text(s5, encoding="utf-8", newline="\n")
print("OK: patched signal_engine.py (COUNTERS_CLEANUP_V1)")
