from pathlib import Path

p = Path(r"app\bots\signal_engine.py")
s = p.read_text(encoding="utf-8", errors="ignore")

if "WARN_TUNE_V1" in s:
    raise SystemExit("REFUSE: WARN_TUNE_V1 already applied")

old = (
"        # COUNTERS_CLEANUP_V1: warn if strategies exist but nothing matched (helps catch silent logic death)\n"
"        if c_setups_checked > 0 and c_matched_added == 0:\n"
"            print(f\"[WARN] strategies_present_but_no_match setups_checked={c_setups_checked} missing_logic={c_missing_logic} logic_none={c_logic_none} regime_blocked={c_regime_blocked} setup_types_empty={c_setup_types_empty}\", flush=True)\n"
)

if old not in s:
    raise SystemExit("PATCH FAILED: could not find the COUNTERS_CLEANUP_V1 warn block to replace")

new = (
"        # WARN_TUNE_V1: no-match is normal. Warn only on suspicious conditions.\n"
"        if c_setups_checked > 0 and c_matched_added == 0:\n"
"            suspicious = (\n"
"                (c_missing_logic > 0) or\n"
"                (c_setup_types_empty > 0) or\n"
"                (c_regime_blocked == c_setups_checked and c_setups_checked >= 3)\n"
"            )\n"
"            if suspicious:\n"
"                print(\n"
"                    f\"[WARN] suspicious_no_match setups_checked={c_setups_checked} missing_logic={c_missing_logic} \"\n"
"                    f\"logic_none={c_logic_none} regime_blocked={c_regime_blocked} setup_types_empty={c_setup_types_empty}\",\n"
"                    flush=True,\n"
"                )\n"
)

s2 = s.replace(old, new)
p.write_text(s2, encoding="utf-8", newline="\n")
print("OK: patched signal_engine.py (WARN_TUNE_V1)")
