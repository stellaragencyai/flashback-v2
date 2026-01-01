from pathlib import Path

p = Path(r"app\bots\signal_engine.py")
lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()

# Refuse if already patched
if any("STRAT_COUNTERS_V1" in l for l in lines):
    raise SystemExit("REFUSE: counters patch already applied (STRAT_COUNTERS_V1 found)")

# 1) Insert counter vars right after matched list init
anchor = "matched: List[Dict[str, Any]] = []"
idx = None
for i,l in enumerate(lines):
    if l.strip() == anchor.strip():
        idx = i
        break
if idx is None:
    raise SystemExit("PATCH FAILED: could not find matched list anchor")

indent = lines[idx][:len(lines[idx]) - len(lines[idx].lstrip())]
insert = [
    indent + "# STRAT_COUNTERS_V1",
    indent + "c_setup_types_empty = 0",
    indent + "c_setups_checked = 0",
    indent + "c_missing_logic = 0",
    indent + "c_logic_none = 0",
    indent + "c_regime_blocked = 0",
    indent + "c_matched_added = 0",
]
out = lines[:idx+1] + insert + lines[idx+1:]

# 2) Wrap logic_fn lookup with counters
needle = "logic_fn = SETUP_LOGIC.get(str(setup))"
hit = None
for i,l in enumerate(out):
    if needle in l:
        hit = i
        break
if hit is None:
    raise SystemExit("PATCH FAILED: could not find logic_fn lookup line")

loop_indent = out[hit][:len(out[hit]) - len(out[hit].lstrip())]
inject_block = [
    loop_indent + "c_setups_checked += 1",
    out[hit],
    loop_indent + "if not logic_fn:",
    loop_indent + "    c_missing_logic += 1",
    loop_indent + "    continue",
]
out = out[:hit] + inject_block + out[hit+1:]

# 3) Count logic returning None
needle2 = "s_side, s_reason = logic_fn(candles)"
hit2 = None
for i,l in enumerate(out):
    if needle2 in l:
        hit2 = i
        break
if hit2 is None:
    raise SystemExit("PATCH FAILED: could not find logic_fn(candles) call line")

call_indent = out[hit2][:len(out[hit2]) - len(out[hit2].lstrip())]
inject2 = [
    out[hit2],
    call_indent + "if not s_side:",
    call_indent + "    c_logic_none += 1",
    call_indent + "    continue",
]
out = out[:hit2] + inject2 + out[hit2+1:]

# 4) Count matched additions
hit3 = None
for i,l in enumerate(out):
    if "matched.append(" in l:
        hit3 = i
        break
if hit3 is None:
    raise SystemExit("PATCH FAILED: could not find matched.append(...)")

app_indent = out[hit3][:len(out[hit3]) - len(out[hit3].lstrip())]
out = out[:hit3] + [app_indent + "c_matched_added += 1"] + out[hit3:]

# 5) Count setup_types empty
needle4 = 'setup_types = _get_strat_attr(raw_obj, "setup_types", []) or []'
hit4 = None
for i,l in enumerate(out):
    if needle4 in l:
        hit4 = i
        break
if hit4 is not None:
    st_indent = out[hit4][:len(out[hit4]) - len(out[hit4].lstrip())]
    out = out[:hit4+1] + [
        st_indent + "if not setup_types:",
        st_indent + "    c_setup_types_empty += 1",
    ] + out[hit4+1:]

# 6) Print counters once per WHILE loop, right before sleeping
sleep_idx = None
for i,l in enumerate(out):
    if "time.sleep(" in l:
        sleep_idx = i
        break
if sleep_idx is None:
    raise SystemExit("PATCH FAILED: could not find time.sleep(...) anchor")

sl_indent = out[sleep_idx][:len(out[sleep_idx]) - len(out[sleep_idx].lstrip())]
dbg_line = sl_indent + 'print(f"[DBG] setups_checked={c_setups_checked} missing_logic={c_missing_logic} logic_none={c_logic_none} setup_types_empty={c_setup_types_empty} matched_added={c_matched_added}")'

# Insert print immediately before the first time.sleep
out = out[:sleep_idx] + [dbg_line] + out[sleep_idx:]

p.write_text("\n".join(out) + "\n", encoding="utf-8", newline="\n")
print("OK: patched signal_engine.py with STRAT_COUNTERS_V1 (sleep-anchor v2)")
