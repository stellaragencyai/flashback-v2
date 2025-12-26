from pathlib import Path

p = Path(r"app\bots\signal_engine.py")
lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()

anchor = "            matched: List[Dict[str, Any]] = []"
idx = None
for i,l in enumerate(lines):
    if l.strip() == anchor.strip():
        idx = i
        break

if idx is None:
    raise SystemExit("PATCH FAILED: could not find matched list anchor")

# Refuse if already patched
if any("STRAT_COUNTERS_V1" in l for l in lines):
    raise SystemExit("REFUSE: counters patch already applied")

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

# Insert right AFTER the matched list line
out = lines[:idx+1] + insert + lines[idx+1:]

# Now patch inside the setup loop
# We look for the exact line: logic_fn = SETUP_LOGIC.get(str(setup))
needle = "logic_fn = SETUP_LOGIC.get(str(setup))"
hit = None
for i,l in enumerate(out):
    if needle in l:
        hit = i
        break
if hit is None:
    raise SystemExit("PATCH FAILED: could not find logic_fn lookup line")

loop_indent = out[hit][:len(out[hit]) - len(out[hit].lstrip())]

# Add counters around logic_fn and logic result
inject_block = [
    loop_indent + "c_setups_checked += 1",
    out[hit],  # original logic_fn line
    loop_indent + "if not logic_fn:",
    loop_indent + "    c_missing_logic += 1",
    loop_indent + "    continue",
]

# Replace the single line with the block
out = out[:hit] + inject_block + out[hit+1:]

# Patch where s_side, s_reason = logic_fn(candles)
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

# Count matched additions: we anchor on matched.append(
needle3 = "matched.append({"
hit3 = None
for i,l in enumerate(out):
    if l.strip() == needle3.strip():
        hit3 = i
        break
if hit3 is None:
    # fallback: look for 'matched.append('
    for i,l in enumerate(out):
        if "matched.append(" in l:
            hit3 = i
            break

if hit3 is None:
    raise SystemExit("PATCH FAILED: could not find matched.append anchor")

app_indent = out[hit3][:len(out[hit3]) - len(out[hit3].lstrip())]
out = out[:hit3] + [app_indent + "c_matched_added += 1"] + out[hit3:]

# Count empty setup_types: anchor where setup_types is assigned
needle4 = "setup_types = _get_strat_attr(raw_obj, \"setup_types\", []) or []"
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

# Print counters once per loop, right after per-(symbol,tf) loop ends.
# Anchor: after 'for (symbol, tf), strat_list in universe.items():' ends, we insert before heartbeat/loop sleep.
# We use a stable anchor: 'if total_signals_this_loop:' (or similar). We'll search for 'loop_sec =' block near end.
print_anchor = "        # Sleep until next poll"
pa = None
for i,l in enumerate(out):
    if l.strip() == print_anchor.strip():
        pa = i
        break
if pa is None:
    # fallback: search for 'Sleep until next poll' without indentation match
    for i,l in enumerate(out):
        if "Sleep until next poll" in l:
            pa = i
            break
if pa is None:
    raise SystemExit("PATCH FAILED: could not find sleep anchor to print counters")

pr_indent = out[pa][:len(out[pa]) - len(out[pa].lstrip())]
print_lines = [
    pr_indent + "print(f\"[DBG] setups_checked={c_setups_checked} missing_logic={c_missing_logic} logic_none={c_logic_none} setup_types_empty={c_setup_types_empty} matched_added={c_matched_added}\")"
]
out = out[:pa] + print_lines + out[pa:]

p.write_text("\n".join(out) + "\n", encoding="utf-8", newline="\n")
print("OK: patched signal_engine.py with STRAT_COUNTERS_V1 debug counters")
