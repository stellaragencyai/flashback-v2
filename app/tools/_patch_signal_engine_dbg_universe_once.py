from pathlib import Path

p = Path(r"app\bots\signal_engine.py")
lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()

# Refuse if already applied
if any("DBG_UNIVERSE_ONCE_V1" in l for l in lines):
    raise SystemExit("REFUSE: DBG_UNIVERSE_ONCE_V1 already applied")

# Insert right after: matched: List[Dict[str, Any]] = []
anchor = "matched: List[Dict[str, Any]] = []"
idx = None
for i, l in enumerate(lines):
    if l.strip() == anchor.strip():
        idx = i
        break
if idx is None:
    raise SystemExit("PATCH FAILED: could not find matched list anchor")

indent = lines[idx][:len(lines[idx]) - len(lines[idx].lstrip())]

inject = [
    indent + "# DBG_UNIVERSE_ONCE_V1",
    indent + "if os.getenv('SIG_DBG_UNIVERSE','0') == '1':",
    indent + "    if not hasattr(main, '_dbg_universe_once'):",
    indent + "        main._dbg_universe_once = True",
    indent + "        try:",
    indent + "            print(f'[DBG] universe key={symbol} tf={tf_label} strat_list_len={len(strat_list) if strat_list else 0}', flush=True)",
    indent + "            if strat_list:",
    indent + "                for j, strat in enumerate(strat_list[:3], start=1):",
    indent + "                    raw_obj = strat.get('raw')",
    indent + "                    print(f'[DBG] strat#{j} keys={sorted(list(strat.keys()))}', flush=True)",
    indent + "                    print(f'[DBG] strat#{j} raw_type={type(raw_obj).__name__} raw_keys={(sorted(list(raw_obj.keys())) if isinstance(raw_obj, dict) else None)}', flush=True)",
    indent + "                    st = _get_strat_attr(raw_obj, 'setup_types', []) or []",
    indent + "                    print(f'[DBG] strat#{j} setup_types_type={type(st).__name__} setup_types={st}', flush=True)",
    indent + "        except Exception as e:",
    indent + "            print(f'[DBG] universe debug failed: {type(e).__name__}: {e}', flush=True)",
]

out = lines[:idx+1] + inject + lines[idx+1:]
p.write_text("\n".join(out) + "\n", encoding="utf-8", newline="\n")
print("OK: patched signal_engine.py (DBG_UNIVERSE_ONCE_V1)")
