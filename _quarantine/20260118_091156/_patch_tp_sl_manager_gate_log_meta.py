import pathlib, re

p = pathlib.Path(r"app\bots\tp_sl_manager.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# Replace the exact GATE_BLOCKED print with a richer one that includes enforcer meta
pattern = r'print\(\s*f"\[tp_sl_manager\]\s*🚫\s*GATE_BLOCKED\s*symbol=\{symbol\}\s*trade_id=\{trade_id\}\s*reason=\{reason\}"\s*,\s*flush=True\s*,\s*\)'
repl = (
    'm = verdict.get("meta") or {}\\n'
    '        print(\\n'
    '            f"[tp_sl_manager] 🚫 GATE_BLOCKED symbol={symbol} trade_id={trade_id} reason={reason} '
    'tid_norm={m.get(\\\"trade_id_norm\\\")} src={m.get(\\\"source\\\")} count={m.get(\\\"count\\\")}",\\n'
    '            flush=True,\\n'
    '        )'
)

if not re.search(pattern, s, flags=re.DOTALL):
    raise SystemExit("PATCH_FAILED: could not find the GATE_BLOCKED print() block to replace")

s2 = re.sub(pattern, repl, s, count=1, flags=re.DOTALL)
p.write_text(s2, encoding="utf-8")
print("PATCH_OK:", str(p))
