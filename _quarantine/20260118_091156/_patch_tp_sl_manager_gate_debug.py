import pathlib, re

p = pathlib.Path(r"app\bots\tp_sl_manager.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# Find the existing GATE_BLOCKED print line and replace it with a richer one
old = r'print\(\s*f"\[tp_sl_manager\]\s*🚫\s*GATE_BLOCKED symbol=\{symbol\} trade_id=\{trade_id\} reason=\{reason\}",\s*flush=True,\s*\)'

new = (
    'meta = verdict.get("meta") if isinstance(verdict, dict) else None\\n'
    '        tid_in = str(trade_id)\\n'
    '        tid_norm = None\\n'
    '        try:\\n'
    '            tid_norm = meta.get("trade_id_norm") if isinstance(meta, dict) else None\\n'
    '        except Exception:\\n'
    '            tid_norm = None\\n'
    '        print(\\n'
    '            f"[tp_sl_manager] 🚫 GATE_BLOCKED symbol={symbol} trade_id={trade_id} reason={reason} tid_in={tid_in} tid_norm={tid_norm} decision_code={verdict.get(\\\"decision_code\\\")} allow={verdict.get(\\\"allow\\\")} ",\\n'
    '            flush=True,\\n'
    '        )'
)

if not re.search(old, s, flags=re.MULTILINE):
    raise SystemExit("PATCH_FAILED: could not find the exact GATE_BLOCKED print() line to replace")

s2 = re.sub(old, new, s, count=1, flags=re.MULTILINE)
p.write_text(s2, encoding="utf-8")
print("PATCH_OK:", str(p))
