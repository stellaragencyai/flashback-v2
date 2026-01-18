import pathlib, re

p = pathlib.Path(r"app\bots\tp_sl_manager.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# Find the exact block that reads ACCOUNT_LABEL and replace with a safer version
old = r'lab = os.getenv\("ACCOUNT_LABEL", ""\)\.strip\(\)'
new = (
    'lab = os.getenv("ACCOUNT_LABEL", "").strip()\\n'
    '        if not lab:\\n'
    '            # Fallback: derive label from trade_id itself\\n'
    '            if ":" in tid_in:\\n'
    '                lab = tid_in.split(":", 1)[0].strip()\\n'
    '            elif "-" in tid_in:\\n'
    '                lab = tid_in.split("-", 1)[0].strip()'
)

if not re.search(old, s):
    raise SystemExit("PATCH_FAILED: could not find ACCOUNT_LABEL read line in tp_sl_manager")

s2 = re.sub(old, new, s, count=1)
p.write_text(s2, encoding="utf-8")
print("PATCH_OK:", str(p))
