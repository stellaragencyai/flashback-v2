from pathlib import Path
import datetime, re, sys

p = Path(r"C:\Flashback\config\subaccounts.yaml")
s = p.read_text(encoding="utf-8", errors="ignore")

bak = p.with_name(p.name + ".bak_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
bak.write_text(s, encoding="utf-8")

def repl(block_name: str, new_block: str):
    global s
    pat = re.compile(rf"(?ms)^{re.escape(block_name)}:\n(?:(?!^[A-Za-z0-9_]+:).)*", re.MULTILINE)
    m = pat.search(s)
    if not m:
        raise SystemExit(f"MISSING BLOCK: {block_name}")
    s = pat.sub(new_block, s)

repl("flashback05", """flashback05:
  sub_uid: 524637467
  role: hft_market_maker
  strategy_name: Sub5_HFT_MM
  enabled: true
  telegram_channel: sub5
  risk_profile: hft_mm_v1
  enable_tp_sl: true
  enable_journal: true
  enable_ai_stack: true
  ai_profile: hft_mm_v1
  automation_mode: LEARN_DRY
""")

repl("flashback08", """flashback08:
  sub_uid: 524650929
  role: sniper_sol
  strategy_name: Sniper08_SOL
  enabled: true
  telegram_channel: sub8
  risk_profile: sniper_v1
  enable_tp_sl: true
  enable_journal: true
  enable_ai_stack: true
  ai_profile: sniper_v1
  automation_mode: LEARN_DRY
  pinned_symbol: SOLUSDT
""")

repl("flashback09", """flashback09:
  sub_uid: 524693375
  role: sniper_fartcoin
  strategy_name: Sniper09_FARTCOIN
  enabled: true
  telegram_channel: sub9
  risk_profile: sniper_v1
  enable_tp_sl: true
  enable_journal: true
  enable_ai_stack: true
  ai_profile: sniper_v1
  automation_mode: LEARN_DRY
  pinned_symbol: FARTCOINUSDT
""")

p.write_text(s, encoding="utf-8")
print("OK: patched subaccounts.yaml (flashback05/08/09)")
