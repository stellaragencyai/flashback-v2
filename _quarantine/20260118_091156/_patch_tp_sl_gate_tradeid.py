from __future__ import annotations
from pathlib import Path

path = Path(r"app\bots\tp_sl_manager.py")
txt = path.read_text(encoding="utf-8", errors="ignore")

needle = "        verdict = enforce_decision(str(trade_id))"
if needle not in txt:
    raise SystemExit("PATCH_FAILED: could not find enforce_decision call site in tp_sl_manager.py")

replacement = """        # Phase 4: normalize dashed trade_id -> colon form and scope by account_label
        lab = os.getenv("ACCOUNT_LABEL", "").strip()
        tid_in = str(trade_id)

        # Normalize: <label>-<symbol>-<hex>  -> <label>:<hex>
        # Keeps other formats untouched.
        tid_norm = tid_in
        try:
            if lab and tid_in.startswith(lab + "-"):
                parts = tid_in.split("-")
                if len(parts) >= 3:
                    hx = parts[-1]
                    if hx and all(c in "0123456789abcdefABCDEF" for c in hx) and len(hx) >= 8:
                        tid_norm = f"{lab}:{hx}"
        except Exception:
            tid_norm = tid_in

        verdict = enforce_decision(tid_norm, account_label=(lab or None))"""

txt2 = txt.replace(needle, replacement)

path.write_text(txt2, encoding="utf-8", newline="\n")
print("PATCH_OK: tp_sl_manager enforce_decision now normalizes trade_id and passes account_label")
