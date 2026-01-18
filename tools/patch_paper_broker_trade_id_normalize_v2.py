from __future__ import annotations

import re
from pathlib import Path

P = Path("app/sim/paper_broker.py")
s = P.read_text(encoding="utf-8")

if "_normalize_trade_ids_in_state" in s:
    print("SKIP: paper_broker already has trade_id normalization")
    raise SystemExit(0)

# -----------------------
# 1) Inject helpers before _generate_trade_id
# -----------------------
marker = r"(\n\s+def _generate_trade_id\(self, symbol: str\) -> str:\n)"
m = re.search(marker, s)
if not m:
    raise SystemExit("FAIL: could not find _generate_trade_id marker")

insert = """
    def _normalize_trade_id(self, *, symbol: str, trade_id: str) -> str:
        \"\"\"Canonical PAPER trade_id:
        {account}-{symbol}-{suffix}
        Legacy:
        {account}:{suffix} -> upgraded
        \"\"\"
        tid = (trade_id or "").strip()
        if not tid:
            return tid

        acct = str(self._state.account_label)

        if tid.startswith(acct + "-"):
            return tid

        legacy = acct + ":"
        if tid.startswith(legacy):
            suffix = tid[len(legacy):].strip()
            if suffix:
                return f"{acct}-{symbol}-{suffix}"
            return f"{acct}-{symbol}"

        return tid


    def _normalize_trade_ids_in_state(self) -> None:
        changed = 0
        positions = []

        try:
            positions.extend(list(self._state.open_positions))
        except Exception:
            pass

        try:
            positions.extend(list(self._state.closed_trades))
        except Exception:
            pass

        for pos in positions:
            try:
                old = str(getattr(pos, "trade_id", "") or "")
                sym = str(getattr(pos, "symbol", "") or "")
                if not old or not sym:
                    continue

                new = self._normalize_trade_id(symbol=sym, trade_id=old)
                if new != old:
                    if hasattr(pos, "source_trade_id") and not getattr(pos, "source_trade_id", None):
                        pos.source_trade_id = old
                    if hasattr(pos, "client_trade_id") and not getattr(pos, "client_trade_id", None):
                        pos.client_trade_id = new
                    pos.trade_id = new
                    changed += 1
            except Exception:
                continue

        if changed:
            try:
                self._save()
            except Exception:
                pass
"""

s = re.sub(marker, "\n" + insert + r"\1", s, count=1)

# -----------------------
# 2) Normalize trade_id at creation time
# -----------------------
pat = r"(trade_id_final\s*=\s*trade_id\s*or\s*self\._generate_trade_id\(symbol\))"
s = re.sub(
    pat,
    r"\1\n        trade_id_final = self._normalize_trade_id(symbol=symbol, trade_id=trade_id_final)",
    s,
    count=1,
)

# -----------------------
# 3) Run migration inside __init__
# -----------------------
init_pat = r"(self\._state_path\s*=\s*state_path\s*\n)"
s = re.sub(
    init_pat,
    r"\1        try:\n            self._normalize_trade_ids_in_state()\n        except Exception:\n            pass\n",
    s,
    count=1,
)

P.write_text(s, encoding="utf-8", newline="\n")
print("OK: patched paper_broker trade_id normalization + migration (via __init__)")
