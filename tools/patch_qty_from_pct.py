from pathlib import Path
import re

p = Path(r"app\core\flashback_common.py")
s = p.read_text(encoding="utf-8", errors="ignore")

if "def qty_from_pct" in s:
    print("SKIP: qty_from_pct already exists")
    raise SystemExit(0)

anchor = r"def last_price_ws_first\(symbol: str\) -> Decimal:[\s\S]*?return last_price\(symbol\)\n"
m = re.search(anchor, s)
if not m:
    print("PATCH_FAIL: could not find last_price_ws_first anchor")
    raise SystemExit(1)

insert = m.group(0) + r'''

def qty_from_pct(symbol: str, equity_usdt: Decimal, pct_notional: Decimal) -> Decimal:
    """
    Compute order qty from a notional % of equity.

    - Uses WS-first price if available.
    - Quantizes down to qty step.
    - Enforces a minimal notional check (best-effort).
    - Returns Decimal("0") if sizing cannot be computed safely.
    """
    sym = str(symbol or "").strip().upper()
    if not sym:
        raise ValueError("symbol required")

    try:
        eq = Decimal(str(equity_usdt))
    except Exception:
        raise ValueError("equity_usdt must be Decimal-like")
    if eq <= 0:
        return Decimal("0")

    try:
        pct = Decimal(str(pct_notional))
    except Exception:
        raise ValueError("pct_notional must be Decimal-like")
    if pct <= 0:
        return Decimal("0")

    notional = (eq * pct) / Decimal("100")
    if notional <= 0:
        return Decimal("0")

    px = Decimal("0")
    try:
        px = last_price_ws_first(sym)
    except Exception:
        px = Decimal("0")

    if px <= 0:
        try:
            px = last_price(sym)
        except Exception:
            px = Decimal("0")

    if px <= 0:
        return Decimal("0")

    tick, step, min_notional = get_ticks(sym)

    raw_qty = notional / px
    qty = qdown(raw_qty, step)

    try:
        if qty * px < min_notional:
            return Decimal("0")
    except Exception:
        pass

    if qty <= 0:
        return Decimal("0")
    return qty
'''

s2 = s[:m.start()] + insert + s[m.end():]
p.write_text(s2, encoding="utf-8")
print("PATCH_OK: qty_from_pct inserted into flashback_common.py")
