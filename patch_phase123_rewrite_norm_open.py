from pathlib import Path
import re

p = Path("app/core/ai_action_router.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# Replace the entire _normalize_open_action(...) function with a clean implementation.
start = re.search(r"^def _normalize_open_action\(", s, flags=re.M)
if not start:
    raise SystemExit("PATCH_FAIL: def _normalize_open_action( not found")

# Find next top-level def after _normalize_open_action
rest = s[start.start():]
nxt = re.search(r"^def\s+", rest[1:], flags=re.M)  # [1:] avoids matching itself at pos 0
if not nxt:
    raise SystemExit("PATCH_FAIL: could not find next def after _normalize_open_action")

func_start = start.start()
func_end = start.start() + 1 + nxt.start()

replacement = r'''def _normalize_open_action(payload: Dict[str, Any], profile: Dict[str, Any]) -> Dict[str, Any]:
    symbol = _normalize_symbol(payload.get("symbol"))

    # Side: accept LONG/SHORT, also accept buy/sell/long/short
    raw_side = payload.get("side")
    side_s = str(raw_side or "").strip().lower()
    if side_s in ("buy", "long"):
        side = "LONG"
    elif side_s in ("sell", "short"):
        side = "SHORT"
    else:
        side = _normalize_side(raw_side)

    # risk_pct_notional may be missing or explicitly null in JSONL
    rpn_raw = payload.get("risk_pct_notional", "__MISSING__")
    if rpn_raw in ("__MISSING__", None):
        # Compute from size_fraction * profile.max_notional_pct if possible
        max_pct = profile.get("max_notional_pct")
        if not isinstance(max_pct, Decimal):
            try:
                max_pct = Decimal(str(max_pct))
            except Exception:
                max_pct = Decimal("40")  # safe fallback

        sf_raw = payload.get("size_fraction", None)
        if sf_raw is not None:
            try:
                sf = Decimal(str(sf_raw))
            except Exception:
                sf = Decimal("0")
            if sf <= 0:
                raise ValueError("size_fraction must be > 0 when used")
            if sf > 1:
                sf = Decimal("1")
            risk_pct_notional = max_pct * sf
        else:
            # Conservative fallback: min(max_pct, 1%)
            risk_pct_notional = max_pct if max_pct < Decimal("1.0") else Decimal("1.0")
    else:
        risk_pct_notional = _to_decimal(rpn_raw, "risk_pct_notional")

    _validate_symbol_whitelist(symbol, profile)
    _validate_notional_pct(risk_pct_notional, profile)

    # Optional spread cap
    max_spread_raw = payload.get("max_spread_bps", None)
    if max_spread_raw is None:
        max_spread_bps = None
    else:
        try:
            max_spread_bps = _to_decimal(max_spread_raw, "max_spread_bps")
            if max_spread_bps <= 0:
                max_spread_bps = None
        except Exception:
            max_spread_bps = None

    # Optional leverage override
    lev_raw = payload.get("leverage_override", None)
    if lev_raw is None:
        lev = None
    else:
        try:
            lev_int = int(lev_raw)
            lev = lev_int if lev_int > 0 else None
        except Exception:
            raise ValueError(f"Invalid leverage_override: {lev_raw!r}")

    return {
        "type": "OPEN",
        "symbol": symbol,
        "side": side,
        "risk_pct_notional": risk_pct_notional,
        "max_spread_bps": max_spread_bps,
        "leverage_override": lev,
    }

'''

s2 = s[:func_start] + replacement + s[func_end:]
p.write_text(s2, encoding="utf-8")
print("PATCH_OK: rewrote _normalize_open_action cleanly")
