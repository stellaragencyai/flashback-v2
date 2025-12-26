from pathlib import Path
import re

p = Path(r"app\bots\signal_engine.py")
s = p.read_text(encoding="utf-8", errors="ignore")

if "SETUP_LOGIC" in s:
    raise SystemExit("REFUSE: SETUP_LOGIC already exists in signal_engine.py")

# Insert right after compute_simple_signal definition (after its final return)
anchor = r"def compute_simple_signal\(candles: List\[Dict\[str, Any\]\]\) -> Tuple\[Optional\[str\], Dict\[str, Any\]\]:"
m = re.search(anchor, s)
if not m:
    raise SystemExit("PATCH FAILED: could not find compute_simple_signal() definition")

# Find end of compute_simple_signal by locating the NEXT 'def ' after it
tail = s[m.start():]
m2 = re.search(r"\n\ndef\s+", tail)
if not m2:
    raise SystemExit("PATCH FAILED: could not find end of compute_simple_signal() (next def)")

insert_at = m.start() + m2.start()

block = r'''

# ------------------------------------------------------------
# Strategy setup logic map (Phase 8: minimal working v1)
# NOTE: This is intentionally simple: it reuses compute_simple_signal()
# to unblock strategy-only emission. Replace each handler with real setup logic later.
# ------------------------------------------------------------
def _setup_from_simple(setup_name: str):
    def _fn(candles):
        side, dbg = compute_simple_signal(candles)
        if not side:
            return None, ""
        # dbg["reason"] already exists (e.g., close_up_above_ma)
        return side, dbg.get("reason", setup_name)
    return _fn

SETUP_LOGIC = {
    # YAML setup_types -> handler
    "breakout_high": _setup_from_simple("breakout_high"),
    "breakout_range": _setup_from_simple("breakout_range"),
    "dump_fade_reversion": _setup_from_simple("dump_fade_reversion"),
    "ema_trend_follow": _setup_from_simple("ema_trend_follow"),
    "failed_breakout_fade": _setup_from_simple("failed_breakout_fade"),
    "intraday_range_fade": _setup_from_simple("intraday_range_fade"),
    "mm_reversion_micro": _setup_from_simple("mm_reversion_micro"),
    "mm_spread_capture": _setup_from_simple("mm_spread_capture"),
    "pump_chase_momo": _setup_from_simple("pump_chase_momo"),
    "scalp_liquidity_sweep": _setup_from_simple("scalp_liquidity_sweep"),
    "scalp_reversal_snapback": _setup_from_simple("scalp_reversal_snapback"),
    "scalp_trend_continuation": _setup_from_simple("scalp_trend_continuation"),
    "squeeze_release": _setup_from_simple("squeeze_release"),
    "swing_reversion_channel": _setup_from_simple("swing_reversion_channel"),
    "swing_reversion_extreme": _setup_from_simple("swing_reversion_extreme"),
    "swing_trend_continuation": _setup_from_simple("swing_trend_continuation"),
    "swing_trend_follow": _setup_from_simple("swing_trend_follow"),
    "trend_breakout_retest": _setup_from_simple("trend_breakout_retest"),
    "trend_pullback": _setup_from_simple("trend_pullback"),
}
'''

s2 = s[:insert_at] + block + s[insert_at:]
p.write_text(s2, encoding="utf-8", newline="\n")
print("OK: inserted SETUP_LOGIC mapping (minimal v1) after compute_simple_signal()")
