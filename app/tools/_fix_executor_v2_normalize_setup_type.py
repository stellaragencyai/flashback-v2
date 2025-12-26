#!/usr/bin/env python3
from __future__ import annotations
from pathlib import Path
import re

TARGET = Path(r"app\bots\executor_v2.py")

def main() -> int:
    if not TARGET.exists():
        print("FAIL: executor_v2.py not found")
        return 1

    text = TARGET.read_text(encoding="utf-8", errors="ignore")

    pat = re.compile(
        r"def _normalize_setup_type\(raw: Any\)[\s\S]*?(?=^def |\Z)",
        re.MULTILINE
    )
    m = pat.search(text)
    if not m:
        print("FAIL: _normalize_setup_type not found")
        return 1

    fixed_fn = '''
def _normalize_setup_type(raw: Any) -> Tuple[str, str]:
    """
    Accept both:
      - coarse canonical types (breakout/pullback/etc.)
      - rich signal labels like 'ma_long_trend_pullback:close_up_above_ma'
    Returns:
      (setup_type_family, reason)
    """
    s0 = _clean_token(raw)
    if not s0:
        return "unknown", "empty"

    # Heuristic normalization for verbose setup strings
    if "trend_pullback" in s0:
        return "pullback", "substring:trend_pullback"
    if "range_fade" in s0 or "intraday_range_fade" in s0:
        return "range_fade", "substring:range_fade"
    if "mean_reversion" in s0:
        return "mean_reversion", "substring:mean_reversion"
    if "breakout_pullback" in s0:
        return "breakout_pullback", "substring:breakout_pullback"
    if "breakout" in s0:
        return "breakout", "substring:breakout"

    # Fast-path: exact aliases
    if s0 in _SETUP_TYPE_ALIASES:
        return _SETUP_TYPE_ALIASES[s0], "alias"

    # Rich-label parsing fallback
    for k, v in _SETUP_TYPE_ALIASES.items():
        if s0.startswith(k):
            return v, "prefix"

    return "unknown", "unrecognized"
'''.strip("\n")

    text2 = text[:m.start()] + fixed_fn + "\n\n" + text[m.end():]
    TARGET.write_text(text2, encoding="utf-8")

    print("OK: fixed _normalize_setup_type (indentation + logic hardened)")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
