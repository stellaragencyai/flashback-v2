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

    changed = 0

    # ------------------------------------------------------------------
    # 1) Ensure _is_live_like exists (used by label gates)
    # ------------------------------------------------------------------
    if "_is_live_like" not in text:
        # Insert after _normalize_setup_type definition if possible, else near top after helpers.
        insert_pat = re.compile(r"(def _normalize_setup_type\(raw: Any\)[\s\S]*?\n)(?=def |\Z)", re.MULTILINE)
        m = insert_pat.search(text)
        helper = """

def _is_live_like(trade_mode: str) -> bool:
    m = str(trade_mode or "").upper().strip()
    return m in ("LIVE_CANARY", "LIVE_FULL")
"""
        if m:
            # place helper right after normalize_setup_type block
            insert_at = m.end(1)
            text = text[:insert_at] + helper + text[insert_at:]
            changed += 1
        else:
            # fallback: insert after _normalize_timeframe definition
            m2 = re.search(r"(def _normalize_timeframe\(.*?\n[\s\S]*?\n)(?=def |\Z)", text, flags=re.MULTILINE)
            if m2:
                insert_at = m2.end(1)
                text = text[:insert_at] + helper + text[insert_at:]
                changed += 1
            else:
                # worst-case: prepend near top after imports
                m3 = re.search(r"^from __future__ import annotations\s*\n", text, flags=re.MULTILINE)
                if m3:
                    insert_at = m3.end(0)
                    text = text[:insert_at] + helper + text[insert_at:]
                    changed += 1
                else:
                    text = helper + "\n" + text
                    changed += 1

    # ------------------------------------------------------------------
    # 2) Patch _normalize_setup_type heuristics to cover real labels
    # ------------------------------------------------------------------
    pat_fn = re.compile(
        r"def _normalize_setup_type\(raw: Any\) -> Tuple\[str, str\]:[\s\S]*?(?=^def |\Z)",
        re.MULTILINE
    )
    m = pat_fn.search(text)
    if not m:
        print("FAIL: _normalize_setup_type not found")
        TARGET.write_text(text, encoding="utf-8")
        return 1

    fn = m.group(0)

    # Replace function body with hardened version (keeps it simple + deterministic)
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

    # Heuristics for verbose setup strings produced by Signal Engine
    # NOTE: _clean_token normalizes ':' -> '_' so substring checks work.
    if "trend_pullback" in s0:
        return "pullback", "substring:trend_pullback"

    if "scalp" in s0 or "liquidity_sweep" in s0:
        return "scalp", "substring:scalp_or_liquidity_sweep"

    if "pump_chase" in s0 or "momo" in s0 or "momentum" in s0:
        return "breakout", "substring:pump_chase_momo"

    if "range_fade" in s0 or "intraday_range_fade" in s0:
        return "range_fade", "substring:range_fade"

    if "mean_reversion" in s0:
        return "mean_reversion", "substring:mean_reversion"

    if "breakout_pullback" in s0:
        return "breakout_pullback", "substring:breakout_pullback"

    if "breakout" in s0:
        return "breakout", "substring:breakout"

    # Exact aliases
    if s0 in _SETUP_TYPE_ALIASES:
        return _SETUP_TYPE_ALIASES[s0], "alias"

    # Prefix fallback (covers 'ma_long_breakout_*' etc)
    for k, v in _SETUP_TYPE_ALIASES.items():
        if s0.startswith(k):
            return v, "prefix"

    return "unknown", "unrecognized"
'''.strip("\n")

    text = text[:m.start()] + fixed_fn + "\n\n" + text[m.end():]
    changed += 1

    TARGET.write_text(text, encoding="utf-8")

    print(f"OK: patched executor_v2.py (changes={changed})")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
