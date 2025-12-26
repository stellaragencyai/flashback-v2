#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Patch executor_v2 label normalization:
- Accept 60m timeframe (prevents dropping 1h-style signals emitted as 60m)
- Normalize verbose setup_type strings like:
    ma_long_trend_pullback:close_up_above_ma
  into canonical setup types (pullback / breakout / range_fade / mean_reversion / etc.)
So ai_events_spine stops dropping them as "unknown".
"""
from __future__ import annotations

from pathlib import Path
import re

TARGET = Path(r"app\bots\executor_v2.py")

def die(msg: str, code: int = 1) -> int:
    print("FAIL:", msg)
    return code

def main() -> int:
    if not TARGET.exists():
        return die(f"missing {TARGET}")

    s = TARGET.read_text(encoding="utf-8", errors="ignore")

    # -------------------------------
    # 1) Add '60m' to _CANON_TF set
    # -------------------------------
    # Find the _CANON_TF = { ... } block and inject 60m if missing.
    m = re.search(r"_CANON_TF\s*=\s*\{.*?\n\}", s, flags=re.DOTALL)
    if not m:
        return die("could not find _CANON_TF block")

    canon_block = m.group(0)
    if "'60m'" not in canon_block and '"60m"' not in canon_block:
        # Insert right after 30m if present, else near top
        if '"30m",' in canon_block:
            canon_block2 = canon_block.replace('"30m",', '"30m",\n    "60m",')
        elif "'30m'," in canon_block:
            canon_block2 = canon_block.replace("'30m',", "'30m',\n    '60m',")
        else:
            # fallback: add near beginning
            canon_block2 = canon_block.replace("{", "{\n    '60m',", 1)
        s = s[:m.start()] + canon_block2 + s[m.end():]

    # -----------------------------------------
    # 2) Make _clean_token replace ':' with '_'
    # -----------------------------------------
    # We want: ma_long_trend_pullback:close_up_above_ma -> ma_long_trend_pullback_close_up_above_ma
    m2 = re.search(r"def\s+_clean_token\s*\(.*?\)\s*->\s*str\s*:\s*\n(.*?\n)\s*s\s*=\s*s\.replace\(\" \", \"_\"\)\.replace\(\"-\", \"_\"\)", s, flags=re.DOTALL)
    if not m2:
        # Slightly looser search: find the replace line and patch around it
        m2b = re.search(r"def\s+_clean_token\s*\(.*?\)\s*->\s*str\s*:\s*.*?\n\s*s\s*=\s*s\.replace\(\" \", \"_\"\)\.replace\(\"-\", \"_\"\)", s, flags=re.DOTALL)
        if not m2b:
            return die("could not find _clean_token replace line")

        line = m2b.group(0)
        if '.replace(":", "_")' not in line:
            line2 = line.replace('.replace(" ", "_").replace("-", "_")', '.replace(" ", "_").replace("-", "_").replace(":", "_")')
            s = s.replace(line, line2, 1)
    else:
        # if we matched earlier, still patch the replace chain
        pass

    # ---------------------------------------------------
    # 3) Expand _normalize_setup_type with substring rules
    # ---------------------------------------------------
    # Insert rules just after alias lookup section.
    # We’ll look for:
    #   if s in _SETUP_TYPE_ALIASES:
    #       return _SETUP_TYPE_ALIASES[s], "alias"
    # and inject after it.
    needle = (
        "    if s in _SETUP_TYPE_ALIASES:\n"
        "        return _SETUP_TYPE_ALIASES[s], \"alias\"\n"
    )
    if needle not in s:
        # tolerate single quotes
        needle2 = (
            "    if s in _SETUP_TYPE_ALIASES:\n"
            "        return _SETUP_TYPE_ALIASES[s], 'alias'\n"
        )
        if needle2 not in s:
            return die("could not find alias return in _normalize_setup_type")

        needle = needle2

    inject = (
        needle
        + "\n"
        + "    # Heuristic normalization for verbose setup strings (produced by signal engine)\n"
        + "    # Examples: ma_long_trend_pullback_close_up_above_ma\n"
        + "    #           ma_short_trend_pullback_close_down_below_ma\n"
        + "    #           intraday_range_fade_close_down_below_ma\n"
        + "    if \"trend_pullback\" in s:\n"
        + "        return \"pullback\", \"substring:trend_pullback\"\n"
        + "    if \"range_fade\" in s or \"intraday_range_fade\" in s:\n"
        + "        return \"range_fade\", \"substring:range_fade\"\n"
        + "    if \"mean_reversion\" in s:\n"
        + "        return \"mean_reversion\", \"substring:mean_reversion\"\n"
        + "    if \"breakout_pullback\" in s:\n"
        + "        return \"breakout_pullback\", \"substring:breakout_pullback\"\n"
        + "    if \"breakout\" in s:\n"
        + "        return \"breakout\", \"substring:breakout\"\n"
    )

    if "substring:trend_pullback" not in s:
        s = s.replace(needle, inject, 1)

    TARGET.write_text(s, encoding="utf-8")
    print("OK: patched executor_v2.py (60m accepted; setup_type heuristics added; ':' token cleaned)")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
