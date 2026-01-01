#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Robust patch for executor_v2.py label normalization:
- Add 60m to _CANON_TF if missing
- Ensure _clean_token replaces ':' with '_'
- Inject heuristic setup_type mapping into _normalize_setup_type()
  right before the final unknown return, regardless of formatting.
"""
from __future__ import annotations

from pathlib import Path
import re

TARGET = Path(r"app\bots\executor_v2.py")

def fail(msg: str) -> int:
    print("FAIL:", msg)
    return 1

def patch_canon_tf(text: str) -> tuple[str, bool]:
    # Locate _CANON_TF = { ... } including newlines (non-greedy)
    m = re.search(r"(_CANON_TF\s*=\s*\{)(.*?)(\n\})", text, flags=re.DOTALL)
    if not m:
        return text, False
    head, body, tail = m.group(1), m.group(2), m.group(3)

    if re.search(r"['\"]60m['\"]", body):
        return text, True

    # Insert after 30m if present, else near start of body
    if re.search(r"['\"]30m['\"]\s*,", body):
        body2 = re.sub(r"(['\"]30m['\"]\s*,)", r"\1\n    '60m',", body, count=1)
    else:
        body2 = "\n    '60m'," + (body if body.startswith("\n") else ("\n" + body))

    out = text[:m.start()] + head + body2 + tail + text[m.end():]
    return out, True

def patch_clean_token(text: str) -> tuple[str, bool]:
    # Find the line that does s = s.replace(" ", "_").replace("-", "_") (quote style may vary)
    # and ensure it also does replace(":", "_")
    pat = re.compile(r"(s\s*=\s*s\.replace\(\s*['\"]\s+['\"]\s*,\s*['\"]_['\"]\s*\)\.replace\(\s*['\"]-['\"]\s*,\s*['\"]_['\"]\s*\))")
    m = pat.search(text)
    if not m:
        return text, False

    line = m.group(1)
    if ".replace(':', '_')" in line or ".replace(\":\", \"_\")" in line or ".replace(':', \"_\")" in line or ".replace(\":\", '_')" in line:
        return text, True

    # Add : replacement at end (use double quotes for consistency)
    line2 = line + '.replace(":", "_")'
    out = text[:m.start(1)] + line2 + text[m.end(1):]
    return out, True

def inject_setup_heuristics(text: str) -> tuple[str, bool]:
    # Extract def _normalize_setup_type(...) block up to next def at column 0
    m = re.search(r"^def\s+_normalize_setup_type\s*\(.*?\):\s*\n(.*?)(?=^def\s|\Z)", text, flags=re.DOTALL | re.MULTILINE)
    if not m:
        return text, False

    block = m.group(0)

    # If already injected, no-op
    if "substring:trend_pullback" in block:
        return text, True

    # Find the last return unknown line inside this function
    # supports both single and double quotes
    r = re.search(r"^\s*return\s+['\"]unknown['\"].*$", block, flags=re.MULTILINE)
    if not r:
        return text, False

    heur = (
        "\n"
        "    # Heuristic normalization for verbose setup strings produced by Signal Engine\n"
        "    # Example: ma_long_trend_pullback:close_up_above_ma\n"
        "    # We also normalize ':' -> '_' in _clean_token so substring checks work.\n"
        "    if 'trend_pullback' in s:\n"
        "        return 'pullback', 'substring:trend_pullback'\n"
        "    if 'range_fade' in s or 'intraday_range_fade' in s:\n"
        "        return 'range_fade', 'substring:range_fade'\n"
        "    if 'mean_reversion' in s:\n"
        "        return 'mean_reversion', 'substring:mean_reversion'\n"
        "    if 'breakout_pullback' in s:\n"
        "        return 'breakout_pullback', 'substring:breakout_pullback'\n"
        "    if 'breakout' in s:\n"
        "        return 'breakout', 'substring:breakout'\n"
        "\n"
    )

    # Insert heuristics immediately before the first (or last) return unknown we found
    insert_at = r.start()
    block2 = block[:insert_at] + heur + block[insert_at:]

    out = text[:m.start()] + block2 + text[m.end():]
    return out, True

def main() -> int:
    if not TARGET.exists():
        return fail(f"missing {TARGET}")

    text = TARGET.read_text(encoding="utf-8", errors="ignore")
    changed_any = False

    text2, ok_tf = patch_canon_tf(text)
    if ok_tf:
        text = text2
        changed_any = True
    else:
        print("WARN: could not patch _CANON_TF (pattern miss)")

    text2, ok_clean = patch_clean_token(text)
    if ok_clean:
        text = text2
        changed_any = True
    else:
        print("WARN: could not patch _clean_token replace chain (pattern miss)")

    text2, ok_setup = inject_setup_heuristics(text)
    if ok_setup:
        text = text2
        changed_any = True
    else:
        return fail("could not inject heuristics into _normalize_setup_type (pattern miss)")

    if not changed_any:
        print("OK: nothing changed (already patched?)")
        return 0

    TARGET.write_text(text, encoding="utf-8")
    print("OK: patched executor_v2.py (60m accepted; ':' cleaned; setup_type heuristics injected)")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
