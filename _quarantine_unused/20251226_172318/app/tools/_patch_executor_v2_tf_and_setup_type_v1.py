from __future__ import annotations

from pathlib import Path
import re

p = Path(r"app\bots\executor_v2.py")
s = p.read_text(encoding="utf-8", errors="ignore")

# --- 1) Normalize 60m -> 1h inside _normalize_timeframe ---
# Find the block that handles suffix "m/h/d" conversion and add a rule for 60m -> 1h.
if "if s[-1] == \"m\":" not in s:
    raise SystemExit("FAIL: expected _normalize_timeframe() suffix block not found (pattern changed).")

# Insert a 60m -> 1h mapping right after we compute tf for minutes.
# We'll patch the first occurrence inside _normalize_timeframe.
pat_tf = re.compile(r'(?ms)(if s\[-1\] == "m":\s*\n\s*tf = f"\{n\}m"\s*\n)')
m = pat_tf.search(s)
if not m:
    raise SystemExit("FAIL: could not locate minutes suffix normalization block.")

inject = m.group(1) + "        if tf == \"60m\":\n            tf = \"1h\"  # canonicalize 60m -> 1h\n"
s = s[:m.start(1)] + inject + s[m.end(1):]

# --- 2) Expand _normalize_setup_type to understand rich labels ---
# We’ll replace the whole function body with a smarter version if we can find the function def.
pat_st = re.compile(r'(?ms)^def _normalize_setup_type\(raw: Any\) -> Tuple\[str, str\]:\s*\n(.*?)^\n', re.MULTILINE)
m2 = pat_st.search(s)
if not m2:
    raise SystemExit("FAIL: could not find _normalize_setup_type() to patch.")

new_fn = r'''def _normalize_setup_type(raw: Any) -> Tuple[str, str]:
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

    # Fast-path: exact aliases already known
    if s0 in _SETUP_TYPE_ALIASES:
        return _SETUP_TYPE_ALIASES[s0], "alias"

    # Rich-label parsing (common in your signal engine)
    # Examples:
    #   ma_long_trend_pullback:close_up_above_ma
    #   ma_short_trend_pullback:close_down_below_ma
    #   intraday_range_fade:close_down_below_ma
    s = str(raw).strip().lower()

    # Strip the prefix direction marker if present
    s_family = s.split(":", 1)[0].strip()

    # Map rich families to canonical families (keep family level for AI buckets)
    if "trend_pullback" in s_family:
        return "trend_pullback", f"family:{s_family}"
    if "range_fade" in s_family:
        return "range_fade", f"family:{s_family}"
    if "breakout" in s_family:
        return "breakout", f"family:{s_family}"
    if "pullback" in s_family:
        return "pullback", f"family:{s_family}"

    # Coarse canonical types allowed
    if s0 in ("breakout", "pullback", "trend_continuation", "range_fade", "mean_reversion", "scalp", "breakout_pullback", "trend_pullback"):
        return s0, "canonical"

    return "unknown", f"unrecognized:{s0}"'''
s = s[:m2.start()] + new_fn + s[m2.end():]

# --- 3) Ensure features_payload carries the raw setup label too (for high-res bucketing later) ---
# Add a field near features_payload creation: "setup_label_raw": setup_type_raw
if '"setup_type": setup_type_val,' in s and '"setup_type": setup_type_val,' in s:
    s = s.replace(
        '"setup_type": setup_type_val,',
        '"setup_type": setup_type_val,\n        "setup_label_raw": str(setup_type_raw) if setup_type_raw is not None else None,'
    )
else:
    raise SystemExit("FAIL: could not locate features_payload setup_type assignment to extend.")

p.write_text(s, encoding="utf-8")
print("OK: patched executor_v2.py (60m->1h, rich setup_type normalization, preserved setup_label_raw)")
