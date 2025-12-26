from pathlib import Path
import re

p = Path(r"app\bots\signal_engine.py")
s = p.read_text(encoding="utf-8", errors="ignore")

if "REPLACED_GET_STRAT_ATTR_V2" in s:
    raise SystemExit("REFUSE: patch already applied (REPLACED_GET_STRAT_ATTR_V2 found)")

# Replace the entire _get_strat_attr function block up to the next top-level def
pattern = re.compile(r"(?ms)^def _get_strat_attr\(.*?\n(?=def\s)", re.M | re.S)
m = pattern.search(s)
if not m:
    raise SystemExit("PATCH FAILED: could not find _get_strat_attr block to replace")

new_fn = r'''def _get_strat_attr(obj, key: str, default=None):
    """
    Robust attribute extractor for strategy config.
    Supports:
      - dict objects
      - pydantic/dataclass-style objects via getattr
      - Strategy objects that store the real payload in obj.raw (dict)
      - model_dump()/dict() fallbacks
    """
    # REPLACED_GET_STRAT_ATTR_V2
    if obj is None:
        return default

    # 1) dict
    if isinstance(obj, dict):
        v = obj.get(key, default)
        return default if v is None else v

    # 2) direct attribute
    try:
        if hasattr(obj, key):
            v = getattr(obj, key)
            if v is not None:
                return v
    except Exception:
        pass

    # 3) unwrap obj.raw dict (your Strategy case)
    try:
        raw = getattr(obj, "raw", None)
        if isinstance(raw, dict):
            v = raw.get(key, None)
            if v is not None:
                return v
    except Exception:
        pass

    # 4) model_dump()/dict() fallback
    try:
        if hasattr(obj, "model_dump"):
            d = obj.model_dump()
            if isinstance(d, dict):
                v = d.get(key, None)
                if v is not None:
                    return v
        if hasattr(obj, "dict"):
            d = obj.dict()
            if isinstance(d, dict):
                v = d.get(key, None)
                if v is not None:
                    return v
    except Exception:
        pass

    return default
'''

s2 = s[:m.start()] + new_fn + "\n\n" + s[m.end():]
p.write_text(s2, encoding="utf-8", newline="\n")
print("OK: replaced _get_strat_attr with robust version (REPLACED_GET_STRAT_ATTR_V2)")
