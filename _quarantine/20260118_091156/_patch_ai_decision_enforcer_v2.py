import pathlib
import re

p = pathlib.Path(r".\app\ai\ai_decision_enforcer.py")
s = p.read_text(encoding="utf-8")

# 1) Ensure fallback helper exists
if "def _scan_file_for_trade_id" not in s:
    helper = r'''
# ---- FALLBACK_FULL_SCAN: if tail scan misses an older decision, scan file once (bounded).
def _scan_file_for_trade_id(path: Path, trade_id: str, *, max_mb: int = 200):
    try:
        if not path.exists():
            return None
        if path.stat().st_size > max_mb * 1024 * 1024:
            return None
        import orjson
        with path.open("rb") as f:
            for raw in f:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    d = orjson.loads(raw)
                except Exception:
                    continue
                if isinstance(d, dict) and (
                    str(d.get("trade_id") or "") == trade_id
                    or str(d.get("client_trade_id") or "") == trade_id
                ):
                    return d
        return None
    except Exception:
        return None
'''
    # Insert helper just before _read_lines_reverse (safe location)
    s = s.replace("def _read_lines_reverse()", helper + "\n\ndef _read_lines_reverse()")

# 2) Inject fallback scan before NO_DECISION return (regex, not brittle string match)
# We find the NO_DECISION return dict and wrap it with a fallback attempt.
pattern = re.compile(
    r'''
return\s*\{\s*
\s*"allow"\s*:\s*False\s*,\s*
\s*"size_multiplier"\s*:\s*0\.0\s*,\s*
\s*"decision_code"\s*:\s*"NO_DECISION"\s*,\s*
\s*"reason"\s*:\s*"no_decision_found"\s*,?\s*
\}\s*
''',
    re.VERBOSE
)

if "full_scan_fallback" not in s:
    repl = r'''
d2 = _scan_file_for_trade_id(DECISIONS_PATH, str(trade_id))
    if isinstance(d2, dict):
        return {
            "allow": bool(d2.get("allow", False)),
            "size_multiplier": float(d2.get("size_multiplier", 0.0) or 0.0),
            "decision_code": str(d2.get("decision_code") or d2.get("decision") or "FOUND_BY_FULL_SCAN"),
            "reason": str(d2.get("reason") or "full_scan_fallback"),
        }

    return {
        "allow": False,
        "size_multiplier": 0.0,
        "decision_code": "NO_DECISION",
        "reason": "no_decision_found",
    }
'''
    s2, n = pattern.subn(repl, s, count=1)
    if n == 0:
        raise SystemExit("PATCH_FAILED: could not find NO_DECISION return block to hook")
    s = s2

p.write_text(s, encoding="utf-8")
print("PATCH_OK_REGEX_HOOK")
