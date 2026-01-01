from pathlib import Path

path = Path("app/core/ai_decision_logger.py")
s = path.read_text(encoding="utf-8", errors="ignore")

anchor = 'payload = _normalize_decision_context(dict(decision))'
tag = 'pilot tagging: normalize legacy pilot input rows'

if anchor not in s:
    raise SystemExit("❌ anchor not found: " + anchor)

if tag in s:
    print("pilot tagging block already present")
    raise SystemExit(0)

block = anchor + """

        # --- pilot tagging: normalize legacy pilot input rows ---
        try:
            if payload.get("schema_version") == 1 and ("decision" in payload) and (not str(payload.get("event_type") or "").strip()):
                payload["event_type"] = "pilot_decision"
        except Exception:
            pass
"""

path.write_text(s.replace(anchor, block, 1), encoding="utf-8")
print("inserted pilot tagging block")
