from pathlib import Path
import re
import time

p = Path("app/bots/executor_v2.py")
s = p.read_text("utf-8")

# backup once
bak = p.with_suffix(".py.bak")
if not bak.exists():
    bak.write_text(s, encoding="utf-8")

# 1) enforce_decision must receive account_label (fix first occurrence)
s, n1 = re.subn(
    r'enforced\s*=\s*enforce_decision\(\s*client_trade_id\s*\)',
    'enforced = enforce_decision(client_trade_id, account_label=account_label)',
    s,
    count=1
)

# 2) insert emit_pilot_enforced_decision if missing
anchor = "\n# ---------------------------------------------------------------------------\n# ✅ Decision emitter (executor audit rows for joining/telemetry)\n# ---------------------------------------------------------------------------\n"

if "def emit_pilot_enforced_decision" not in s:
    func = r'''
def emit_pilot_enforced_decision(setup_event, enforced, *, pilot_row=None, size_multiplier_applied=1.0, enforced_code=None, enforced_reason="ok"):
    """
    Append a second pilot-schema (schema_version==1) decision row AFTER enforcement,
    so joiners/audits see the enforced multiplier/reason.
    """
    try:
        if not isinstance(setup_event, dict) or not isinstance(enforced, dict):
            return None

        trade_id = str(setup_event.get("trade_id") or "").strip()
        if not trade_id:
            return None

        client_trade_id = str(setup_event.get("client_trade_id") or trade_id)
        symbol = str(setup_event.get("symbol") or "").upper()
        account_label = str(setup_event.get("account_label") or "")
        timeframe = str(setup_event.get("timeframe") or "")

        policy_hash = ""
        pol = setup_event.get("policy")
        if isinstance(pol, dict):
            policy_hash = str(pol.get("policy_hash") or "")

        decision_code = enforced_code or enforced.get("decision_code") or "ALLOW_TRADE"
        allow = bool(enforced.get("allow", True))

        try:
            sm = float(size_multiplier_applied)
        except Exception:
            sm = 1.0

        row = {
            "schema_version": 1,
            "ts": int(time.time() * 1000),
            "trade_id": trade_id,
            "client_trade_id": client_trade_id,
            "source_trade_id": setup_event.get("source_trade_id"),
            "symbol": symbol,
            "account_label": account_label,
            "timeframe": timeframe,
            "policy_hash": policy_hash,
            "decision": str(decision_code),
            "allow": allow,
            "size_multiplier": sm,
            "gates": {"reason": enforced_reason, "enforced": True},
            "meta": {
                "source": "executor_post_enforce",
                "stage": "post_enforce",
                "enforced_code": decision_code,
                "enforced_reason": enforced_reason,
                "enforced_size_multiplier": sm,
            },
        }
        _append_decision(row)
        return row
    except Exception:
        return None
'''
    s = s.replace(anchor, "\n" + func + anchor, 1)

# 3) inject call after size_multiplier_applied assignment
needle = "        if sm > 0:\n            size_multiplier_applied = sm\n"
inject = """        if sm > 0:
            size_multiplier_applied = sm

        # write post-enforce pilot row
        try:
            emit_pilot_enforced_decision(
                pilot_setup_event,
                enforced,
                pilot_row=pilot_row,
                size_multiplier_applied=float(size_multiplier_applied),
                enforced_code=str(enforced_code or ""),
                enforced_reason=str(enforced_reason or "ok"),
            )
        except Exception:
            pass
"""

if needle in s and "emit_pilot_enforced_decision(" not in s:
    s = s.replace(needle, inject, 1)

p.write_text(s, encoding="utf-8")

print(
    "patched_ok=1",
    "enforce_fix_replacements=", n1,
    "has_func=", ("def emit_pilot_enforced_decision" in s),
    "has_call=", ("emit_pilot_enforced_decision(" in s),
)
