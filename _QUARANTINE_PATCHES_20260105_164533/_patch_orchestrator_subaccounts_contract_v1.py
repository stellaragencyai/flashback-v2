from __future__ import annotations
from pathlib import Path
import re

TARGET = Path(r"app\ops\orchestrator_v1.py")

def main():
    s = TARGET.read_text(encoding="utf-8", errors="ignore")

    # If already patched, bail
    if "ORCH_SUBACCOUNT_CONTRACT_V1" in s:
        print("OK: ORCH_SUBACCOUNT_CONTRACT_V1 already present")
        return

    # Find where out dict is built (near end) and inject subaccounts contract build
    # We'll add a subaccounts dict that mirrors started labels with useful fields.
    anchor = '    out = {\n'
    if anchor not in s:
        raise SystemExit("STOP: could not find out = { anchor")

    inject = r'''
    # --- ORCH_SUBACCOUNT_CONTRACT_V1 ---
    # Ensure cockpit/dashboard has stable subaccounts objects (never null).
    subaccounts_out: Dict[str, Any] = {}
    for label in started:
        p = procs.get(label, {}) or {}
        subaccounts_out[label] = {
            "label": label,
            "enabled": True,
            "online": bool(p.get("alive", False)),
            "phase": mode,
            "last_heartbeat_ms": int(p.get("started_ts_ms", 0) or 0),
            "strategy": {
                "name": str((next((rr.get("strategy_name") for rr in rows if str(rr.get("account_label","")).strip()==label), None)) or "unknown"),
                "version": "unknown",
            },
        }
    # Also include manifest entries even if skipped, so UI can show whole fleet.
    for rr in rows:
        label = str(rr.get("account_label") or "").strip()
        if not label:
            continue
        if label not in subaccounts_out:
            enabled = bool(rr.get("enabled", True)) and bool(rr.get("enable_ai_stack", True))
            subaccounts_out[label] = {
                "label": label,
                "enabled": bool(enabled),
                "online": False,
                "phase": mode,
                "last_heartbeat_ms": 0,
                "strategy": {
                    "name": str(rr.get("strategy_name") or "unknown"),
                    "version": "unknown",
                },
            }
    # --- END ORCH_SUBACCOUNT_CONTRACT_V1 ---

'''
    # Insert inject just before out = { line
    s2 = s.replace(anchor, inject + anchor)

    # Ensure out includes subaccounts
    # Insert into out dict right after mode
    s2 = s2.replace(
        '        "mode": mode,\n',
        '        "mode": mode,\n        "subaccounts": subaccounts_out,\n'
    )

    TARGET.write_text(s2, encoding="utf-8")
    print("OK: patched orchestrator_v1.py (subaccounts contract v1 written)")

if __name__ == "__main__":
    main()
