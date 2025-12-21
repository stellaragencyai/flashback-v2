from pathlib import Path
import shutil
import sys
import re

p = Path(r"app\core\ai_decision_logger.py")
bak = Path(str(p) + ".bak_before_canonical_contract_patch")
if not bak.exists():
    shutil.copy2(p, bak)

s = p.read_text(encoding="utf-8", errors="ignore")

needle = "payload = _normalize_decision_context(dict(decision))"
if needle not in s:
    print("PATCH_FAIL: cannot find normalize needle")
    print("Backup:", bak)
    sys.exit(1)

insert = r'''
        # ------------------------------------------------------------------
        # ✅ Canonical Decision Store Contract (Phase 4 determinism)
        # ------------------------------------------------------------------
        # Goal: one stable shape per (trade_id, account_label, stage, event_type)
        # - pilot_decision: schema_version=1
        # - ai_decision:    schema_version=2
        # Drop placeholder/junk rows before they hit the store.

        # Normalize / infer event_type if missing
        et = _safe_str(payload.get("event_type"))
        if not et:
            if _safe_str(payload.get("decision_code")) or _safe_str(payload.get("decision")):
                # Prefer ai_decision for decision_code-bearing rows; pilot rows are schema_version==1 with "decision"
                et = "ai_decision"
            payload["event_type"] = et

        # Ensure schema_version exists and is stable
        sv_raw = payload.get("schema_version", None)
        sv = _safe_int(sv_raw, default=0)
        et = _safe_str(payload.get("event_type"))

        if sv <= 0:
            if et == "pilot_decision":
                payload["schema_version"] = 1
            elif et == "ai_decision":
                payload["schema_version"] = 2
            else:
                # default to v2 unless explicitly pilot-tagged
                payload["schema_version"] = 2

        # Normalize decision_code from decision/payload
        dc = _safe_str(payload.get("decision_code"))
        d = _safe_str(payload.get("decision"))
        pl = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
        if not dc and isinstance(pl, dict):
            dc = _safe_str(pl.get("decision_code"))
        if not d and isinstance(pl, dict):
            d = _safe_str(pl.get("decision"))

        # If decision_code missing but decision present, copy it
        if not dc and d:
            payload["decision_code"] = d
            dc = d

        # Drop junk placeholders early
        if _safe_str(dc).upper() in ("NO_DECISION",):
            return

        # Force stage tag for stable dedupe behavior
        try:
            payload.setdefault("extra", {})
            if isinstance(payload["extra"], dict):
                stage = _safe_str(payload["extra"].get("stage"))
                if not stage:
                    # default stage by event type
                    payload["extra"]["stage"] = "pilot" if _safe_str(payload.get("event_type")) == "pilot_decision" else "enforced"
        except Exception:
            pass
'''

# Insert right after normalization line
s2 = s.replace(needle, needle + insert)

# Sanity: ensure we didn't accidentally patch twice
if s2.count("Canonical Decision Store Contract (Phase 4 determinism)") != 1:
    print("PATCH_FAIL: patch marker count != 1 (already patched?)")
    print("Backup:", bak)
    sys.exit(1)

p.write_text(s2, encoding="utf-8")
print("PATCH_OK: ai_decision_logger canonical contract enforced.")
print("Backup:", bak)
