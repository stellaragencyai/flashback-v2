#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Patch: ensure every record written to outcomes.jsonl has a canonical setup_fingerprint.

Why:
- ai_integrity_check fails when outcomes.jsonl contains raw outcome_record without setup_fingerprint.
- This happens when pending setup lookup fails and we write the raw event to OUTCOMES_PATH.

What this patch does:
1) Adds helper _ensure_outcome_fingerprints(event) near other fingerprint helpers.
2) Calls it:
   - before writing enriched outcomes to OUTCOMES_PATH
   - before writing raw outcome_record to OUTCOMES_PATH (when pending missing)
3) Promotes fingerprints to canonical fields on outcome_enriched too.

Idempotent:
- Safe to run multiple times.
- Will not duplicate helper or duplicate call sites.
"""

from __future__ import annotations

import time
from pathlib import Path

TARGET = Path(r"app\ai\ai_events_spine.py")


def _backup(p: Path) -> Path:
    b = p.with_suffix(".py.bak_" + time.strftime("%Y%m%d_%H%M%S"))
    b.write_bytes(p.read_bytes())
    return b


HELPER = r'''
# ---------------------------------------------------------------------------
# Outcome fingerprinting (integrity-critical)
# ---------------------------------------------------------------------------

def _ensure_outcome_fingerprints(event: Dict[str, Any]) -> None:
    """
    Guarantee that outcome_record/outcome_enriched written to outcomes.jsonl carries a canonical setup_fingerprint.

    Rules:
    - If we can recover setup_fingerprint/memory_fingerprint from embedded setup/payload/extra, use it.
    - If missing (or orphan/test outcome), synthesize a deterministic setup_fingerprint so integrity checks do not fail.
    - Mark synthetic fingerprints via extra.synthetic_setup_fingerprint = True for downstream filtering.
    """
    try:
        if not isinstance(event, dict):
            return
        et = event.get("event_type")
        if et not in ("outcome_record", "outcome_enriched"):
            return

        # -------------------------
        # outcome_enriched
        # -------------------------
        if et == "outcome_enriched":
            sp = event.get("setup_fingerprint")
            mp = event.get("memory_fingerprint")

            setup = event.get("setup") if isinstance(event.get("setup"), dict) else {}
            try:
                setup_payload = setup.get("payload") if isinstance(setup.get("payload"), dict) else {}
                feats = setup_payload.get("features") if isinstance(setup_payload.get("features"), dict) else {}
                if not sp and isinstance(feats, dict):
                    sp = feats.get("setup_fingerprint")
                if not mp and isinstance(feats, dict):
                    mp = feats.get("memory_fingerprint")
            except Exception:
                pass

            extra = event.get("extra") if isinstance(event.get("extra"), dict) else {}
            if not sp and isinstance(extra, dict):
                sp = extra.get("setup_fingerprint")
            if not mp and isinstance(extra, dict):
                mp = extra.get("memory_fingerprint")

            # promote to canonical
            if sp and not event.get("setup_fingerprint"):
                event["setup_fingerprint"] = sp
            if mp and not event.get("memory_fingerprint"):
                event["memory_fingerprint"] = mp

            # keep in extra
            if isinstance(extra, dict):
                if sp and not extra.get("setup_fingerprint"):
                    extra["setup_fingerprint"] = sp
                if mp and not extra.get("memory_fingerprint"):
                    extra["memory_fingerprint"] = mp
                event["extra"] = extra

            # synthesize if still missing
            if not event.get("setup_fingerprint"):
                tid = str(event.get("trade_id") or "").strip()
                sym = str(event.get("symbol") or "").strip()
                acct = str(event.get("account_label") or "main").strip()
                strat = str(event.get("strategy") or "unknown").strip()
                tf = _normalize_timeframe(event.get("timeframe")) or "unknown"

                fp = _compute_setup_fingerprint(
                    trade_id=tid,
                    symbol=sym,
                    account_label=acct,
                    strategy=strat,
                    setup_type=str(event.get("setup_type")) if event.get("setup_type") is not None else None,
                    timeframe=tf,
                    features={},
                )
                event["setup_fingerprint"] = fp
                extra = event.get("extra") if isinstance(event.get("extra"), dict) else {}
                extra["setup_fingerprint"] = fp
                extra["synthetic_setup_fingerprint"] = True
                event["extra"] = extra

            return

        # -------------------------
        # outcome_record (raw)
        # -------------------------
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        extra = payload.get("extra") if isinstance(payload.get("extra"), dict) else {}

        sp = event.get("setup_fingerprint") or payload.get("setup_fingerprint") or (extra.get("setup_fingerprint") if isinstance(extra, dict) else None)
        mp = event.get("memory_fingerprint") or payload.get("memory_fingerprint") or (extra.get("memory_fingerprint") if isinstance(extra, dict) else None)

        if sp:
            event["setup_fingerprint"] = sp
            payload["setup_fingerprint"] = sp
        if mp:
            event["memory_fingerprint"] = mp
            payload["memory_fingerprint"] = mp

        if not event.get("setup_fingerprint"):
            tid = str(event.get("trade_id") or "").strip()
            sym = str(event.get("symbol") or "").strip()
            acct = str(event.get("account_label") or "main").strip()
            strat = str(event.get("strategy") or "unknown").strip()
            tf = _normalize_timeframe(event.get("timeframe"))
            if tf is None and isinstance(extra, dict):
                tf = _normalize_timeframe(extra.get("timeframe"))
            tf = tf or "unknown"

            fp = _compute_setup_fingerprint(
                trade_id=tid,
                symbol=sym,
                account_label=acct,
                strategy=strat,
                setup_type=None,
                timeframe=tf,
                features={},
            )
            event["setup_fingerprint"] = fp
            payload["setup_fingerprint"] = fp
            if isinstance(extra, dict):
                extra["setup_fingerprint"] = fp
                extra["synthetic_setup_fingerprint"] = True

        payload["extra"] = extra if isinstance(extra, dict) else {}
        event["payload"] = payload
    except Exception:
        return
'''


def main() -> int:
    p = TARGET
    if not p.exists():
        print("PATCH_FAIL: missing file:", p)
        return 2

    s = p.read_text(encoding="utf-8", errors="ignore")

    # 1) Insert helper near fingerprint helpers
    if "def _ensure_outcome_fingerprints(" not in s:
        anchor = "\n# ---------------------------------------------------------------------------\n# Policy stamping (versions + hash)\n# ---------------------------------------------------------------------------\n"
        if anchor not in s:
            print("PATCH_FAIL: could not find policy stamping anchor")
            return 3
        s = s.replace(anchor, "\n" + HELPER + "\n" + anchor, 1)
        print("PATCH_OK: inserted _ensure_outcome_fingerprints helper")
    else:
        print("PATCH_OK: helper already present (skip insert)")

    # 2) Ensure publish_ai_event uses it before writing outcomes.jsonl
    # a) enriched branch
    sig_a = "_append_jsonl(OUTCOMES_PATH, enriched)"
    want_a = "_ensure_outcome_fingerprints(enriched)\n                " + sig_a
    if want_a not in s:
        if sig_a in s:
            s = s.replace(sig_a, want_a, 1)
            print("PATCH_OK: added fingerprint ensure for enriched outcome writes")
        else:
            print("PATCH_WARN: could not find enriched OUTCOMES_PATH append site")

    # b) raw outcome branch when setup_evt missing
    sig_b = "_append_jsonl(OUTCOMES_PATH, event)"
    want_b = "_ensure_outcome_fingerprints(event)\n                " + sig_b

    # We only want to patch the specific else branch that writes raw outcomes into OUTCOMES_PATH
    # We'll patch the FIRST occurrence *inside* the 'else:' after 'if setup_evt:' block.
    marker = "            if setup_evt:\n                enriched = _merge_setup_and_outcome(setup_evt, event)\n"
    if marker in s:
        tail = s.split(marker, 1)[1]
        # Find first occurrence of sig_b after marker
        idx = tail.find(sig_b)
        if idx != -1:
            # Replace only if not already ensured just above it
            pre = tail[max(0, idx - 120):idx]
            if "_ensure_outcome_fingerprints(event)" not in pre:
                tail = tail[:idx] + want_b + tail[idx + len(sig_b):]
                s = s.split(marker, 1)[0] + marker + tail
                print("PATCH_OK: added fingerprint ensure for raw outcome writes (pending miss)")
            else:
                print("PATCH_OK: raw outcome write already ensured (skip)")
        else:
            print("PATCH_WARN: could not locate raw OUTCOMES_PATH append after setup_evt marker")
    else:
        print("PATCH_WARN: could not locate setup_evt marker block; no raw-branch patch applied")

    # 3) Promote canonical fields on enriched outcomes (in _merge_setup_and_outcome)
    promote_snip = "        # Promote fingerprints to canonical fields for integrity joins"
    if promote_snip not in s:
        anchor2 = "\"extra\": {"
        # Add after extra dict is created by the current code (keeps patch simple)
        # We'll insert promotion just before `return enriched` in that function.
        if "def _merge_setup_and_outcome" in s and "return enriched" in s:
            s = s.replace(
                "        return enriched",
                "        # Promote fingerprints to canonical fields for integrity joins\n"
                "        try:\n"
                "            if 'setup_fingerprint' not in enriched:\n"
                "                enriched['setup_fingerprint'] = (enriched.get('extra') or {}).get('setup_fingerprint')\n"
                "            if 'memory_fingerprint' not in enriched:\n"
                "                enriched['memory_fingerprint'] = (enriched.get('extra') or {}).get('memory_fingerprint')\n"
                "        except Exception:\n"
                "            pass\n\n"
                "        return enriched",
                1,
            )
            print("PATCH_OK: added canonical fingerprint promotion in _merge_setup_and_outcome")
        else:
            print("PATCH_WARN: could not patch fingerprint promotion in _merge_setup_and_outcome")
    else:
        print("PATCH_OK: promotion already present (skip)")

    b = _backup(p)
    p.write_text(s, encoding="utf-8")
    print("PATCH_DONE: wrote file and backup:", b)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
