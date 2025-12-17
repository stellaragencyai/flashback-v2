#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Phase 3 Integrity Checker v1.2 (training-safe)

Changes vs v1.1:
- outcomes_raw.jsonl is treated as DEBUG/RAW: parse checks only.
  It no longer fails the whole integrity check for missing "enriched" fields.
- outcomes.jsonl (canonical enriched) is only REQUIRED once enforcement is active
  (>= min_lines_to_enforce) AND there is evidence joins should exist.
- Better messaging around "unmatched outcomes" so you don't chase ghosts.

Rationale:
- Early in build-out, you can have executions/outcomes before you have
  reliable setup trade_id linking (orderLinkId).
- Failing Phase 3 because raw debug logs are not enriched is nonsense.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import orjson

ROOT = Path(__file__).resolve().parents[2]

STATE_DIR = ROOT / "state"
AI_EVENTS_DIR = STATE_DIR / "ai_events"
AI_PERF_DIR = STATE_DIR / "ai_perf"

OUTCOMES_PATH = AI_EVENTS_DIR / "outcomes.jsonl"  # enriched-only canonical
OUTCOMES_RAW_PATH = AI_EVENTS_DIR / "outcomes_raw.jsonl"  # raw debug
OUTCOMES_UNMATCHED_PATH = AI_EVENTS_DIR / "outcomes_unmatched.jsonl"  # quarantined raw

SETUPS_PATH = AI_EVENTS_DIR / "setups.jsonl"
PENDING_PATH = AI_EVENTS_DIR / "pending_setups.json"

OUTCOME_DEDUPE_PATH = AI_EVENTS_DIR / "outcome_dedupe.json"
SETUP_PERF_PATH = AI_PERF_DIR / "setup_perf.json"


def _read_jsonl(path: Path) -> Tuple[int, int, list[Dict[str, Any]]]:
    """
    Returns: (total_lines, parse_failures, parsed_dicts)
    """
    if not path.exists():
        return 0, 0, []
    total = 0
    bad = 0
    parsed: list[Dict[str, Any]] = []
    for b in path.read_bytes().splitlines():
        total += 1
        if not b.strip():
            continue
        try:
            obj = orjson.loads(b)
            if isinstance(obj, dict):
                parsed.append(obj)
            else:
                bad += 1
        except Exception:
            bad += 1
    return total, bad, parsed


def _pct(n: int, d: int) -> float:
    return (float(n) / float(d)) if d else 0.0


def _get_thresholds() -> Dict[str, Any]:
    # Keep defaults aligned with your v1.1 prints
    return {
        "max_parse_fail_frac": 0.005,
        "max_missing_required_frac": 0.01,
        "max_missing_fingerprint_frac": 0.05,
        "max_missing_terminal_flag_frac": 0.01,
        "max_missing_final_status_frac": 0.01,
        "allow_duplicate_trade_ids": False,
        "max_pending_count": 500,
        "max_pending_already_outcomed": 0,
        "min_lines_to_enforce": 50,
    }


def _setup_required_missing(d: Dict[str, Any]) -> bool:
    if d.get("event_type") != "setup_context":
        return False  # ignore non-setup lines if any
    if not d.get("trade_id"):
        return True
    if not d.get("symbol"):
        return True
    if not d.get("account_label"):
        return True
    if not (d.get("strategy") or d.get("strategy_name")):
        return True

    # timeframe can be top-level OR in payload.extra.timeframe (legacy)
    tf = d.get("timeframe")
    if not tf:
        payload = d.get("payload") if isinstance(d.get("payload"), dict) else {}
        extra = payload.get("extra") if isinstance(payload.get("extra"), dict) else {}
        tf = extra.get("timeframe")
    if not tf:
        return True

    payload = d.get("payload") if isinstance(d.get("payload"), dict) else {}
    feats = payload.get("features") if isinstance(payload.get("features"), dict) else {}
    fp = feats.get("setup_fingerprint")
    if not (isinstance(fp, str) and fp.strip()):
        return True

    return False


def _canon_outcome_missing_fields(d: Dict[str, Any]) -> Dict[str, bool]:
    """
    Canonical enriched outcome requirements.
    Returns a dict of specific missing flags.
    """
    missing: Dict[str, bool] = {
        "required": False,
        "fingerprint": False,
        "terminal": False,
        "final_status": False,
    }

    if d.get("event_type") not in ("outcome_record", "outcome_enriched"):
        # If it's not an outcome entry, treat as required missing
        missing["required"] = True
        return missing

    for k in ("trade_id", "symbol", "account_label"):
        if not d.get(k):
            missing["required"] = True

    # timeframe required for training joins
    if not d.get("timeframe"):
        missing["required"] = True

    # strategy required
    if not d.get("strategy"):
        missing["required"] = True

    payload = d.get("payload") if isinstance(d.get("payload"), dict) else {}
    if "pnl_usd" not in payload:
        missing["required"] = True

    extra = d.get("extra") if isinstance(d.get("extra"), dict) else {}
    if extra.get("is_terminal") is not True:
        missing["terminal"] = True

    if not extra.get("final_status"):
        missing["final_status"] = True

    fp = extra.get("setup_fingerprint")
    if not (isinstance(fp, str) and fp.strip()):
        missing["fingerprint"] = True

    return missing


def _read_json(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8") or "null")
    except Exception:
        return None


def main() -> None:
    th = _get_thresholds()
    print("Flashback — Phase 3 Integrity Checker v1.2")
    print(f"Root: {ROOT}")
    print("Thresholds:")
    for k, v in th.items():
        print(f"  {k}: {v}")
    print()

    fail = False

    # ------------------------------------------------------------------
    # A) outcomes.jsonl (canonical enriched)
    # ------------------------------------------------------------------
    print("=" * 70)
    print("A) outcomes.jsonl (canonical)")
    print("=" * 70)

    canon_total, canon_bad, canon = _read_jsonl(OUTCOMES_PATH)
    if not OUTCOMES_PATH.exists():
        print(f"Missing: {OUTCOMES_PATH}")
    else:
        miss_required = 0
        miss_fp = 0
        miss_terminal = 0
        miss_final = 0

        for d in canon:
            flags = _canon_outcome_missing_fields(d)
            miss_required += 1 if flags["required"] else 0
            miss_fp += 1 if flags["fingerprint"] else 0
            miss_terminal += 1 if flags["terminal"] else 0
            miss_final += 1 if flags["final_status"] else 0

        print(f"File: {OUTCOMES_PATH}")
        print(f"Lines total: {canon_total}")
        print(f"Parse failures: {canon_bad} ({_pct(canon_bad, canon_total)*100:.2f}%)")
        print(f"Missing required fields: {miss_required} ({_pct(miss_required, canon_total)*100:.2f}%)")
        print(f"Missing setup_fingerprint: {miss_fp} ({_pct(miss_fp, canon_total)*100:.2f}%)")
        print(f"Missing terminal flags: {miss_terminal} ({_pct(miss_terminal, canon_total)*100:.2f}%)")
        print(f"Missing final_status: {miss_final} ({_pct(miss_final, canon_total)*100:.2f}%)")

        if canon_total >= th["min_lines_to_enforce"]:
            if _pct(canon_bad, canon_total) > th["max_parse_fail_frac"]:
                print("FAIL: parse_fail_frac > threshold")
                fail = True
            if _pct(miss_required, canon_total) > th["max_missing_required_frac"]:
                print("FAIL: missing_required_frac > threshold")
                fail = True
            if _pct(miss_fp, canon_total) > th["max_missing_fingerprint_frac"]:
                print("FAIL: missing_fingerprint_frac > threshold")
                fail = True
            if _pct(miss_terminal, canon_total) > th["max_missing_terminal_flag_frac"]:
                print("FAIL: missing_terminal_flag_frac > threshold")
                fail = True
            if _pct(miss_final, canon_total) > th["max_missing_final_status_frac"]:
                print("FAIL: missing_final_status_frac > threshold")
                fail = True
        else:
            print(f"NOTE: Only {canon_total} lines. Threshold enforcement disabled until >= {th['min_lines_to_enforce']} lines.")

    print()

    # ------------------------------------------------------------------
    # B) outcomes_raw.jsonl (raw debug)
    # ------------------------------------------------------------------
    print("=" * 70)
    print("B) outcomes_raw.jsonl (raw debug)")
    print("=" * 70)

    raw_total, raw_bad, _raw = _read_jsonl(OUTCOMES_RAW_PATH)
    if not OUTCOMES_RAW_PATH.exists():
        print(f"Missing: {OUTCOMES_RAW_PATH} (not fatal, but recommended)")
    else:
        print(f"File: {OUTCOMES_RAW_PATH}")
        print(f"Lines total: {raw_total}")
        print(f"Parse failures: {raw_bad} ({_pct(raw_bad, raw_total)*100:.2f}%)")
        if raw_total >= th["min_lines_to_enforce"] and _pct(raw_bad, raw_total) > th["max_parse_fail_frac"]:
            print("FAIL: raw parse_fail_frac > threshold")
            fail = True
        else:
            print("NOTE: Raw outcomes are not required to be enriched. Only parse integrity is enforced.")
    print()

    # ------------------------------------------------------------------
    # C) setups.jsonl + pending_setups.json
    # ------------------------------------------------------------------
    print("=" * 70)
    print("C) setups.jsonl + pending_setups.json")
    print("=" * 70)

    setups_total, setups_bad, setups = _read_jsonl(SETUPS_PATH)
    if not SETUPS_PATH.exists():
        print(f"Missing: {SETUPS_PATH}")
        fail = True
    else:
        missing_req = 0
        for d in setups:
            if _setup_required_missing(d):
                missing_req += 1

        print(f"File: {SETUPS_PATH}")
        print(f"Lines total: {setups_total}")
        print(f"Parse failures: {setups_bad} ({_pct(setups_bad, setups_total)*100:.2f}%)")
        print(f"Missing required fields: {missing_req} ({_pct(missing_req, setups_total)*100:.2f}%)")

        if setups_total >= th["min_lines_to_enforce"]:
            if _pct(setups_bad, setups_total) > th["max_parse_fail_frac"]:
                print("FAIL: setups parse_fail_frac > threshold")
                fail = True
            if _pct(missing_req, setups_total) > th["max_missing_required_frac"]:
                print("FAIL: setups missing_required_frac > threshold")
                fail = True
        else:
            print(f"NOTE: Only {setups_total} lines. Threshold enforcement disabled until >= {th['min_lines_to_enforce']} lines.")

    pending = _read_json(PENDING_PATH)
    if pending is None:
        print(f"Missing: {PENDING_PATH} (ok if not using pending registry yet)")
    else:
        if isinstance(pending, dict):
            print(f"Pending entries: {len(pending)}")
            if len(pending) > th["max_pending_count"]:
                print("FAIL: pending_count > max_pending_count")
                fail = True
        else:
            print("WARN: pending_setups.json exists but is not a dict")
    print()

    # ------------------------------------------------------------------
    # D) outcome_dedupe.json
    # ------------------------------------------------------------------
    print("=" * 70)
    print("D) outcome_dedupe.json")
    print("=" * 70)

    dedupe = _read_json(OUTCOME_DEDUPE_PATH)
    if isinstance(dedupe, dict):
        keys = list(dedupe.keys())
        print(f"Entries: {len(keys)}")
        if keys:
            print(f"Sample key: {keys[0]}")
    else:
        print("Entries: 0")
    print()

    # ------------------------------------------------------------------
    # E) setup_perf.json
    # ------------------------------------------------------------------
    print("=" * 70)
    print("E) setup_perf.json (performance store)")
    print("=" * 70)

    if not SETUP_PERF_PATH.exists():
        print(f"Missing: {SETUP_PERF_PATH} (not fatal if you haven't run perf store yet)")
    else:
        print(f"File: {SETUP_PERF_PATH}")
    print()

    # ------------------------------------------------------------------
    # Canonical outcomes missing: enforce only when appropriate
    # ------------------------------------------------------------------
    if not OUTCOMES_PATH.exists():
        # If you're early-stage (setups < min_lines_to_enforce), don't fail.
        # This prevents "no joins yet" from blocking Phase 3 while you wire orderLinkId.
        if setups_total >= th["min_lines_to_enforce"]:
            print("NOTE: outcomes.jsonl missing and enforcement is active (setups >= min_lines_to_enforce).")
            print("FAIL: Canonical enriched outcomes are required at this stage.")
            fail = True
        else:
            print("NOTE: outcomes.jsonl missing but setups are below enforcement threshold.")
            print("      This usually means you have raw outcomes but no reliable setup↔outcome joins yet (orderLinkId not wired).")

    print()
    print("=" * 70)
    print("RESULT")
    print("=" * 70)

    if fail:
        print("FAIL ❌ Phase 3 integrity check failed.")
        print()
        print("Common fixes:")
        print("- Wire orderLinkId=trade_id on entry/TP/SL orders so outcomes can join setups.")
        print("- Ensure setup_context features include setup_fingerprint.")
        print("- Ensure canonical outcomes are terminal: extra.is_terminal=True and extra.final_status.")
    else:
        print("PASS ✅ Phase 3 integrity check passed.")
        print()
        print("Next priority if you want REAL learning:")
        print("- Wire orderLinkId so outcomes are joinable and outcomes.jsonl becomes populated.")


if __name__ == "__main__":
    main()
