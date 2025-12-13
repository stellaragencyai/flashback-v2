#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Phase 3 Integrity Checker v1.0

Purpose
-------
Validate Phase 3 data integrity for AI learning + performance aggregation.

Checks (high signal)
--------------------
A) outcomes.jsonl (canonical):
   - JSON parse rate
   - allowed event_type
   - required fields present
   - trade_id duplicates (hard fail)
   - setup_fingerprint presence rate
   - terminal truth: extra.is_terminal=True AND stats.is_terminal=True for enriched
   - final_status presence for enriched
   - timeframe validity

B) outcomes_raw.jsonl (raw debug):
   - parse rate, basic required fields

C) setups.jsonl + pending_setups.json:
   - parse rate (setups)
   - pending registry size
   - pending trade_ids that already have outcomes (stuck/bug)

D) outcome_dedupe.json:
   - format sanity

E) setup_perf.json:
   - exists + parse + setup count sanity

Exit code
---------
0 = PASS (within thresholds)
2 = FAIL (threshold exceeded / hard fail)
"""

from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

try:
    from app.core.config import settings  # type: ignore
    ROOT: Path = settings.ROOT  # type: ignore[attr-defined]
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

STATE_DIR = ROOT / "state"
AI_EVENTS_DIR = STATE_DIR / "ai_events"
AI_PERF_DIR = STATE_DIR / "ai_perf"

OUTCOMES_PATH = AI_EVENTS_DIR / "outcomes.jsonl"
OUTCOMES_RAW_PATH = AI_EVENTS_DIR / "outcomes_raw.jsonl"
SETUPS_PATH = AI_EVENTS_DIR / "setups.jsonl"
PENDING_PATH = AI_EVENTS_DIR / "pending_setups.json"
DEDUPE_PATH = AI_EVENTS_DIR / "outcome_dedupe.json"

PERF_STORE_PATH = AI_PERF_DIR / "setup_perf.json"

ALLOWED_OUTCOME_TYPES = {"outcome_enriched", "outcome_record"}

REQUIRED_OUTCOME_FIELDS = ("account_label", "strategy_name", "symbol", "timeframe")
REQUIRED_SETUP_FIELDS = ("account_label", "strategy_name", "symbol", "timeframe", "trade_id")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)).strip())
    except Exception:
        return default


@dataclass
class Thresholds:
    # parse health
    max_parse_fail_frac: float = 0.005  # 0.5%
    # field health
    max_missing_required_frac: float = 0.01  # 1%
    max_missing_fingerprint_frac: float = 0.05  # 5%
    max_missing_terminal_flag_frac: float = 0.01  # 1%
    max_missing_final_status_frac: float = 0.01  # 1%
    # duplicates
    allow_duplicate_trade_ids: bool = False
    # pending registry
    max_pending_count: int = 500
    max_pending_already_outcomed: int = 0  # should be 0
    # minimum data volume to judge (avoid false fail on tiny files)
    min_lines_to_enforce: int = 50


def load_thresholds() -> Thresholds:
    t = Thresholds()
    t.max_parse_fail_frac = _env_float("PH3_MAX_PARSE_FAIL_FRAC", t.max_parse_fail_frac)
    t.max_missing_required_frac = _env_float("PH3_MAX_MISSING_REQUIRED_FRAC", t.max_missing_required_frac)
    t.max_missing_fingerprint_frac = _env_float("PH3_MAX_MISSING_FINGERPRINT_FRAC", t.max_missing_fingerprint_frac)
    t.max_missing_terminal_flag_frac = _env_float("PH3_MAX_MISSING_TERMINAL_FLAG_FRAC", t.max_missing_terminal_flag_frac)
    t.max_missing_final_status_frac = _env_float("PH3_MAX_MISSING_FINAL_STATUS_FRAC", t.max_missing_final_status_frac)
    t.max_pending_count = _env_int("PH3_MAX_PENDING_COUNT", t.max_pending_count)
    t.max_pending_already_outcomed = _env_int("PH3_MAX_PENDING_ALREADY_OUTCOMED", t.max_pending_already_outcomed)
    t.min_lines_to_enforce = _env_int("PH3_MIN_LINES_TO_ENFORCE", t.min_lines_to_enforce)
    t.allow_duplicate_trade_ids = os.getenv("PH3_ALLOW_DUPLICATE_TRADE_IDS", "false").strip().lower() in ("1", "true", "yes", "y")
    return t


def _read_lines(path: Path, max_lines: Optional[int] = None) -> Iterable[bytes]:
    if not path.exists():
        return []
    # binary read for speed + resilience
    def gen() -> Iterable[bytes]:
        with path.open("rb") as f:
            i = 0
            for line in f:
                if max_lines is not None and i >= max_lines:
                    break
                i += 1
                yield line
    return gen()


def _json_loads_line(line: bytes) -> Optional[Dict[str, Any]]:
    line = line.strip()
    if not line:
        return None
    try:
        obj = json.loads(line.decode("utf-8"))
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _is_valid_timeframe(tf: Any) -> bool:
    if not isinstance(tf, str):
        return False
    s = tf.strip().upper()
    if not s:
        return False
    if s in ("UNKNOWN", "NA", "N/A", "NONE"):
        return False
    # accept typical Bybit strings: 1,3,5,15,30,60,120,240, etc OR "5m", "1h"
    return True


def _get_setup_fingerprint(evt: Dict[str, Any]) -> Optional[str]:
    fp = evt.get("setup_fingerprint")
    if isinstance(fp, str) and fp.strip():
        return fp.strip()

    setup = evt.get("setup")
    if isinstance(setup, dict):
        payload = setup.get("payload")
        if isinstance(payload, dict):
            feats = payload.get("features")
            if isinstance(feats, dict):
                fp2 = feats.get("setup_fingerprint")
                if isinstance(fp2, str) and fp2.strip():
                    return fp2.strip()

    extra = evt.get("extra")
    if isinstance(extra, dict):
        fp3 = extra.get("setup_fingerprint")
        if isinstance(fp3, str) and fp3.strip():
            return fp3.strip()

    return None


def _get_terminal_flags(evt: Dict[str, Any]) -> Tuple[Optional[bool], Optional[bool]]:
    extra_term: Optional[bool] = None
    stats_term: Optional[bool] = None

    extra = evt.get("extra")
    if isinstance(extra, dict) and isinstance(extra.get("is_terminal"), bool):
        extra_term = bool(extra.get("is_terminal"))

    stats = evt.get("stats")
    if isinstance(stats, dict) and isinstance(stats.get("is_terminal"), bool):
        stats_term = bool(stats.get("is_terminal"))

    return extra_term, stats_term


def _get_final_status(evt: Dict[str, Any]) -> Optional[str]:
    extra = evt.get("extra")
    if isinstance(extra, dict):
        fs = extra.get("final_status")
        if isinstance(fs, str) and fs.strip():
            return fs.strip().upper()
    payload = evt.get("payload")
    if isinstance(payload, dict):
        extra2 = payload.get("extra")
        if isinstance(extra2, dict):
            fs2 = extra2.get("final_status")
            if isinstance(fs2, str) and fs2.strip():
                return fs2.strip().upper()
    return None


def _missing_required_fields(evt: Dict[str, Any], required: Tuple[str, ...]) -> List[str]:
    missing: List[str] = []
    for k in required:
        v = evt.get(k)
        if v is None:
            missing.append(k)
        elif isinstance(v, str) and not v.strip():
            missing.append(k)
    return missing


def _print_section(title: str) -> None:
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def _fmt_pct(n: int, d: int) -> str:
    if d <= 0:
        return "0.00%"
    return f"{(100.0 * n / d):.2f}%"


def check_outcomes(th: Thresholds) -> Tuple[bool, Set[str]]:
    """
    Returns:
      (pass, trade_ids_seen)
    """
    _print_section("A) outcomes.jsonl (canonical)")

    if not OUTCOMES_PATH.exists():
        print(f"Missing: {OUTCOMES_PATH}")
        return False, set()

    total = 0
    parse_fail = 0
    wrong_type = 0

    missing_required = 0
    missing_fingerprint = 0
    missing_terminal_flags = 0
    missing_final_status = 0
    bad_timeframe = 0

    trade_ids: List[str] = []
    dupes: List[str] = []

    for line in _read_lines(OUTCOMES_PATH):
        total += 1
        evt = _json_loads_line(line)
        if evt is None:
            parse_fail += 1
            continue

        et = evt.get("event_type") or evt.get("type")
        if et not in ALLOWED_OUTCOME_TYPES:
            wrong_type += 1
            continue

        tid = evt.get("trade_id")
        if isinstance(tid, str) and tid.strip():
            trade_ids.append(tid.strip())

        miss = _missing_required_fields(evt, REQUIRED_OUTCOME_FIELDS)
        if miss:
            missing_required += 1

        tf = evt.get("timeframe")
        if not _is_valid_timeframe(tf):
            bad_timeframe += 1

        fp = _get_setup_fingerprint(evt)
        if not fp:
            missing_fingerprint += 1

        # terminal truth checks only for enriched (canonical learning event)
        if et == "outcome_enriched":
            extra_term, stats_term = _get_terminal_flags(evt)
            if extra_term is not True or stats_term is not True:
                missing_terminal_flags += 1

            fs = _get_final_status(evt)
            if not fs:
                missing_final_status += 1

    # dupes
    seen: Set[str] = set()
    for tid in trade_ids:
        if tid in seen:
            dupes.append(tid)
        else:
            seen.add(tid)

    print(f"File: {OUTCOMES_PATH}")
    print(f"Lines total: {total}")
    print(f"Parse failures: {parse_fail} ({_fmt_pct(parse_fail, total)})")
    print(f"Wrong event_type: {wrong_type} ({_fmt_pct(wrong_type, total)})")
    print(f"Missing required fields: {missing_required} ({_fmt_pct(missing_required, total)})")
    print(f"Bad timeframe: {bad_timeframe} ({_fmt_pct(bad_timeframe, total)})")
    print(f"Missing setup_fingerprint: {missing_fingerprint} ({_fmt_pct(missing_fingerprint, total)})")
    print(f"Missing terminal flags (enriched): {missing_terminal_flags} ({_fmt_pct(missing_terminal_flags, total)})")
    print(f"Missing final_status (enriched): {missing_final_status} ({_fmt_pct(missing_final_status, total)})")
    print(f"Unique trade_id: {len(seen)}")
    print(f"Duplicate trade_id occurrences: {len(dupes)}")

    # show a few dupes
    if dupes:
        sample = dupes[:10]
        print(f"Sample duplicate trade_id: {sample}")

    enforce = total >= th.min_lines_to_enforce

    ok = True
    if enforce:
        if total > 0 and (parse_fail / total) > th.max_parse_fail_frac:
            ok = False
            print(f"FAIL: parse_fail_frac > {th.max_parse_fail_frac}")

        if total > 0 and (missing_required / total) > th.max_missing_required_frac:
            ok = False
            print(f"FAIL: missing_required_frac > {th.max_missing_required_frac}")

        if total > 0 and (missing_fingerprint / total) > th.max_missing_fingerprint_frac:
            ok = False
            print(f"FAIL: missing_fingerprint_frac > {th.max_missing_fingerprint_frac}")

        if total > 0 and (missing_terminal_flags / total) > th.max_missing_terminal_flag_frac:
            ok = False
            print(f"FAIL: missing_terminal_flag_frac > {th.max_missing_terminal_flag_frac}")

        if total > 0 and (missing_final_status / total) > th.max_missing_final_status_frac:
            ok = False
            print(f"FAIL: missing_final_status_frac > {th.max_missing_final_status_frac}")

    # duplicate trade_id is a hard fail unless explicitly allowed
    if dupes and not th.allow_duplicate_trade_ids:
        ok = False
        print("FAIL: duplicate trade_id detected (learning poison).")

    if not enforce:
        print(f"NOTE: Only {total} lines. Threshold enforcement disabled until >= {th.min_lines_to_enforce} lines.")

    return ok, seen


def check_outcomes_raw(th: Thresholds) -> bool:
    _print_section("B) outcomes_raw.jsonl (raw debug)")

    if not OUTCOMES_RAW_PATH.exists():
        print(f"Missing: {OUTCOMES_RAW_PATH} (not fatal, but recommended)")
        return True

    total = 0
    parse_fail = 0
    missing_required = 0

    for line in _read_lines(OUTCOMES_RAW_PATH):
        total += 1
        evt = _json_loads_line(line)
        if evt is None:
            parse_fail += 1
            continue

        et = evt.get("event_type") or evt.get("type")
        if et not in ("outcome_record", "outcome_enriched"):
            continue

        miss = _missing_required_fields(evt, REQUIRED_OUTCOME_FIELDS)
        if miss:
            missing_required += 1

    print(f"File: {OUTCOMES_RAW_PATH}")
    print(f"Lines total: {total}")
    print(f"Parse failures: {parse_fail} ({_fmt_pct(parse_fail, total)})")
    print(f"Missing required fields: {missing_required} ({_fmt_pct(missing_required, total)})")

    enforce = total >= th.min_lines_to_enforce
    ok = True
    if enforce and total > 0 and (parse_fail / total) > th.max_parse_fail_frac:
        ok = False
        print(f"FAIL: parse_fail_frac > {th.max_parse_fail_frac}")
    if enforce and total > 0 and (missing_required / total) > th.max_missing_required_frac:
        ok = False
        print(f"FAIL: missing_required_frac > {th.max_missing_required_frac}")

    if not enforce:
        print(f"NOTE: Only {total} lines. Threshold enforcement disabled until >= {th.min_lines_to_enforce} lines.")

    return ok


def check_setups_and_pending(outcome_trade_ids: Set[str], th: Thresholds) -> bool:
    _print_section("C) setups.jsonl + pending_setups.json")

    ok = True

    # setups.jsonl parse health
    if SETUPS_PATH.exists():
        total = 0
        parse_fail = 0
        missing_required = 0

        for line in _read_lines(SETUPS_PATH):
            total += 1
            evt = _json_loads_line(line)
            if evt is None:
                parse_fail += 1
                continue
            et = evt.get("event_type") or evt.get("type")
            if et != "setup_context":
                continue
            miss = _missing_required_fields(evt, REQUIRED_SETUP_FIELDS)
            if miss:
                missing_required += 1

        print(f"File: {SETUPS_PATH}")
        print(f"Lines total: {total}")
        print(f"Parse failures: {parse_fail} ({_fmt_pct(parse_fail, total)})")
        print(f"Missing required fields: {missing_required} ({_fmt_pct(missing_required, total)})")

        enforce = total >= th.min_lines_to_enforce
        if enforce and total > 0 and (parse_fail / total) > th.max_parse_fail_frac:
            ok = False
            print(f"FAIL: setups parse_fail_frac > {th.max_parse_fail_frac}")
        if enforce and total > 0 and (missing_required / total) > th.max_missing_required_frac:
            ok = False
            print(f"FAIL: setups missing_required_frac > {th.max_missing_required_frac}")
        if not enforce:
            print(f"NOTE: Only {total} lines. Threshold enforcement disabled until >= {th.min_lines_to_enforce} lines.")
    else:
        print(f"Missing: {SETUPS_PATH} (not fatal if you are early)")

    # pending registry health
    if PENDING_PATH.exists():
        try:
            pending = json.loads(PENDING_PATH.read_text(encoding="utf-8") or "{}")
            if not isinstance(pending, dict):
                ok = False
                print("FAIL: pending_setups.json is not a dict")
                pending = {}
        except Exception as e:
            ok = False
            print(f"FAIL: cannot parse pending_setups.json: {e}")
            pending = {}

        pending_count = len(pending)
        stuck_already_outcomed = 0
        sample_stuck: List[str] = []

        for tid in list(pending.keys()):
            if tid in outcome_trade_ids:
                stuck_already_outcomed += 1
                if len(sample_stuck) < 10:
                    sample_stuck.append(tid)

        print(f"Pending registry count: {pending_count}")
        print(f"Pending entries that already have outcomes: {stuck_already_outcomed}")
        if sample_stuck:
            print(f"Sample stuck trade_ids: {sample_stuck}")

        if pending_count > th.max_pending_count:
            ok = False
            print(f"FAIL: pending_count > {th.max_pending_count} (you have a leak)")
        if stuck_already_outcomed > th.max_pending_already_outcomed:
            ok = False
            print(f"FAIL: pending already outcomed > {th.max_pending_already_outcomed} (merge/removal bug)")
    else:
        print(f"Missing: {PENDING_PATH} (ok if not using pending registry yet)")

    return ok


def check_dedupe_registry() -> bool:
    _print_section("D) outcome_dedupe.json")

    if not DEDUPE_PATH.exists():
        print(f"Missing: {DEDUPE_PATH} (not fatal, but recommended for idempotency)")
        return True

    try:
        obj = json.loads(DEDUPE_PATH.read_text(encoding="utf-8") or "{}")
    except Exception as e:
        print(f"FAIL: cannot parse outcome_dedupe.json: {e}")
        return False

    if not isinstance(obj, dict):
        print("FAIL: outcome_dedupe.json is not a dict")
        return False

    keys = list(obj.keys())
    print(f"Entries: {len(keys)}")
    if keys:
        print(f"Sample key: {keys[0]}")
    return True


def check_perf_store() -> bool:
    _print_section("E) setup_perf.json (performance store)")

    if not PERF_STORE_PATH.exists():
        print(f"Missing: {PERF_STORE_PATH} (not fatal if you haven't run perf store yet)")
        return True

    try:
        obj = json.loads(PERF_STORE_PATH.read_text(encoding="utf-8") or "{}")
    except Exception as e:
        print(f"FAIL: cannot parse setup_perf.json: {e}")
        return False

    if not isinstance(obj, dict):
        print("FAIL: setup_perf.json is not a dict")
        return False

    setups = obj.get("setups")
    if not isinstance(setups, dict):
        print("FAIL: setup_perf.json.setups is not a dict")
        return False

    print(f"Setups tracked: {len(setups)}")
    last_update = obj.get("last_update")
    if isinstance(last_update, dict):
        print("Last update summary:")
        for k in ("processed_lines", "updated_setups", "skipped_nonterminal", "skipped_missing_learn_r"):
            if k in last_update:
                print(f"  - {k}: {last_update.get(k)}")
    return True


def main() -> int:
    th = load_thresholds()

    print("Flashback — Phase 3 Integrity Checker v1.0")
    print(f"Root: {ROOT}")
    print("Thresholds:")
    print(f"  max_parse_fail_frac: {th.max_parse_fail_frac}")
    print(f"  max_missing_required_frac: {th.max_missing_required_frac}")
    print(f"  max_missing_fingerprint_frac: {th.max_missing_fingerprint_frac}")
    print(f"  max_missing_terminal_flag_frac: {th.max_missing_terminal_flag_frac}")
    print(f"  max_missing_final_status_frac: {th.max_missing_final_status_frac}")
    print(f"  allow_duplicate_trade_ids: {th.allow_duplicate_trade_ids}")
    print(f"  max_pending_count: {th.max_pending_count}")
    print(f"  max_pending_already_outcomed: {th.max_pending_already_outcomed}")
    print(f"  min_lines_to_enforce: {th.min_lines_to_enforce}")

    ok_a, outcome_trade_ids = check_outcomes(th)
    ok_b = check_outcomes_raw(th)
    ok_c = check_setups_and_pending(outcome_trade_ids, th)
    ok_d = check_dedupe_registry()
    ok_e = check_perf_store()

    overall = ok_a and ok_b and ok_c and ok_d and ok_e

    _print_section("RESULT")
    if overall:
        print("PASS ✅ Phase 3 integrity is within thresholds.")
        return 0

    print("FAIL ❌ Phase 3 integrity check failed.")
    print("\nCommon fixes (because humans love creating bugs):")
    print("- Duplicate trade_id: stop double-writing outcomes (two workers running) or reset outcome_dedupe.json.")
    print("- Missing setup_fingerprint: ensure setup_context builder always includes features.setup_fingerprint.")
    print("- Missing terminal flags/final_status: ensure ai_events_spine enriched outcomes write extra.is_terminal=True and extra.final_status.")
    print("- Pending bloat: your merge/removal is failing or your outcome trade_id doesn't match setup trade_id.")
    return 2


if __name__ == "__main__":
    sys.exit(main())
