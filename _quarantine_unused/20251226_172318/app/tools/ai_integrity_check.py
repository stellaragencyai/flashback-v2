
# -*- coding: utf-8 -*-
"""
Flashback — AI Integrity Check Tool v1.1 (Phase 3)

Validates:
  ✅ setups == outcomes (or still pending within TTL)
  ✅ pending_setups doesn't grow without bound
  ✅ outcomes have fingerprints (or explain why)
  ✅ R-multiple presence rate
  ✅ basic counts by account/strategy/symbol

Inputs:
  - state/ai_events/setups.jsonl
  - state/ai_events/outcomes.jsonl
  - state/ai_events/pending_setups.json   (OPTIONAL; missing => treated as empty)

Output:
  - prints a report
  - exits code 0 if PASS, 2 if FAIL
"""

from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import orjson


def _now_ms() -> int:
    return int(time.time() * 1000)


def _read_jsonl_counts(path: Path, max_lines: int = 200_000) -> Tuple[int, int]:
    """
    Returns (lines_total, lines_parsed_ok)
    """
    if not path.exists():
        return 0, 0
    total = 0
    ok = 0
    with path.open("rb") as f:
        for raw in f:
            total += 1
            if total > max_lines:
                break
            raw = raw.strip()
            if not raw:
                continue
            try:
                _ = orjson.loads(raw)
                ok += 1
            except Exception:
                pass
    return total, ok


def _load_pending(path: Path) -> Dict[str, Any]:
    """
    Pending registry is OPTIONAL.
    Missing file => empty dict (valid cold-start state).
    """
    if not path.exists():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8") or "{}")
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _safe_get(d: Any, *keys: str) -> Any:
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _extract_fp_from_setup_event(setup_evt: Dict[str, Any]) -> Optional[str]:
    feats = _safe_get(setup_evt, "payload", "features")
    if isinstance(feats, dict):
        fp = feats.get("setup_fingerprint")
        if isinstance(fp, str) and fp.strip():
            return fp.strip()
    return None


def _extract_fp_from_outcome_evt(out_evt: Dict[str, Any]) -> Optional[str]:
    # outcome_enriched: setup.payload.features.setup_fingerprint
    fp = _safe_get(out_evt, "setup", "payload", "features", "setup_fingerprint")
    if isinstance(fp, str) and fp.strip():
        return fp.strip()

    # sometimes direct
    fp2 = out_evt.get("setup_fingerprint")
    if isinstance(fp2, str) and fp2.strip():
        return fp2.strip()

    return None


def _extract_r(out_evt: Dict[str, Any]) -> Optional[float]:
    r = _safe_get(out_evt, "stats", "r_multiple")
    if r is None:
        r = _safe_get(out_evt, "payload", "r_multiple")
    if r is None:
        return None
    try:
        v = float(r)
        if v != v:  # NaN
            return None
        return v
    except Exception:
        return None


def _extract_trade_id(evt: Dict[str, Any]) -> Optional[str]:
    tid = evt.get("trade_id")
    if isinstance(tid, str) and tid.strip():
        return tid.strip()
    if tid is not None:
        return str(tid)
    return None


def _extract_identity(evt: Dict[str, Any]) -> Tuple[str, str, str]:
    label = evt.get("account_label") or "unknown_label"
    strat = evt.get("strategy") or evt.get("strategy_name") or "unknown_strategy"
    sym = evt.get("symbol") or "unknown_symbol"
    return str(label), str(strat), str(sym)


def run_check(
    root: Path,
    pending_ttl_minutes: int = 20,
    max_lines: int = 200_000,
    strict: bool = False,
) -> int:
    state_dir = root / "state"
    ai_dir = state_dir / "ai_events"

    setups_path = ai_dir / "setups.jsonl"
    outcomes_path = ai_dir / "outcomes.jsonl"
    pending_path = ai_dir / "pending_setups.json"

    # Presence checks (pending is OPTIONAL)
    missing_required = []
    for p in (setups_path, outcomes_path):
        if not p.exists():
            missing_required.append(str(p))
    if missing_required:
        print("❌ Missing required files:")
        for m in missing_required:
            print("  -", m)
        return 2

    # Quick JSON sanity
    setups_total, setups_ok = _read_jsonl_counts(setups_path, max_lines=max_lines)
    outcomes_total, outcomes_ok = _read_jsonl_counts(outcomes_path, max_lines=max_lines)

    pending = _load_pending(pending_path)
    pending_missing = not pending_path.exists()

    # Build sets: trade_id in setups/outcomes
    setups_by_trade: Dict[str, Dict[str, Any]] = {}
    outcomes_by_trade: Dict[str, Dict[str, Any]] = {}

    fp_orphans = 0
    outcomes_missing_fp = 0
    outcomes_missing_r = 0

    counts_setups = Counter()
    counts_outcomes = Counter()

    # Load setups
    with setups_path.open("rb") as f:
        n = 0
        for raw in f:
            n += 1
            if n > max_lines:
                break
            raw = raw.strip()
            if not raw:
                continue
            try:
                evt = orjson.loads(raw)
            except Exception:
                continue
            if not isinstance(evt, dict):
                continue
            if evt.get("event_type") != "setup_context":
                continue
            tid = _extract_trade_id(evt)
            if not tid:
                continue
            setups_by_trade[tid] = evt
            label, strat, sym = _extract_identity(evt)
            counts_setups[(label, strat, sym)] += 1

    # Load outcomes
    with outcomes_path.open("rb") as f:
        n = 0
        for raw in f:
            n += 1
            if n > max_lines:
                break
            raw = raw.strip()
            if not raw:
                continue
            try:
                evt = orjson.loads(raw)
            except Exception:
                continue
            if not isinstance(evt, dict):
                continue
            et = evt.get("event_type")
            if et not in ("outcome_enriched", "outcome_record"):
                continue
            tid = _extract_trade_id(evt)
            if not tid:
                continue
            outcomes_by_trade[tid] = evt
            label, strat, sym = _extract_identity(evt)
            counts_outcomes[(label, strat, sym)] += 1

            fp = _extract_fp_from_outcome_evt(evt)
            if not fp:
                outcomes_missing_fp += 1

            r = _extract_r(evt)
            if r is None:
                outcomes_missing_r += 1

    # Compare setups vs outcomes
    missing_outcomes = []
    for tid in setups_by_trade.keys():
        if tid not in outcomes_by_trade:
            missing_outcomes.append(tid)

    orphan_outcomes = []
    for tid in outcomes_by_trade.keys():
        if tid not in setups_by_trade:
            orphan_outcomes.append(tid)

    # Pending TTL check
    ttl_ms = pending_ttl_minutes * 60_000
    now = _now_ms()
    stale_pending = []
    for tid, setup_evt in pending.items():
        if not isinstance(setup_evt, dict):
            continue
        ts = setup_evt.get("ts") or setup_evt.get("ts_ms")
        try:
            ts_i = int(ts) if ts is not None else 0
        except Exception:
            ts_i = 0
        if ts_i and (now - ts_i) > ttl_ms:
            stale_pending.append(tid)

    # Missing outcomes partition:
    # - ok_missing: the setup is pending and not stale
    # - bad_missing: not in pending at all (or pending file missing)
    stale_pending_set = set(stale_pending)
    pending_keys = set(str(k) for k in pending.keys())

    ok_missing = [tid for tid in missing_outcomes if tid in pending_keys and tid not in stale_pending_set]
    bad_missing = [tid for tid in missing_outcomes if tid not in pending_keys]

    # Outcome fingerprints orphan check: if we have outcome fp but no setup fp, count as orphan
    for tid, out_evt in outcomes_by_trade.items():
        fp = _extract_fp_from_outcome_evt(out_evt)
        if not fp:
            continue
        s_evt = setups_by_trade.get(tid)
        if not s_evt:
            fp_orphans += 1
            continue
        s_fp = _extract_fp_from_setup_event(s_evt)
        if not s_fp:
            fp_orphans += 1

    # Report
    print("\n🧪 Phase-3 AI Integrity Check")
    print("================================")
    print(f"• setups.jsonl   : lines={setups_total} parsed_ok={setups_ok}")
    print(f"• outcomes.jsonl : lines={outcomes_total} parsed_ok={outcomes_ok}")
    pend_note = " (MISSING => treated as empty)" if pending_missing else ""
    print(f"• pending_setups : {len(pending)} entries{pend_note}\n")

    print("📌 Linkage")
    print(f"• setups_count             : {len(setups_by_trade)} (by trade_id)")
    print(f"• outcomes_count           : {len(outcomes_by_trade)} (by trade_id)")
    print(f"• missing_outcomes_total   : {len(missing_outcomes)}")
    print(f"• missing_outcomes_ok      : {len(ok_missing)} (pending & within TTL)")
    print(f"• missing_outcomes_bad     : {len(bad_missing)} (NOT pending)")
    print(f"• orphan_outcomes          : {len(orphan_outcomes)}")
    print(f"• stale_pending(>{pending_ttl_minutes}m): {len(stale_pending)}\n")

    print("📌 Data quality")
    print(f"• outcomes_missing_fp : {outcomes_missing_fp}")
    print(f"• outcomes_missing_r  : {outcomes_missing_r}")
    print(f"• fp_orphans          : {fp_orphans}\n")

    # Top offenders
    if stale_pending:
        print("🧟 Stale pending trade_ids (sample up to 20):")
        for tid in stale_pending[:20]:
            print("  -", tid)
        print()

    if bad_missing:
        print("🕳️ Setups missing outcomes AND not pending (sample up to 20):")
        for tid in bad_missing[:20]:
            print("  -", tid)
        print()

    if orphan_outcomes:
        print("👻 Outcomes without setups (sample up to 20):")
        for tid in orphan_outcomes[:20]:
            print("  -", tid)
        print()

    # Counts by identity (only show if nontrivial)
    def _top(counter: Counter, n: int = 10):
        items = counter.most_common(n)
        for (label, strat, sym), c in items:
            print(f"  - {label} | {strat} | {sym} : {c}")

    if counts_setups:
        print("📊 Top setup identities:")
        _top(counts_setups, 10)
        print()

    if counts_outcomes:
        print("📊 Top outcome identities:")
        _top(counts_outcomes, 10)
        print()

    # PASS/FAIL rules
    fail_reasons = []

    # These are non-negotiable
    if setups_ok < max(0, setups_total - 5):
        fail_reasons.append("Too many invalid JSON lines in setups.jsonl")
    if outcomes_ok < max(0, outcomes_total - 5):
        fail_reasons.append("Too many invalid JSON lines in outcomes.jsonl")

    if strict:
        if missing_outcomes:
            fail_reasons.append("Strict: setups missing outcomes")
        if stale_pending:
            fail_reasons.append("Strict: stale pending setups exist")
        if orphan_outcomes:
            fail_reasons.append("Strict: outcomes without setups exist")
    else:
        # Non-strict: missing outcomes are ONLY acceptable if they're pending and within TTL.
        if stale_pending:
            fail_reasons.append("Stale pending setups exist (reconciler needed or stuck pipeline)")
        if bad_missing:
            fail_reasons.append("Setups missing outcomes and NOT pending (lost outcomes or pending registry broken)")

    # missing fp is bad (learning will be garbage)
    if outcomes_missing_fp > 0:
        fail_reasons.append("Some outcomes are missing setup_fingerprint")

    # R missing isn't always fatal yet (fills), but if it's too much it's a problem
    if len(outcomes_by_trade) > 0:
        frac_missing_r = outcomes_missing_r / max(1, len(outcomes_by_trade))
        if frac_missing_r > 0.60:
            fail_reasons.append(f"Too many outcomes missing R (missing_frac={frac_missing_r:.2f})")

    if fail_reasons:
        print("❌ FAIL")
        for r in fail_reasons:
            print("  -", r)
        print()
        return 2

    print("✅ PASS\n")
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(description="Flashback Phase-3 AI Integrity Check")
    ap.add_argument("--root", type=str, default=".", help="Project root (default: .)")
    ap.add_argument("--ttl-min", type=int, default=20, help="Pending TTL minutes (default: 20)")
    ap.add_argument("--max-lines", type=int, default=200000, help="Max lines per file to scan")
    ap.add_argument("--strict", action="store_true", help="Fail on any missing linkages")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    rc = run_check(
        root=root,
        pending_ttl_minutes=int(args.ttl_min),
        max_lines=int(args.max_lines),
        strict=bool(args.strict),
    )
    raise SystemExit(rc)


if __name__ == "__main__":
    main()

