# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Optional

ROOT = Path(__file__).resolve().parents[2]

OUTCOMES = ROOT / "state" / "ai_events" / "outcomes.v1.jsonl"
SETUPS = ROOT / "state" / "ai_events" / "setups.jsonl"

TRAINABLE_OUT = ROOT / "state" / "ai_events" / "outcomes.v1.trainable.rebuilt.jsonl"
REJECTS_OUT = ROOT / "state" / "ai_events" / "outcomes.v1.rejects.rebuilt.jsonl"

CUTOVER_PATH = ROOT / "state" / "ai_events" / "training_cutover.json"

BAD_SETUP_TYPES = {"", "unknown", "test_manual", "manual_test"}
BAD_TRADE_PREFIXES = ("PIPE_", "TEST_", "THIS_IS_NOT_REAL")


def _load_jsonl(path: Path):
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except Exception:
            continue


def _load_cutover_ts_ms() -> Optional[int]:
    if not CUTOVER_PATH.exists():
        return None
    try:
        j = json.loads(CUTOVER_PATH.read_text(encoding="utf-8", errors="ignore") or "{}")
        ts = j.get("ts_ms")
        if ts is None:
            return None
        return int(ts)
    except Exception:
        return None


def _outcome_ts_ms(o: Dict[str, Any]) -> Optional[int]:
    """
    Prefer closed_ts_ms, fallback to opened_ts_ms. Return None if neither parse.
    """
    for k in ("closed_ts_ms", "opened_ts_ms"):
        v = o.get(k)
        if v is None:
            continue
        try:
            return int(v)
        except Exception:
            continue
    return None


def _setup_ctx_from_row(j: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if str(j.get("event_type") or "") != "setup_context":
        return None
    tid = str(j.get("trade_id") or "").strip()
    if not tid:
        return None

    # Common fields
    symbol = str(j.get("symbol") or "").strip().upper()
    account_label = str(j.get("account_label") or "").strip()
    timeframe = str(j.get("timeframe") or "").strip()
    setup_type = str(j.get("setup_type") or "").strip()

    # Some payloads nest inside payload/features/signal
    payload = j.get("payload") or {}
    if isinstance(payload, dict):
        st2 = payload.get("setup_type")
        tf2 = payload.get("timeframe")
        if (not setup_type) and st2:
            setup_type = str(st2).strip()
        if (not timeframe) and tf2:
            timeframe = str(tf2).strip()

        feats = payload.get("features") or {}
        if isinstance(feats, dict):
            if not symbol and feats.get("symbol"):
                symbol = str(feats.get("symbol")).strip().upper()
            if not timeframe and feats.get("timeframe"):
                timeframe = str(feats.get("timeframe")).strip()
            if not setup_type and feats.get("setup_type"):
                setup_type = str(feats.get("setup_type")).strip()

            sig = feats.get("signal") or {}
            if isinstance(sig, dict):
                if not symbol and sig.get("symbol"):
                    symbol = str(sig.get("symbol")).strip().upper()
                if not timeframe and sig.get("timeframe"):
                    timeframe = str(sig.get("timeframe")).strip()
                if not setup_type and sig.get("setup_type"):
                    setup_type = str(sig.get("setup_type")).strip()

    return {
        "trade_id": tid,
        "symbol": symbol,
        "account_label": account_label,
        "timeframe": timeframe,
        "setup_type": setup_type,
    }


def _reject(fh, row: Dict[str, Any], reason: str, extra: Optional[Dict[str, Any]] = None) -> None:
    out = {"reason": reason, "row": row}
    if extra:
        out["extra"] = extra
    fh.write(json.dumps(out, ensure_ascii=False) + "\n")


def main() -> None:
    ap = argparse.ArgumentParser(prog="rebuild_trainable_from_outcomes_and_setups_v1", add_help=True)
    ap.add_argument("--ignore-cutover", action="store_true", help="Ignore training_cutover.json if present")
    args = ap.parse_args()

    if not OUTCOMES.exists():
        raise SystemExit(f"FAIL: missing outcomes: {OUTCOMES}")
    if not SETUPS.exists():
        raise SystemExit(f"FAIL: missing setups: {SETUPS}")

    cutover_ts_ms: Optional[int] = None
    if not args.ignore_cutover:
        cutover_ts_ms = _load_cutover_ts_ms()

    # Index setup_context rows by trade_id
    setup_map: Dict[str, Dict[str, Any]] = {}
    setup_dupes = 0
    setup_total = 0

    for j in _load_jsonl(SETUPS):
        sc = _setup_ctx_from_row(j)
        if not sc:
            continue
        setup_total += 1
        tid = sc["trade_id"]
        if tid in setup_map:
            setup_dupes += 1
            continue
        setup_map[tid] = sc

    TRAINABLE_OUT.parent.mkdir(parents=True, exist_ok=True)

    stats = Counter()
    reasons = Counter()

    with TRAINABLE_OUT.open("w", encoding="utf-8", newline="\n") as f_ok, REJECTS_OUT.open("w", encoding="utf-8", newline="\n") as f_bad:
        for o in _load_jsonl(OUTCOMES):
            stats["outcomes_total"] += 1

            # Cutover filter (default ON if file exists)
            if cutover_ts_ms is not None:
                ots = _outcome_ts_ms(o)
                if ots is None:
                    reasons["cutover_missing_ts"] += 1
                    _reject(f_bad, o, "cutover_missing_ts", {"cutover_ts_ms": cutover_ts_ms})
                    continue
                if ots < cutover_ts_ms:
                    reasons["cutover_before_ts"] += 1
                    _reject(f_bad, o, "cutover_before_ts", {"cutover_ts_ms": cutover_ts_ms, "outcome_ts_ms": ots})
                    continue

            tid = str(o.get("trade_id") or "").strip()
            if not tid:
                reasons["missing_trade_id"] += 1
                _reject(f_bad, o, "missing_trade_id")
                continue

            if tid.startswith(BAD_TRADE_PREFIXES):
                reasons["bad_trade_prefix"] += 1
                _reject(f_bad, o, "bad_trade_prefix")
                continue

            sc = setup_map.get(tid)
            if not sc:
                reasons["orphan_no_setup_context"] += 1
                _reject(f_bad, o, "orphan_no_setup_context")
                continue

            stats["joined"] += 1

            # Repair fields from setup_context if missing/unknown
            repaired = 0
            o_sym = str(o.get("symbol") or "").strip().upper()
            o_tf = str(o.get("timeframe") or "").strip()
            o_st = str(o.get("setup_type") or "").strip()

            if (not o_sym) and sc.get("symbol"):
                o["symbol"] = sc["symbol"]
                repaired += 1
            if ((not o_tf) or o_tf == "unknown") and sc.get("timeframe"):
                o["timeframe"] = sc["timeframe"]
                repaired += 1
            if ((not o_st) or o_st == "unknown") and sc.get("setup_type"):
                o["setup_type"] = sc["setup_type"]
                repaired += 1

            if repaired:
                stats["repaired_fields"] += repaired

            # Enforce trainable constraints
            st = str(o.get("setup_type") or "").strip()
            tf = str(o.get("timeframe") or "").strip()
            sym = str(o.get("symbol") or "").strip().upper()

            if not sym:
                reasons["empty_symbol_after_repair"] += 1
                _reject(f_bad, o, "empty_symbol_after_repair", {"setup_context": sc})
                continue
            if not tf:
                reasons["empty_timeframe_after_repair"] += 1
                _reject(f_bad, o, "empty_timeframe_after_repair", {"setup_context": sc})
                continue
            if st.lower() in BAD_SETUP_TYPES:
                reasons["bad_setup_type"] += 1
                _reject(f_bad, o, "bad_setup_type", {"setup_type": st, "setup_context": sc})
                continue

            # Attach join context for dashboard/debug (safe; contract ignores extras)
            o["joined_setup_context"] = True
            o["account_label"] = sc.get("account_label") or o.get("account_label") or ""
            f_ok.write(json.dumps(o, ensure_ascii=False) + "\n")
            stats["trainable_final"] += 1

    print("=== TRAINABLE REBUILD v1 ===")
    if cutover_ts_ms is None:
        print("CUTOVER=", "OFF (no file or --ignore-cutover)")
    else:
        print("CUTOVER=", f"ON ts_ms={cutover_ts_ms} path={CUTOVER_PATH}")
    print("SETUP_CONTEXT_ROWS=", setup_total, "SETUP_CONTEXT_DUPES_SKIPPED=", setup_dupes, "SETUP_MAP_SIZE=", len(setup_map))
    print("OUTCOMES_TOTAL=", stats["outcomes_total"])
    print("JOINED=", stats["joined"])
    print("REPAIRED_FIELDS=", stats["repaired_fields"])
    print("TRAINABLE_FINAL=", stats["trainable_final"])
    print("--- REJECTS BY REASON ---")
    for k, v in reasons.most_common():
        print(f"{k}: {v}")
    print("WROTE:", str(TRAINABLE_OUT))
    print("WROTE:", str(REJECTS_OUT))


if __name__ == "__main__":
    main()
