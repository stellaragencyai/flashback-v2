# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional, Iterable, Tuple
from collections import defaultdict

ROOT = Path(__file__).resolve().parents[2]
SETUPS = ROOT / "state" / "ai_events" / "setups.jsonl"
OUTCOMES = ROOT / "state" / "ai_events" / "outcomes.v1.jsonl"
CUTOVER = ROOT / "state" / "ai_events" / "training_cutover.json"

BAD_SETUP_TYPES = {"", "unknown", "test_manual", "manual_test"}
BAD_TRADE_PREFIXES = ("PIPE_", "TEST_", "THIS_IS_NOT_REAL")


def _load_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
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


def _get_cutover_ts_ms() -> int:
    if not CUTOVER.exists():
        return 0
    try:
        j = json.loads(CUTOVER.read_text(encoding="utf-8", errors="ignore") or "{}")
        return int(j.get("ts_ms") or 0)
    except Exception:
        return 0


def _setup_ctx_from_row(j: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if str(j.get("event_type") or "") != "setup_context":
        return None
    tid = str(j.get("trade_id") or "").strip()
    if not tid:
        return None

    symbol = str(j.get("symbol") or "").strip().upper()
    account_label = str(j.get("account_label") or "").strip()
    timeframe = str(j.get("timeframe") or "").strip()
    setup_type = str(j.get("setup_type") or "").strip()

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


def main() -> None:
    cut_ts = _get_cutover_ts_ms()
    print("=== PIPELINE HEALTH (POST-CUTOVER) ===")
    print("CUTOVER_TS_MS=", cut_ts, "PATH=", str(CUTOVER))

    if not SETUPS.exists():
        raise SystemExit(f"FAIL: missing setups: {SETUPS}")
    if not OUTCOMES.exists():
        raise SystemExit(f"FAIL: missing outcomes: {OUTCOMES}")

    # Index setup_context by trade_id (all-time; join by trade_id)
    setup_map: Dict[str, Dict[str, Any]] = {}
    for j in _load_jsonl(SETUPS):
        sc = _setup_ctx_from_row(j)
        if not sc:
            continue
        tid = sc["trade_id"]
        if tid not in setup_map:
            setup_map[tid] = sc

    stats = defaultdict(lambda: {"outcomes": 0, "joined": 0, "unknown": 0, "orphans": 0})
    total = {"outcomes": 0, "joined": 0, "unknown": 0, "orphans": 0}

    for o in _load_jsonl(OUTCOMES):
        # Cutover filter uses outcome ts_ms (writer sets it)
        ts = o.get("ts_ms")
        try:
            ts_ms = int(ts) if ts is not None else 0
        except Exception:
            ts_ms = 0

        if cut_ts and ts_ms < cut_ts:
            continue

        tid = str(o.get("trade_id") or "").strip()
        if not tid:
            continue
        if tid.startswith(BAD_TRADE_PREFIXES):
            continue

        acct = str(o.get("account_label") or "").strip() or "UNKNOWN"
        stats[acct]["outcomes"] += 1
        total["outcomes"] += 1

        sc = setup_map.get(tid)
        if not sc:
            stats[acct]["orphans"] += 1
            total["orphans"] += 1
            continue

        stats[acct]["joined"] += 1
        total["joined"] += 1

        st = str(o.get("setup_type") or "").strip().lower()
        if st in BAD_SETUP_TYPES:
            stats[acct]["unknown"] += 1
            total["unknown"] += 1

    # Print table
    print("")
    hdr = ["ACCOUNT", "outcomes", "joined", "join_pct", "unknown", "unk_pct", "orphans"]
    print("{:<12s} {:>8s} {:>8s} {:>8s} {:>8s} {:>8s} {:>8s}".format(*hdr))
    for acct in sorted(stats.keys()):
        o = stats[acct]["outcomes"]
        j = stats[acct]["joined"]
        u = stats[acct]["unknown"]
        orp = stats[acct]["orphans"]
        join_pct = (100.0 * j / o) if o else 0.0
        unk_pct = (100.0 * u / o) if o else 0.0
        print("{:<12s} {:>8d} {:>8d} {:>7.2f}% {:>8d} {:>7.2f}% {:>8d}".format(
            acct, o, j, join_pct, u, unk_pct, orp
        ))

    print("")
    o = total["outcomes"]; j = total["joined"]; u = total["unknown"]; orp = total["orphans"]
    join_pct = (100.0 * j / o) if o else 0.0
    unk_pct = (100.0 * u / o) if o else 0.0
    print("TOTAL outcomes=", o, "joined=", j, f"join_pct={join_pct:.2f}%", "unknown=", u, f"unk_pct={unk_pct:.2f}%", "orphans=", orp)


if __name__ == "__main__":
    main()
