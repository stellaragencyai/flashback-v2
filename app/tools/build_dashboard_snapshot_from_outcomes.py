#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build dashboard truth snapshot from outcomes.v1.jsonl.

Writes:
  state/dashboard_snapshot.json

Truth source: outcomes.v1.jsonl (not ops_snapshot.json).
Adds:
- expected_accounts + missing_accounts
- normalization (mode/timeframe/setup)
- freshness_sec per account
"""
from __future__ import annotations

import json
import os
import time
from collections import defaultdict, Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List

ROOT = Path(__file__).resolve().parents[2]
OUTCOMES = ROOT / "state" / "ai_events" / "outcomes.v1.jsonl"
OUT_SNAPSHOT = ROOT / "state" / "dashboard_snapshot.json"

def _now_ms() -> int:
    return int(time.time() * 1000)

def _safe_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

def _ms_to_iso(ts_ms: int | None) -> str | None:
    if not ts_ms:
        return None
    try:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts_ms / 1000.0))
    except Exception:
        return None

def _norm_mode(x: Any) -> str:
    s = str(x or "unknown").strip()
    if not s:
        return "unknown"
    return s.upper()

def _norm_timeframe(x: Any) -> str:
    s = str(x or "unknown").strip()
    if not s:
        return "unknown"
    # normalize bare numbers like "5" -> "5m"
    if s.isdigit():
        return f"{s}m"
    return s

def _norm_setup(x: Any) -> str:
    s = str(x or "unknown").strip().lower()
    return s or "unknown"

def _read_jsonl(path: Path, max_lines: int | None = None) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        return []
    if max_lines is not None and max_lines > 0:
        try:
            data = path.read_bytes()
            lines = data.splitlines()[-max_lines:]
            out: List[Dict[str, Any]] = []
            for b in lines:
                try:
                    out.append(json.loads(b.decode("utf-8", errors="replace")))
                except Exception:
                    continue
            return out
        except Exception:
            pass

    def _gen():
        with path.open("r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except Exception:
                    continue
    return _gen()

def _default_agg() -> Dict[str, Any]:
    return {
        "n": 0,
        "n_win": 0,
        "n_loss": 0,
        "pnl_sum": 0.0,
        "fees_sum": 0.0,
        "last_ts_ms": 0,
        "symbols": Counter(),
        "setups": Counter(),
        "timeframes": Counter(),
        "modes": Counter(),
        "close_reasons": Counter(),
    }

def _finalize(obj: Dict[str, Any], now_ms: int) -> Dict[str, Any]:
    n = int(obj["n"])
    n_win = int(obj["n_win"])
    pnl_sum = float(obj["pnl_sum"])
    fees_sum = float(obj["fees_sum"])
    winrate = (n_win / n) if n > 0 else 0.0
    avg_pnl = (pnl_sum / n) if n > 0 else 0.0
    last_ts_ms = int(obj["last_ts_ms"])
    freshness_sec = round(((now_ms - last_ts_ms) / 1000.0), 3) if last_ts_ms > 0 else None

    return {
        "n": n,
        "n_win": n_win,
        "n_loss": int(obj["n_loss"]),
        "winrate": round(winrate, 6),
        "pnl_sum": round(pnl_sum, 8),
        "fees_sum": round(fees_sum, 8),
        "avg_pnl": round(avg_pnl, 8),
        "last_ts_ms": last_ts_ms,
        "last_ts_iso": _ms_to_iso(last_ts_ms),
        "freshness_sec": freshness_sec,
        "top_symbols": obj["symbols"].most_common(10),
        "top_setups": obj["setups"].most_common(10),
        "top_timeframes": obj["timeframes"].most_common(10),
        "top_modes": obj["modes"].most_common(10),
        "top_close_reasons": obj["close_reasons"].most_common(10),
    }

def build_snapshot() -> Dict[str, Any]:
    now_ms = _now_ms()
    max_lines = int(os.getenv("DASH_OUTCOMES_MAX_LINES", "0") or "0")
    rows = _read_jsonl(OUTCOMES, max_lines=max_lines if max_lines > 0 else None)

    per_acc: Dict[str, Dict[str, Any]] = defaultdict(_default_agg)
    global_stats = _default_agg()

    for r in rows:
        if str(r.get("schema_version", "")).strip() != "outcome.v1":
            continue

        acc = str(r.get("account_label") or "unknown").strip() or "unknown"
        sym = str(r.get("symbol") or "UNKNOWN").strip() or "UNKNOWN"
        setup = _norm_setup(r.get("setup_type"))
        tf = _norm_timeframe(r.get("timeframe"))
        mode = _norm_mode(r.get("mode"))
        close_reason = str(r.get("close_reason") or "unknown").strip().lower() or "unknown"

        pnl = _safe_float(r.get("pnl_usd"))
        fees = _safe_float(r.get("fees_usd"))
        ts_ms = int(r.get("ts_ms") or 0)

        rec = per_acc[acc]
        rec["n"] += 1
        global_stats["n"] += 1

        rec["pnl_sum"] += pnl
        rec["fees_sum"] += fees
        global_stats["pnl_sum"] += pnl
        global_stats["fees_sum"] += fees

        if pnl > 0:
            rec["n_win"] += 1
            global_stats["n_win"] += 1
        elif pnl < 0:
            rec["n_loss"] += 1
            global_stats["n_loss"] += 1

        rec["symbols"][sym] += 1
        rec["setups"][setup] += 1
        rec["timeframes"][tf] += 1
        rec["modes"][mode] += 1
        rec["close_reasons"][close_reason] += 1

        global_stats["symbols"][sym] += 1
        global_stats["setups"][setup] += 1
        global_stats["timeframes"][tf] += 1
        global_stats["modes"][mode] += 1
        global_stats["close_reasons"][close_reason] += 1

        if ts_ms and ts_ms > int(rec["last_ts_ms"]):
            rec["last_ts_ms"] = ts_ms
        if ts_ms and ts_ms > int(global_stats["last_ts_ms"]):
            global_stats["last_ts_ms"] = ts_ms

    expected = ["main"] + [f"flashback{str(i).zfill(2)}" for i in range(1, 11)]
    present = sorted([k for k in per_acc.keys() if k and k != "unknown"])
    missing = [a for a in expected if a not in per_acc]

    accounts_out: Dict[str, Any] = {}
    for acc in sorted(per_acc.keys()):
        accounts_out[acc] = _finalize(per_acc[acc], now_ms)

    snapshot = {
        "schema_version": 2,
        "updated_ms": now_ms,
        "source": {
            "outcomes_path": str(OUTCOMES),
            "outcomes_exists": OUTCOMES.exists(),
            "outcomes_size": OUTCOMES.stat().st_size if OUTCOMES.exists() else 0,
        },
        "expected_accounts": expected,
        "present_accounts": present,
        "missing_accounts": missing,
        "global": _finalize(global_stats, now_ms),
        "accounts": accounts_out,
    }
    return snapshot

def main() -> int:
    snap = build_snapshot()
    OUT_SNAPSHOT.parent.mkdir(parents=True, exist_ok=True)
    OUT_SNAPSHOT.write_text(json.dumps(snap, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"OK wrote {OUT_SNAPSHOT} (present={len(snap.get('present_accounts', []))}, missing={len(snap.get('missing_accounts', []))})")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
