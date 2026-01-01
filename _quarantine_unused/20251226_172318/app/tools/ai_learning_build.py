#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Learning Builder v1.3 (Phase 6)

Reads (read-only):
- state/ai_memory/memory_index.sqlite (table: memory_entries)

Writes (derived, rebuildable):
- state/ai_learning/learning.sqlite (table: memory_stats_v1)
- state/ai_learning/memory_stats_v1.jsonl (deterministic ordering)
- state/ai_learning/drift_report_v1.json

Hard rules:
- NEVER mutate Phase 5 memory
- Policy isolation: stats are scoped by policy_hash (no silent mixing)
- Deterministic outputs: same inputs -> same JSONL bytes
- Advisory only: no execution hooks

IMPORTANT: Learning grain (fix)
------------------------------
Phase 6 actionable learning MUST aggregate at a bucket level that can accumulate samples.

We aggregate by:
  (policy_hash, symbol, timeframe, setup_type)

We still emit a "memory_fingerprint" column for backward compatibility, but it is now
a deterministic bucket_id (sha256 over the bucket key). It is NOT the Phase 5 per-trade
memory_fingerprint, because that atomizes the data into useless 1–3 sample groups.

Determinism Note
----------------
DO NOT use wall-clock time for built_ts_ms.
built_ts_ms MUST be derived from input memory so repeated builds are byte-identical.
We define built_ts_ms = MAX(ts_ms) over the filtered memory_entries.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from app.ai.ai_memory_contract import ContractPaths
from app.ai.ai_learning_contract import LearningPaths, LEARNING_SCHEMA_VERSION


# -------------------------
# Config (Phase 6 constants)
# -------------------------

DEFAULT_MIN_TRADES = 30
DEFAULT_TRIM_PCT = 0.10

# Drift windows (trade-count based, deterministic)
DEFAULT_RECENT_N = 30
DEFAULT_LONG_N = 200

# Drift thresholds (conservative)
DEFAULT_DRIFT_WINRATE_DROP = 0.15   # recent winrate worse by 15%+
DEFAULT_DRIFT_MEANR_DROP = 0.50     # recent mean R worse by 0.5R+


# -------------------------
# Utilities
# -------------------------

def _exists_nonempty(p: Path) -> bool:
    try:
        return p.exists() and p.stat().st_size > 0
    except Exception:
        return False


def _connect_ro(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _connect_rw(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    return conn


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
        return None


def _median(sorted_vals: List[float]) -> Optional[float]:
    n = len(sorted_vals)
    if n == 0:
        return None
    mid = n // 2
    if n % 2 == 1:
        return float(sorted_vals[mid])
    return float((sorted_vals[mid - 1] + sorted_vals[mid]) / 2.0)


def _mad(sorted_vals: List[float], med: float) -> Optional[float]:
    if not sorted_vals:
        return None
    dev = [abs(v - med) for v in sorted_vals]
    dev.sort()
    return _median(dev)


def _trimmed(sorted_vals: List[float], trim_pct: float) -> Optional[float]:
    n = len(sorted_vals)
    if n == 0:
        return None
    if n < 5:
        return float(sum(sorted_vals) / n)
    k = int(math.floor(n * trim_pct))
    if k * 2 >= n:
        return float(sum(sorted_vals) / n)
    trimmed = sorted_vals[k : (n - k)]
    if not trimmed:
        return float(sum(sorted_vals) / n)
    return float(sum(trimmed) / len(trimmed))


def _winsorized(sorted_vals: List[float], winsor_pct: float) -> Optional[float]:
    n = len(sorted_vals)
    if n == 0:
        return None
    if n < 5:
        return float(sum(sorted_vals) / n)
    k = int(math.floor(n * winsor_pct))
    if k * 2 >= n:
        return float(sum(sorted_vals) / n)
    lo = sorted_vals[k]
    hi = sorted_vals[n - k - 1]
    adj = sorted_vals[:]
    for i in range(0, k):
        adj[i] = lo
    for i in range(n - k, n):
        adj[i] = hi
    return float(sum(adj) / n)


def _confidence(n: int) -> Tuple[str, float]:
    """
    Deterministic, pessimistic confidence.
    Returns (state, confidence_0_1)
    """
    if n < DEFAULT_MIN_TRADES:
        return ("INSUFFICIENT_DATA", 0.0)
    # Simple saturating curve: ~0.5 at 60 trades, ~0.8 at 120, ~0.95 at 240
    conf = 1.0 - math.exp(-(n / 120.0))
    conf = max(0.0, min(1.0, conf))
    if n < 100:
        return ("LOW_CONFIDENCE", conf)
    if n < 200:
        return ("MED_CONFIDENCE", conf)
    return ("HIGH_CONFIDENCE", conf)


def _bucket_id(policy_hash: str, symbol: str, timeframe: str, setup_type: str) -> str:
    """
    Deterministic bucket id (stable across runs, stable across machines).

    We intentionally do NOT use Phase 5 memory_fingerprint as the key here,
    because that yields near-unique rows and kills confidence.
    """
    payload = {
        "policy_hash": policy_hash or "",
        "symbol": symbol or "",
        "timeframe": timeframe or "",
        "setup_type": setup_type or "",
        "schema_version": int(LEARNING_SCHEMA_VERSION),
        "grain": "policy|symbol|timeframe|setup_type",
    }
    b = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return _sha256_bytes(b)


# -------------------------
# Aggregation
# -------------------------

@dataclass
class StatRow:
    schema_version: int
    built_ts_ms: int

    policy_hash: str
    memory_fingerprint: str  # backward compat: now bucket_id
    symbol: str
    timeframe: str
    setup_type: str

    n: int
    wins: int
    losses: int
    win_rate: Optional[float]

    mean_r: Optional[float]
    median_r: Optional[float]
    mad_r: Optional[float]
    trimmed_mean_r: Optional[float]
    winsor_mean_r: Optional[float]

    expectancy_r: Optional[float]
    variance_r: Optional[float]

    first_ts_ms: int
    last_ts_ms: int

    confidence_state: str
    confidence: float

    # drift fields
    recent_n: int
    recent_win_rate: Optional[float]
    recent_mean_r: Optional[float]
    drift_flag: bool
    drift_reason: str


def _fetch_memory_rows(
    conn: sqlite3.Connection,
    *,
    policy_hash: Optional[str] = None,
) -> Iterable[sqlite3.Row]:
    """
    Fetch memory rows in deterministic order at the grain needed for Phase 6.

    Note: We do NOT require memory_fingerprint here. Phase 6 should be able to
    learn even if Phase 5 fingerprint is missing (as long as setup_type exists).
    """
    where = ["1=1"]
    params: Dict[str, Any] = {}
    if policy_hash:
        where.append("policy_hash = :policy_hash")
        params["policy_hash"] = policy_hash

    sql = f"""
    SELECT
      ts_ms, policy_hash,
      symbol, timeframe, setup_type,
      r_multiple, win, trade_id
    FROM memory_entries
    WHERE {' AND '.join(where)}
    ORDER BY policy_hash ASC, symbol ASC, timeframe ASC, COALESCE(setup_type,'') ASC, ts_ms ASC, trade_id ASC;
    """
    cur = conn.cursor()
    cur.execute(sql, params)
    for row in cur:
        yield row


def _group_key(r: sqlite3.Row) -> Tuple[str, str, str, str]:
    """
    Actionable Phase 6 learning grain:
      (policy_hash, symbol, timeframe, setup_type)
    """
    return (
        str(r["policy_hash"] or ""),
        str(r["symbol"] or ""),
        str(r["timeframe"] or ""),
        str(r["setup_type"] or ""),
    )


def _deterministic_built_ts_ms(conn: sqlite3.Connection, policy_hash: Optional[str]) -> int:
    where = ["1=1"]
    params: Dict[str, Any] = {}
    if policy_hash:
        where.append("policy_hash = :policy_hash")
        params["policy_hash"] = policy_hash
    cur = conn.cursor()
    cur.execute(f"SELECT MAX(ts_ms) AS m FROM memory_entries WHERE {' AND '.join(where)};", params)
    row = cur.fetchone()
    m = row[0] if row else None
    try:
        return int(m) if m is not None else 0
    except Exception:
        return 0


def build_learning_rows(
    *,
    memory_index_path: Path,
    trim_pct: float = DEFAULT_TRIM_PCT,
    recent_n: int = DEFAULT_RECENT_N,
    long_n: int = DEFAULT_LONG_N,
    drift_winrate_drop: float = DEFAULT_DRIFT_WINRATE_DROP,
    drift_meanr_drop: float = DEFAULT_DRIFT_MEANR_DROP,
    policy_hash_filter: Optional[str] = None,
) -> Tuple[List[StatRow], Dict[str, Any]]:
    if not _exists_nonempty(memory_index_path):
        return ([], {"ok": False, "reason": f"missing_or_empty_memory_index: {memory_index_path}"})

    conn = _connect_ro(memory_index_path)
    built_ts_ms = _deterministic_built_ts_ms(conn, policy_hash_filter)
    rows_iter = _fetch_memory_rows(conn, policy_hash=policy_hash_filter)

    out: List[StatRow] = []
    drift_events: List[Dict[str, Any]] = []

    current_key: Optional[Tuple[str, str, str, str]] = None
    buf: List[Tuple[int, Optional[float], Optional[int]]] = []  # (ts_ms, r, win01)

    def flush() -> None:
        nonlocal buf, current_key, out, drift_events

        if not current_key or not buf:
            buf = []
            return

        policy_hash, symbol, timeframe, setup_type = current_key
        bucket_fp = _bucket_id(policy_hash, symbol, timeframe, setup_type)

        ts_list = [t for (t, _, _) in buf if t is not None]
        r_list = [r for (_, r, _) in buf if r is not None]
        w_list = [w for (_, _, w) in buf if w is not None]

        n = len(buf)
        wins = sum(1 for w in w_list if w == 1)
        losses = sum(1 for w in w_list if w == 0)
        win_rate = (wins / len(w_list)) if w_list else None

        first_ts_ms = int(min(ts_list)) if ts_list else 0
        last_ts_ms = int(max(ts_list)) if ts_list else 0

        mean_r = (sum(r_list) / len(r_list)) if r_list else None

        variance_r = None
        if r_list and len(r_list) >= 2:
            mu = sum(r_list) / len(r_list)
            variance_r = sum((x - mu) ** 2 for x in r_list) / (len(r_list) - 1)

        r_sorted = sorted(r_list)
        median_r = _median(r_sorted) if r_sorted else None
        mad_r = _mad(r_sorted, median_r) if (r_sorted and median_r is not None) else None
        trimmed_mean_r = _trimmed(r_sorted, trim_pct) if r_sorted else None
        winsor_mean_r = _winsorized(r_sorted, trim_pct) if r_sorted else None

        expectancy_r = mean_r

        conf_state, conf = _confidence(n)

        drift_flag = False
        drift_reason = ""

        recent_slice = buf[-recent_n:] if len(buf) >= recent_n else []
        long_slice = buf[-long_n:] if len(buf) >= min(long_n, recent_n) else buf[:]

        def slice_stats(sl: List[Tuple[int, Optional[float], Optional[int]]]) -> Tuple[int, Optional[float], Optional[float]]:
            if not sl:
                return (0, None, None)
            rr = [r for (_, r, _) in sl if r is not None]
            ww = [w for (_, _, w) in sl if w is not None]
            cn = len(sl)
            wr = (sum(1 for w in ww if w == 1) / len(ww)) if ww else None
            mr = (sum(rr) / len(rr)) if rr else None
            return (cn, wr, mr)

        recent_cnt, recent_wr, recent_mr = slice_stats(recent_slice)
        long_cnt, long_wr, long_mr = slice_stats(long_slice)

        if recent_cnt >= DEFAULT_MIN_TRADES and long_cnt >= DEFAULT_MIN_TRADES and conf_state != "INSUFFICIENT_DATA":
            wr_drop = (long_wr - recent_wr) if (recent_wr is not None and long_wr is not None) else None
            mr_drop = (long_mr - recent_mr) if (recent_mr is not None and long_mr is not None) else None

            reasons = []
            if wr_drop is not None and wr_drop >= drift_winrate_drop:
                reasons.append(f"winrate_drop={wr_drop:.3f}")
            if mr_drop is not None and mr_drop >= drift_meanr_drop:
                reasons.append(f"meanR_drop={mr_drop:.3f}")

            if reasons:
                drift_flag = True
                drift_reason = ",".join(reasons)
                drift_events.append(
                    {
                        "policy_hash": policy_hash,
                        "memory_fingerprint": bucket_fp,  # compat: bucket id
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "setup_type": setup_type,
                        "n": n,
                        "recent_n": recent_cnt,
                        "recent_win_rate": recent_wr,
                        "recent_mean_r": recent_mr,
                        "baseline_n": long_cnt,
                        "baseline_win_rate": long_wr,
                        "baseline_mean_r": long_mr,
                        "reason": drift_reason,
                        "last_ts_ms": last_ts_ms,
                    }
                )

        out.append(
            StatRow(
                schema_version=LEARNING_SCHEMA_VERSION,
                built_ts_ms=built_ts_ms,
                policy_hash=policy_hash,
                memory_fingerprint=bucket_fp,
                symbol=symbol,
                timeframe=timeframe,
                setup_type=setup_type,
                n=n,
                wins=wins,
                losses=losses,
                win_rate=win_rate,
                mean_r=mean_r,
                median_r=median_r,
                mad_r=mad_r,
                trimmed_mean_r=trimmed_mean_r,
                winsor_mean_r=winsor_mean_r,
                expectancy_r=expectancy_r,
                variance_r=variance_r,
                first_ts_ms=first_ts_ms,
                last_ts_ms=last_ts_ms,
                confidence_state=conf_state,
                confidence=conf,
                recent_n=recent_cnt,
                recent_win_rate=recent_wr,
                recent_mean_r=recent_mr,
                drift_flag=drift_flag,
                drift_reason=drift_reason,
            )
        )

        buf = []

    for r in rows_iter:
        key = _group_key(r)
        if current_key is None:
            current_key = key

        if key != current_key:
            flush()
            current_key = key

        ts_ms = _safe_int(r["ts_ms"]) or 0
        rr = _safe_float(r["r_multiple"])
        win01 = _safe_int(r["win"])
        if win01 not in (0, 1, None):
            win01 = None

        buf.append((ts_ms, rr, win01))

    flush()
    conn.close()

    meta = {
        "ok": True,
        "built_ts_ms": built_ts_ms,
        "rows": len(out),
        "drift_events": len(drift_events),
        "schema_version": LEARNING_SCHEMA_VERSION,
        "policy_hash_filter": policy_hash_filter,
        "grain": "policy|symbol|timeframe|setup_type",
    }

    drift_report = {
        "schema_version": LEARNING_SCHEMA_VERSION,
        "built_ts_ms": built_ts_ms,
        "recent_n": recent_n,
        "long_n": long_n,
        "winrate_drop_threshold": drift_winrate_drop,
        "meanR_drop_threshold": drift_meanr_drop,
        "events": drift_events,
    }

    meta["drift_report"] = drift_report
    return (out, meta)


# -------------------------
# Persistence
# -------------------------

def _init_learning_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS memory_stats_v1 (
            policy_hash TEXT NOT NULL,
            memory_fingerprint TEXT NOT NULL,   -- compat: bucket id
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            setup_type TEXT NOT NULL,

            n INTEGER NOT NULL,
            wins INTEGER NOT NULL,
            losses INTEGER NOT NULL,
            win_rate REAL,

            mean_r REAL,
            median_r REAL,
            mad_r REAL,
            trimmed_mean_r REAL,
            winsor_mean_r REAL,
            expectancy_r REAL,
            variance_r REAL,

            first_ts_ms INTEGER NOT NULL,
            last_ts_ms INTEGER NOT NULL,

            confidence_state TEXT NOT NULL,
            confidence REAL NOT NULL,

            recent_n INTEGER NOT NULL,
            recent_win_rate REAL,
            recent_mean_r REAL,
            drift_flag INTEGER NOT NULL,
            drift_reason TEXT,

            schema_version INTEGER NOT NULL,
            built_ts_ms INTEGER NOT NULL,

            PRIMARY KEY (policy_hash, memory_fingerprint, symbol, timeframe, setup_type)
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ms_mfp ON memory_stats_v1(memory_fingerprint);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ms_policy ON memory_stats_v1(policy_hash);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ms_sym_tf ON memory_stats_v1(symbol, timeframe);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ms_setup ON memory_stats_v1(setup_type);")
    conn.commit()


def write_learning_artifacts(
    *,
    learning_sqlite_path: Path,
    jsonl_path: Path,
    drift_json_path: Path,
    rows: List[StatRow],
    drift_report: Dict[str, Any],
) -> Dict[str, Any]:
    # Fresh outputs for determinism
    for p in (learning_sqlite_path, jsonl_path, drift_json_path):
        if p.exists():
            try:
                p.unlink()
            except Exception:
                pass

    conn = _connect_rw(learning_sqlite_path)
    _init_learning_db(conn)

    cur = conn.cursor()
    ins = 0
    for r in rows:
        cur.execute(
            """
            INSERT OR REPLACE INTO memory_stats_v1 (
              policy_hash, memory_fingerprint, symbol, timeframe, setup_type,
              n, wins, losses, win_rate,
              mean_r, median_r, mad_r, trimmed_mean_r, winsor_mean_r, expectancy_r, variance_r,
              first_ts_ms, last_ts_ms,
              confidence_state, confidence,
              recent_n, recent_win_rate, recent_mean_r, drift_flag, drift_reason,
              schema_version, built_ts_ms
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
            """,
            (
                r.policy_hash,
                r.memory_fingerprint,
                r.symbol,
                r.timeframe,
                r.setup_type,
                int(r.n),
                int(r.wins),
                int(r.losses),
                r.win_rate,
                r.mean_r,
                r.median_r,
                r.mad_r,
                r.trimmed_mean_r,
                r.winsor_mean_r,
                r.expectancy_r,
                r.variance_r,
                int(r.first_ts_ms),
                int(r.last_ts_ms),
                r.confidence_state,
                float(r.confidence),
                int(r.recent_n),
                r.recent_win_rate,
                r.recent_mean_r,
                1 if r.drift_flag else 0,
                r.drift_reason or "",
                int(r.schema_version),
                int(r.built_ts_ms),
            ),
        )
        ins += 1

    conn.commit()
    conn.close()

    def row_to_dict(sr: StatRow) -> Dict[str, Any]:
        return {
            "schema_version": sr.schema_version,
            "built_ts_ms": sr.built_ts_ms,
            "policy_hash": sr.policy_hash,
            "memory_fingerprint": sr.memory_fingerprint,  # bucket id
            "symbol": sr.symbol,
            "timeframe": sr.timeframe,
            "setup_type": sr.setup_type,
            "n": sr.n,
            "wins": sr.wins,
            "losses": sr.losses,
            "win_rate": sr.win_rate,
            "mean_r": sr.mean_r,
            "median_r": sr.median_r,
            "mad_r": sr.mad_r,
            "trimmed_mean_r": sr.trimmed_mean_r,
            "winsor_mean_r": sr.winsor_mean_r,
            "expectancy_r": sr.expectancy_r,
            "variance_r": sr.variance_r,
            "first_ts_ms": sr.first_ts_ms,
            "last_ts_ms": sr.last_ts_ms,
            "confidence_state": sr.confidence_state,
            "confidence": sr.confidence,
            "recent_n": sr.recent_n,
            "recent_win_rate": sr.recent_win_rate,
            "recent_mean_r": sr.recent_mean_r,
            "drift_flag": bool(sr.drift_flag),
            "drift_reason": sr.drift_reason,
            "grain": "policy|symbol|timeframe|setup_type",
        }

    # Deterministic JSONL ordering:
    rows_sorted = sorted(
        rows,
        key=lambda sr: (
            sr.policy_hash,
            sr.symbol,
            sr.timeframe,
            sr.setup_type,
            sr.last_ts_ms,
            sr.memory_fingerprint,
        ),
    )

    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    with jsonl_path.open("wb") as f:
        for sr in rows_sorted:
            line = json.dumps(row_to_dict(sr), separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            f.write(line + b"\n")

    drift_json_path.parent.mkdir(parents=True, exist_ok=True)
    drift_json_path.write_text(json.dumps(drift_report, indent=2, sort_keys=True), encoding="utf-8")

    jsonl_bytes = jsonl_path.read_bytes()
    return {
        "ok": True,
        "inserted": ins,
        "learning_sqlite": str(learning_sqlite_path),
        "memory_stats_jsonl": str(jsonl_path),
        "drift_report_json": str(drift_json_path),
        "jsonl_sha256": _sha256_bytes(jsonl_bytes),
        "jsonl_bytes": len(jsonl_bytes),
    }


def build_and_write(
    *,
    trim_pct: float = DEFAULT_TRIM_PCT,
    recent_n: int = DEFAULT_RECENT_N,
    long_n: int = DEFAULT_LONG_N,
    drift_winrate_drop: float = DEFAULT_DRIFT_WINRATE_DROP,
    drift_meanr_drop: float = DEFAULT_DRIFT_MEANR_DROP,
    policy_hash_filter: Optional[str] = None,
) -> Dict[str, Any]:
    mpaths = ContractPaths.default()
    lpaths = LearningPaths.default()

    rows, meta = build_learning_rows(
        memory_index_path=mpaths.memory_index_path,
        trim_pct=trim_pct,
        recent_n=recent_n,
        long_n=long_n,
        drift_winrate_drop=drift_winrate_drop,
        drift_meanr_drop=drift_meanr_drop,
        policy_hash_filter=policy_hash_filter,
    )

    if not meta.get("ok"):
        return meta

    drift_report = meta.get("drift_report", {})
    write_stats = write_learning_artifacts(
        learning_sqlite_path=lpaths.learning_sqlite_path,
        jsonl_path=lpaths.memory_stats_jsonl_path,
        drift_json_path=lpaths.drift_report_json_path,
        rows=rows,
        drift_report=drift_report,
    )

    return {
        **meta,
        **write_stats,
        "learning_rows": len(rows),
    }


# -------------------------
# CLI (THIS WAS MISSING 🙃)
# -------------------------

def _clamp_float(x: float, lo: float, hi: float) -> float:
    try:
        x = float(x)
    except Exception:
        return lo
    return max(lo, min(hi, x))


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Flashback Phase 6: build AI learning artifacts from Phase 5 memory_index.sqlite")
    ap.add_argument("--rebuild", action="store_true", help="Rebuild learning artifacts (default behavior)")
    ap.add_argument("--trim-pct", type=float, default=DEFAULT_TRIM_PCT, help="Trim/Winsor pct (default 0.10)")
    ap.add_argument("--recent-n", type=int, default=DEFAULT_RECENT_N, help="Recent window trade count (default 30)")
    ap.add_argument("--long-n", type=int, default=DEFAULT_LONG_N, help="Long window trade count (default 200)")
    ap.add_argument("--drift-winrate-drop", type=float, default=DEFAULT_DRIFT_WINRATE_DROP, help="Winrate drop threshold (default 0.15)")
    ap.add_argument("--drift-meanr-drop", type=float, default=DEFAULT_DRIFT_MEANR_DROP, help="Mean R drop threshold (default 0.50)")
    ap.add_argument("--policy-hash", type=str, default="", help="Optional: restrict to a single policy_hash")
    args = ap.parse_args(argv)

    trim_pct = _clamp_float(args.trim_pct, 0.0, 0.49)
    recent_n = max(1, int(args.recent_n or DEFAULT_RECENT_N))
    long_n = max(recent_n, int(args.long_n or DEFAULT_LONG_N))
    drift_wr = _clamp_float(args.drift_winrate_drop, 0.0, 1.0)
    drift_mr = float(args.drift_meanr_drop)

    policy = args.policy_hash.strip() or None

    print("=== AI Learning Build v1.3 (Phase 6) ===")
    print(f"trim_pct : {trim_pct}")
    print(f"recent_n : {recent_n}")
    print(f"long_n   : {long_n}")
    print(f"policy   : {policy[:12] + '...' if policy else 'ANY'}")

    try:
        out = build_and_write(
            trim_pct=trim_pct,
            recent_n=recent_n,
            long_n=long_n,
            drift_winrate_drop=drift_wr,
            drift_meanr_drop=drift_mr,
            policy_hash_filter=policy,
        )
    except Exception as e:
        print("FAIL ❌ exception during build:")
        print(f"  {type(e).__name__}: {e}")
        return 2

    if not out.get("ok"):
        print("FAIL ❌ build returned not ok:")
        for k, v in out.items():
            print(f"  {k}: {v}")
        return 1

    lp = LearningPaths.default()
    dbp = Path(lp.learning_sqlite_path)
    size = dbp.stat().st_size if dbp.exists() else 0

    print("OK ✅ build complete")
    print(f"rows      : {out.get('rows')}")
    print(f"inserted  : {out.get('inserted')}")
    print(f"drift_ev  : {out.get('drift_events')}")
    print(f"built_ts  : {out.get('built_ts_ms')}")
    print(f"db_path   : {dbp}")
    print(f"db_bytes  : {size}")
    print(f"jsonl_sha : {out.get('jsonl_sha256')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
