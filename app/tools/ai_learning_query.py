#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Learning Query Tool v1.1 (Phase 6)

v1.1
- Adds sample-size gating defaults for meaningful rankings:
    --min-trades (default 30)
    --include-insufficient (default false)
- "best" sort prioritizes winsor_mean_r + confidence + n + recency
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from app.ai.ai_learning_contract import LearningPaths
from app.ai.ai_memory_contract import normalize_symbol, normalize_timeframe


def _exists_nonempty(p: Path) -> bool:
    try:
        return p.exists() and p.stat().st_size > 0
    except Exception:
        return False


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _build_where(
    *,
    symbol: Optional[str],
    timeframe: Optional[str],
    setup_type: Optional[str],
    policy_hash: Optional[str],
    memory_fp_prefix: Optional[str],
    min_trades: int,
    include_insufficient: bool,
) -> Tuple[str, Dict[str, Any]]:
    clauses = []
    params: Dict[str, Any] = {}

    if symbol:
        clauses.append("symbol = :symbol")
        params["symbol"] = symbol
    if timeframe:
        clauses.append("timeframe = :timeframe")
        params["timeframe"] = timeframe
    if setup_type:
        clauses.append("setup_type = :setup_type")
        params["setup_type"] = setup_type
    if policy_hash:
        clauses.append("policy_hash = :policy_hash")
        params["policy_hash"] = policy_hash
    if memory_fp_prefix:
        clauses.append("memory_fingerprint LIKE :mfp")
        params["mfp"] = memory_fp_prefix + "%"

    # Default gating: exclude insufficient rows unless explicitly asked
    if not include_insufficient:
        clauses.append("n >= :min_trades")
        clauses.append("confidence_state <> 'INSUFFICIENT_DATA'")
        params["min_trades"] = int(min_trades)

    if not clauses:
        return ("1=1", params)
    return (" AND ".join(clauses), params)


def _pct(x: Any) -> str:
    if x is None:
        return "n/a"
    try:
        return f"{float(x) * 100:.1f}%"
    except Exception:
        return "n/a"


def _fmt(x: Any) -> str:
    if x is None:
        return "n/a"
    try:
        if isinstance(x, (int, float)):
            return f"{x:.4f}".rstrip("0").rstrip(".")
    except Exception:
        pass
    return str(x)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", type=str, default="", help="Symbol filter (optional)")
    ap.add_argument("--timeframe", type=str, default="", help="Timeframe filter (optional)")
    ap.add_argument("--setup-type", type=str, default="", help="Setup type filter (optional)")
    ap.add_argument("--policy-hash", type=str, default="", help="Policy hash filter (optional)")
    ap.add_argument("--memory-fp", type=str, default="", help="Memory fingerprint prefix filter (optional)")
    ap.add_argument("--sort", type=str, default="recent", choices=["recent", "best"], help="Sort order: recent|best")
    ap.add_argument("--limit", type=int, default=15, help="Rows to print (default 15, max 200)")
    ap.add_argument("--min-trades", type=int, default=30, help="Minimum trades for ranking (default 30)")
    ap.add_argument("--include-insufficient", action="store_true", help="Include insufficient-data rows")
    args = ap.parse_args()

    lp = LearningPaths.default()
    db_path = lp.learning_sqlite_path

    if not _exists_nonempty(db_path):
        print("FAIL ❌ learning.sqlite missing or empty:")
        print(f"  - {db_path}")
        print("Fix: build Phase 6 learning first:")
        print("  python -m app.tools.ai_learning_build --rebuild")
        return

    symbol = normalize_symbol(args.symbol) if args.symbol.strip() else None
    timeframe = normalize_timeframe(args.timeframe) if args.timeframe.strip() else None
    setup_type = args.setup_type.strip() or None
    policy_hash = args.policy_hash.strip() or None
    mfp = args.memory_fp.strip() or None

    limit = int(args.limit or 15)
    if limit < 1:
        limit = 1
    if limit > 200:
        limit = 200

    where_sql, params = _build_where(
        symbol=symbol,
        timeframe=timeframe,
        setup_type=setup_type,
        policy_hash=policy_hash,
        memory_fp_prefix=mfp,
        min_trades=int(args.min_trades),
        include_insufficient=bool(args.include_insufficient),
    )

    order_sql = "last_ts_ms DESC"
    if args.sort == "best":
        order_sql = "winsor_mean_r DESC, confidence DESC, n DESC, last_ts_ms DESC"

    conn = _connect(db_path)
    cur = conn.cursor()

    cur.execute(f"SELECT COUNT(*) AS n FROM memory_stats_v1 WHERE {where_sql};", params)
    total = int(cur.fetchone()["n"])

    print("=== AI Learning Query v1.1 (Phase 6) ===")
    print(f"db      : {db_path}")
    print(
        "filter  : "
        f"symbol={symbol or 'ANY'}, "
        f"timeframe={timeframe or 'ANY'}, "
        f"setup_type={setup_type or 'ANY'}, "
        f"policy_hash={(policy_hash[:12] + '...') if policy_hash else 'ANY'}, "
        f"memory_fp={(mfp + '...') if mfp else 'ANY'}, "
        f"min_trades={(args.min_trades if not args.include_insufficient else 'OFF')}"
    )
    print(f"sort    : {args.sort}")
    print(f"matched : {total}")

    if total == 0:
        conn.close()
        print("DONE")
        return

    cur.execute(
        f"""
        SELECT
          policy_hash, memory_fingerprint, symbol, timeframe, setup_type,
          n, win_rate,
          mean_r, median_r, mad_r, trimmed_mean_r, winsor_mean_r,
          confidence_state, confidence,
          drift_flag, drift_reason,
          first_ts_ms, last_ts_ms, built_ts_ms
        FROM memory_stats_v1
        WHERE {where_sql}
        ORDER BY {order_sql}
        LIMIT :limit;
        """,
        {**params, "limit": limit},
    )

    rows = cur.fetchall()
    print(f"\n[TOP {len(rows)}]")
    for r in rows:
        ph = (str(r["policy_hash"])[:12] + "...") if r["policy_hash"] else "n/a"
        mfp_short = (str(r["memory_fingerprint"])[:12] + "...") if r["memory_fingerprint"] else "n/a"
        drift = "DRIFT" if int(r["drift_flag"] or 0) == 1 else "OK"
        print(
            " - "
            f"mfp={mfp_short} "
            f"{r['symbol']} {r['timeframe']} setup={r['setup_type'] or 'n/a'} "
            f"n={r['n']} win%={_pct(r['win_rate'])} "
            f"winsorR={_fmt(r['winsor_mean_r'])} medR={_fmt(r['median_r'])} madR={_fmt(r['mad_r'])} "
            f"conf={r['confidence_state']}({_fmt(r['confidence'])}) "
            f"{drift} "
            f"last_ts={r['last_ts_ms']} "
            f"policy={ph}"
        )
        if int(r["drift_flag"] or 0) == 1 and (r["drift_reason"] or ""):
            print(f"    drift_reason: {r['drift_reason']}")

    conn.close()
    print("\nDONE ✅")


if __name__ == "__main__":
    main()
