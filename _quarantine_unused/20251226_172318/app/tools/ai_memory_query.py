#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Memory Query Tool v1 (Phase 5.2)

Purpose
-------
One command to query the MemoryEntry substrate (SQLite index) and print:
- Row count for the filter
- Basic aggregates: win%, avg R, avg pnl, allow%, avg size_multiplier
- Top rows (most recent by ts_ms)

Usage
-----
python app/tools/ai_memory_query.py --limit 10
python app/tools/ai_memory_query.py --symbol BTCUSDT --timeframe 5m --limit 20
python app/tools/ai_memory_query.py --setup-type trend_pullback
python app/tools/ai_memory_query.py --policy-hash <hash> --symbol BTCUSDT
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from app.ai.ai_memory_contract import ContractPaths, normalize_symbol, normalize_timeframe

PATHS = ContractPaths.default()


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _exists_nonempty(p: Path) -> bool:
    try:
        return p.exists() and p.stat().st_size > 0
    except Exception:
        return False


def _build_where(
    *,
    symbol: Optional[str],
    timeframe: Optional[str],
    setup_type: Optional[str],
    policy_hash: Optional[str],
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

    if not clauses:
        return ("1=1", params)

    return (" AND ".join(clauses), params)


def _pct(n: Optional[float]) -> str:
    if n is None:
        return "n/a"
    return f"{n * 100:.1f}%"


def _fmt(n: Any) -> str:
    if n is None:
        return "n/a"
    try:
        if isinstance(n, (int, float)):
            return f"{n:.4f}".rstrip("0").rstrip(".")
    except Exception:
        pass
    return str(n)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", type=str, default="", help="Symbol filter, e.g. BTCUSDT (optional)")
    ap.add_argument("--timeframe", type=str, default="", help="Timeframe filter, e.g. 5m (optional)")
    ap.add_argument("--setup-type", type=str, default="", help="Setup type filter (optional)")
    ap.add_argument("--policy-hash", type=str, default="", help="Policy hash filter (optional)")
    ap.add_argument("--limit", type=int, default=10, help="Rows to print (default 10, max 200)")
    args = ap.parse_args()

    db_path = PATHS.memory_index_path
    if not _exists_nonempty(db_path):
        print("FAIL ❌ memory_index.sqlite missing or empty:")
        print(f"  - {db_path}")
        print("Fix: run memory builder first:")
        print("  python -m app.ai.ai_memory_entry_builder --rebuild")
        return

    symbol = normalize_symbol(args.symbol) if args.symbol.strip() else None
    timeframe = normalize_timeframe(args.timeframe) if args.timeframe.strip() else None
    setup_type = args.setup_type.strip() or None
    policy_hash = args.policy_hash.strip() or None

    limit = int(args.limit or 10)
    if limit < 1:
        limit = 1
    if limit > 200:
        limit = 200

    where_sql, params = _build_where(
        symbol=symbol,
        timeframe=timeframe,
        setup_type=setup_type,
        policy_hash=policy_hash,
    )

    conn = _connect(db_path)
    cur = conn.cursor()

    # Count
    cur.execute(f"SELECT COUNT(*) AS n FROM memory_entries WHERE {where_sql};", params)
    total = int(cur.fetchone()["n"])

    print("=== AI Memory Query v1 ===")
    print(f"db        : {db_path}")
    print(f"filter    : symbol={symbol or 'ANY'}, timeframe={timeframe or 'ANY'}, setup_type={setup_type or 'ANY'}, policy_hash={(policy_hash[:12] + '...') if policy_hash else 'ANY'}")
    print(f"matched   : {total}")

    if total == 0:
        conn.close()
        print("DONE")
        return

    # Aggregates
    # Notes:
    # - win is stored as 0/1/NULL
    # - allow stored as 0/1/NULL
    # - avg(win) gives win rate for non-null
    cur.execute(
        f"""
        SELECT
            AVG(CASE WHEN win IS NULL THEN NULL ELSE win END) AS win_rate,
            AVG(r_multiple) AS avg_r,
            AVG(pnl_usd) AS avg_pnl,
            AVG(CASE WHEN allow IS NULL THEN NULL ELSE allow END) AS allow_rate,
            AVG(size_multiplier) AS avg_size_multiplier,
            COUNT(*) AS n
        FROM memory_entries
        WHERE {where_sql};
        """,
        params,
    )
    agg = cur.fetchone()

    win_rate = agg["win_rate"]
    avg_r = agg["avg_r"]
    avg_pnl = agg["avg_pnl"]
    allow_rate = agg["allow_rate"]
    avg_sm = agg["avg_size_multiplier"]

    print("\n[AGG]")
    print(f"win_rate          : {_pct(win_rate)}")
    print(f"avg_r_multiple    : {_fmt(avg_r)}")
    print(f"avg_pnl_usd       : {_fmt(avg_pnl)}")
    print(f"allow_rate        : {_pct(allow_rate)}")
    print(f"avg_size_multiplier: {_fmt(avg_sm)}")

    # Top rows (most recent)
    cur.execute(
        f"""
        SELECT
            ts_ms, trade_id, account_label, symbol, timeframe, strategy, setup_type, policy_hash,
            allow, size_multiplier, decision, tier_used, gates_reason,
            memory_id, setup_fingerprint, memory_fingerprint,
            pnl_usd, r_multiple, win, exit_reason, pnl_kind
        FROM memory_entries
        WHERE {where_sql}
        ORDER BY ts_ms DESC
        LIMIT :limit;
        """,
        {**params, "limit": limit},
    )

    rows = cur.fetchall()
    print(f"\n[TOP {len(rows)} MOST RECENT]")
    for r in rows:
        ph = r["policy_hash"] or ""
        ph_short = (str(ph)[:12] + "...") if ph else "n/a"
        mem_id = r["memory_id"] or "n/a"
        mem_short = str(mem_id)[:12] + "..." if mem_id and len(str(mem_id)) > 12 else str(mem_id)

        print(
            " - "
            f"ts={r['ts_ms']} "
            f"trade_id={r['trade_id']} "
            f"{r['symbol']} {r['timeframe']} "
            f"setup_type={r['setup_type'] or 'n/a'} "
            f"allow={r['allow']} "
            f"sm={r['size_multiplier']} "
            f"win={r['win']} "
            f"R={_fmt(r['r_multiple'])} "
            f"pnl={_fmt(r['pnl_usd'])} "
            f"mem={mem_short} "
            f"policy={ph_short}"
        )

    conn.close()
    print("\nDONE")


if __name__ == "__main__":
    main()
