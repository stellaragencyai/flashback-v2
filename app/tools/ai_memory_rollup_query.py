#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Memory Rollup Query Tool v1 (Phase 5.6)

Purpose
-------
Query memory_rollups.sqlite and print:
- matched count
- top rollups by most recent or best expectancy proxy

Usage
-----
python app/tools/ai_memory_rollup_query.py --limit 10
python app/tools/ai_memory_rollup_query.py --symbol BTCUSDT --timeframe 5m --limit 20
python app/tools/ai_memory_rollup_query.py --sort best
python app/tools/ai_memory_rollup_query.py --memory-id <idprefix>
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from app.ai.ai_memory_contract import ContractPaths, normalize_symbol, normalize_timeframe

PATHS = ContractPaths.default()


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
    memory_id_prefix: Optional[str],
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

    if memory_id_prefix:
        clauses.append("memory_id LIKE :memory_id_prefix")
        params["memory_id_prefix"] = memory_id_prefix + "%"

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
    ap.add_argument("--memory-id", type=str, default="", help="Memory ID prefix filter (optional)")
    ap.add_argument("--sort", type=str, default="recent", choices=["recent", "best"], help="Sort order: recent|best")
    ap.add_argument("--limit", type=int, default=10, help="Rows to print (default 10, max 200)")
    args = ap.parse_args()

    db_path = (PATHS.memory_index_path.parent / "memory_rollups.sqlite").resolve()
    if not _exists_nonempty(db_path):
        print("FAIL ❌ memory_rollups.sqlite missing or empty:")
        print(f"  - {db_path}")
        print("Fix: run rollup builder first:")
        print("  python app/tools/ai_memory_rollup_build.py --rebuild")
        return

    symbol = normalize_symbol(args.symbol) if args.symbol.strip() else None
    timeframe = normalize_timeframe(args.timeframe) if args.timeframe.strip() else None
    setup_type = args.setup_type.strip() or None
    policy_hash = args.policy_hash.strip() or None
    mem_prefix = args.memory_id.strip() or None

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
        memory_id_prefix=mem_prefix,
    )

    order_sql = "last_ts_ms DESC"
    if args.sort == "best":
        # Crude expectancy proxy for v1:
        # prioritize higher avg_r, then higher win_rate, then more samples.
        order_sql = "avg_r_multiple DESC, win_rate DESC, n DESC, last_ts_ms DESC"

    conn = _connect(db_path)
    cur = conn.cursor()

    cur.execute(f"SELECT COUNT(*) AS n FROM memory_rollups WHERE {where_sql};", params)
    total = int(cur.fetchone()["n"])

    print("=== AI Memory Rollup Query v1 ===")
    print(f"db      : {db_path}")
    print(
        "filter  : "
        f"symbol={symbol or 'ANY'}, "
        f"timeframe={timeframe or 'ANY'}, "
        f"setup_type={setup_type or 'ANY'}, "
        f"policy_hash={(policy_hash[:12] + '...') if policy_hash else 'ANY'}, "
        f"memory_id={(mem_prefix + '...') if mem_prefix else 'ANY'}"
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
            memory_id, symbol, timeframe, setup_type, policy_hash,
            n, wins, losses,
            win_rate, avg_r_multiple, avg_pnl_usd,
            allow_rate, avg_size_multiplier,
            last_ts_ms, built_ts_ms
        FROM memory_rollups
        WHERE {where_sql}
        ORDER BY {order_sql}
        LIMIT :limit;
        """,
        {**params, "limit": limit},
    )

    rows = cur.fetchall()
    print(f"\n[TOP {len(rows)}]")
    for r in rows:
        ph = r["policy_hash"] or ""
        ph_short = (str(ph)[:12] + "...") if ph else "n/a"
        mem = str(r["memory_id"])
        mem_short = mem[:12] + "..." if len(mem) > 12 else mem
        print(
            " - "
            f"mem={mem_short} "
            f"{r['symbol']} {r['timeframe']} "
            f"setup={r['setup_type'] or 'n/a'} "
            f"n={r['n']} "
            f"win%={_pct(r['win_rate'])} "
            f"avgR={_fmt(r['avg_r_multiple'])} "
            f"avgPnL={_fmt(r['avg_pnl_usd'])} "
            f"allow%={_pct(r['allow_rate'])} "
            f"sm={_fmt(r['avg_size_multiplier'])} "
            f"last_ts={r['last_ts_ms']} "
            f"policy={ph_short}"
        )

    conn.close()
    print("\nDONE")


if __name__ == "__main__":
    main()
