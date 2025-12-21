#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Memory Rollup Builder v1.1 (Phase 6 substrate)

v1.1 FIX
- Rollups now include memory_fingerprint explicitly.
- Rollup grouping keys include memory_fingerprint so Phase 6 queries remain stable even if memory_id semantics evolve.
- Adds index on memory_fingerprint.

Reads:
- state/ai_memory/memory_index.sqlite (table: memory_entries)

Writes:
- state/ai_memory/memory_rollups.sqlite (table: memory_rollups)

Usage
-----
python -m app.tools.ai_memory_rollup_build --rebuild
"""

from __future__ import annotations

import argparse
import sqlite3
import time
from pathlib import Path
from typing import Any, Optional

from app.ai.ai_memory_contract import ContractPaths

PATHS = ContractPaths.default()


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


def _init_rollups_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS memory_rollups (
            rollup_id TEXT PRIMARY KEY,

            -- stable lookup key used by Phase 6+: memory_fingerprint
            memory_fingerprint TEXT NOT NULL,

            -- optional/derived identity (can evolve), kept for auditing + future scoping
            memory_id TEXT,

            symbol TEXT,
            timeframe TEXT,
            setup_type TEXT,
            policy_hash TEXT,

            n INTEGER NOT NULL,
            wins INTEGER NOT NULL,
            losses INTEGER NOT NULL,

            win_rate REAL,
            avg_r_multiple REAL,
            avg_pnl_usd REAL,

            allow_rate REAL,
            avg_size_multiplier REAL,

            last_ts_ms INTEGER NOT NULL,
            built_ts_ms INTEGER NOT NULL
        );
        """
    )

    conn.execute("CREATE INDEX IF NOT EXISTS idx_roll_mfp ON memory_rollups(memory_fingerprint);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_roll_mem ON memory_rollups(memory_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_roll_sym_tf ON memory_rollups(symbol, timeframe);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_roll_policy ON memory_rollups(policy_hash);")
    conn.commit()


def _rollup_id(
    memory_fingerprint: str,
    symbol: Optional[str],
    timeframe: Optional[str],
    setup_type: Optional[str],
    policy_hash: Optional[str],
) -> str:
    # Deterministic string key is fine for v1.1.
    return "||".join(
        [
            str(memory_fingerprint or "").strip(),
            str(symbol or "").strip(),
            str(timeframe or "").strip(),
            str(setup_type or "").strip(),
            str(policy_hash or "").strip(),
        ]
    )


def rebuild() -> dict[str, Any]:
    src_db = PATHS.memory_index_path
    if not _exists_nonempty(src_db):
        return {"ok": False, "reason": f"missing_or_empty_source_db: {src_db}"}

    out_db = (PATHS.memory_index_path.parent / "memory_rollups.sqlite").resolve()

    # Fresh output
    if out_db.exists():
        try:
            out_db.unlink()
        except Exception:
            pass

    src = _connect_ro(src_db)
    dst = _connect_rw(out_db)
    _init_rollups_db(dst)

    built_ts_ms = int(time.time() * 1000)

    cur = src.cursor()
    cur.execute(
        """
        SELECT
            memory_fingerprint,
            -- memory_id may be NULL for some legacy/backfilled rows; keep it if present
            memory_id,
            symbol,
            timeframe,
            setup_type,
            policy_hash,

            COUNT(*) AS n,

            SUM(CASE WHEN win = 1 THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN win = 0 THEN 1 ELSE 0 END) AS losses,

            AVG(CASE WHEN win IS NULL THEN NULL ELSE win END) AS win_rate,
            AVG(r_multiple) AS avg_r_multiple,
            AVG(pnl_usd) AS avg_pnl_usd,

            AVG(CASE WHEN allow IS NULL THEN NULL ELSE allow END) AS allow_rate,
            AVG(size_multiplier) AS avg_size_multiplier,

            MAX(ts_ms) AS last_ts_ms
        FROM memory_entries
        WHERE memory_fingerprint IS NOT NULL AND TRIM(memory_fingerprint) <> ''
        GROUP BY memory_fingerprint, memory_id, symbol, timeframe, setup_type, policy_hash
        ORDER BY last_ts_ms DESC;
        """
    )
    rows = cur.fetchall()

    ins = dst.cursor()
    inserted = 0
    for r in rows:
        rid = _rollup_id(
            str(r["memory_fingerprint"]),
            r["symbol"],
            r["timeframe"],
            r["setup_type"],
            r["policy_hash"],
        )
        ins.execute(
            """
            INSERT OR REPLACE INTO memory_rollups (
                rollup_id,
                memory_fingerprint,
                memory_id,
                symbol, timeframe, setup_type, policy_hash,
                n, wins, losses,
                win_rate, avg_r_multiple, avg_pnl_usd,
                allow_rate, avg_size_multiplier,
                last_ts_ms, built_ts_ms
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
            """,
            (
                rid,
                r["memory_fingerprint"],
                r["memory_id"],
                r["symbol"],
                r["timeframe"],
                r["setup_type"],
                r["policy_hash"],
                int(r["n"] or 0),
                int(r["wins"] or 0),
                int(r["losses"] or 0),
                r["win_rate"],
                r["avg_r_multiple"],
                r["avg_pnl_usd"],
                r["allow_rate"],
                r["avg_size_multiplier"],
                int(r["last_ts_ms"] or 0),
                int(built_ts_ms),
            ),
        )
        inserted += 1

    dst.commit()
    src.close()
    dst.close()

    return {
        "ok": True,
        "source_db": str(src_db),
        "rollups_db": str(out_db),
        "rollup_rows": inserted,
        "built_ts_ms": built_ts_ms,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rebuild", action="store_true", help="Rebuild rollups from scratch (default).")
    ap.add_argument("--append", action="store_true", help="Reserved. Currently behaves like rebuild.")
    _ = ap.parse_args()

    stats = rebuild()

    print("=== AI Memory Rollup Build v1.1 ===")
    if not stats.get("ok"):
        print("FAIL ❌", stats.get("reason"))
        return

    print("source_db  :", stats["source_db"])
    print("rollups_db :", stats["rollups_db"])
    print("rollup_rows:", stats["rollup_rows"])
    print("built_ts_ms:", stats["built_ts_ms"])
    print("DONE")


if __name__ == "__main__":
    main()
