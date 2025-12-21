#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Memory Contract LOCK Check v1.1 (Phase 5.5)

Brutal invariants, minimal excuses.

PASS criteria:
- state/ai_memory/memory_entries.jsonl exists + non-empty
- state/ai_memory/memory_index.sqlite exists + non-empty
- SQLite table memory_entries exists with required columns
- SQLite primary key is entry_id (history-safe identity)
- JSONL rows are schema-stable:
    - schema_version == 1
    - event_type == "memory_entry"
    - required keys exist
    - no unknown top-level keys
    - decision/outcome sub-objects present with stable types
    - fingerprints + policy_hash present
    - entry_id UNIQUE within JSONL (identity)
    - trade_id MAY repeat (history is expected)
- SQLite row count == JSONL ok row count
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Set, Tuple

from app.ai.ai_memory_contract import ContractPaths, iter_jsonl, normalize_symbol, normalize_timeframe

PATHS = ContractPaths.default()

# --------------------------- helpers ---------------------------

def _fail(msg: str, *, code: int = 2) -> None:
    print(f"FAIL ❌ {msg}")
    sys.exit(code)

def _warn(msg: str) -> None:
    print(f"WARN ⚠️ {msg}")

def _exists_nonempty(p: Path) -> bool:
    try:
        return p.exists() and p.stat().st_size > 0
    except Exception:
        return False

def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn

def _table_info(conn: sqlite3.Connection, table: str) -> list[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table});")
    return cur.fetchall()

def _table_columns(conn: sqlite3.Connection, table: str) -> Set[str]:
    return {str(r["name"]) for r in _table_info(conn, table)}

def _pk_column_names(conn: sqlite3.Connection, table: str) -> Set[str]:
    # PRAGMA table_info: "pk" is 1-based order for PK columns; 0 means not PK.
    rows = _table_info(conn, table)
    return {str(r["name"]) for r in rows if int(r["pk"] or 0) > 0}

def _as_bool(v: Any) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)) and v in (0, 1):
        return bool(int(v))
    s = str(v).strip().lower()
    if s in ("true", "1", "yes"):
        return True
    if s in ("false", "0", "no"):
        return False
    return None

def _as_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None

def _as_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None

# --------------------------- schema rules ---------------------------

TOP_LEVEL_ALLOWED: Set[str] = {
    "schema_version",
    "event_type",
    "entry_id",
    "ts_ms",
    "trade_id",
    "account_label",
    "symbol",
    "timeframe",
    "strategy",
    "setup_type",
    "policy_hash",
    "setup_fingerprint",
    "memory_fingerprint",
    "memory_id",
    "decision",
    "outcome",
}

TOP_LEVEL_REQUIRED: Set[str] = {
    "schema_version",
    "event_type",
    "entry_id",
    "ts_ms",
    "trade_id",
    "symbol",
    "timeframe",
    "policy_hash",
    "memory_fingerprint",
    "decision",
    "outcome",
}

DECISION_REQUIRED: Set[str] = {
    "allow",
    "size_multiplier",
}

OUTCOME_REQUIRED: Set[str] = {
    "pnl_usd",
    "r_multiple",
    "win",
    "exit_reason",
    "pnl_kind",
}

SQL_REQUIRED_COLS: Set[str] = {
    "entry_id",
    "trade_id",
    "ts_ms",
    "symbol",
    "timeframe",
    "policy_hash",
    "allow",
    "size_multiplier",
    "memory_id",
    "memory_fingerprint",
    "raw_json",
}

# --------------------------- validators ---------------------------

def _validate_row(ev: Dict[str, Any]) -> Tuple[bool, str]:
    # unknown keys
    unknown = set(ev.keys()) - TOP_LEVEL_ALLOWED
    if unknown:
        return False, f"unknown_top_level_keys={sorted(list(unknown))}"

    # required keys
    missing = [k for k in TOP_LEVEL_REQUIRED if k not in ev]
    if missing:
        return False, f"missing_required_keys={missing}"

    # schema_version + event_type
    sv = _as_int(ev.get("schema_version"))
    if sv != 1:
        return False, f"bad_schema_version={ev.get('schema_version')}"
    if ev.get("event_type") != "memory_entry":
        return False, f"bad_event_type={ev.get('event_type')}"

    # entry_id
    eid = str(ev.get("entry_id") or "").strip()
    if not eid:
        return False, "missing_entry_id"

    # trade_id
    tid = str(ev.get("trade_id") or "").strip()
    if not tid:
        return False, "missing_trade_id"

    # ts_ms plausibility
    ts_ms = _as_int(ev.get("ts_ms"))
    if ts_ms is None:
        return False, "bad_ts_ms"
    if ts_ms < 1_500_000_000_000 or ts_ms > 4_000_000_000_000:
        return False, f"ts_ms_out_of_range={ts_ms}"

    # symbol/timeframe normalized
    sym = normalize_symbol(ev.get("symbol"))
    if not sym:
        return False, "bad_symbol"
    tf = normalize_timeframe(ev.get("timeframe"))
    if not tf:
        return False, "bad_timeframe"

    # policy_hash + memory_fingerprint
    ph = str(ev.get("policy_hash") or "").strip()
    if not ph:
        return False, "missing_policy_hash"
    mfp = str(ev.get("memory_fingerprint") or "").strip()
    if not mfp:
        return False, "missing_memory_fingerprint"

    # decision object
    d = ev.get("decision")
    if not isinstance(d, dict):
        return False, "decision_not_object"
    for k in DECISION_REQUIRED:
        if k not in d:
            return False, f"decision_missing_{k}"
    allow = _as_bool(d.get("allow"))
    if allow is None:
        return False, "decision_allow_bad_type"
    sm = _as_float(d.get("size_multiplier"))
    if sm is None:
        return False, "decision_size_multiplier_bad_type"
    if sm <= 0 or sm > 100:
        return False, f"decision_size_multiplier_out_of_range={sm}"

    # outcome object
    o = ev.get("outcome")
    if not isinstance(o, dict):
        return False, "outcome_not_object"
    for k in OUTCOME_REQUIRED:
        if k not in o:
            return False, f"outcome_missing_{k}"

    # win should be bool/None
    win = o.get("win")
    if win is not None and _as_bool(win) is None:
        return False, "outcome_win_bad_type"

    # pnl/r_multiple should be float/None
    if o.get("pnl_usd") is not None and _as_float(o.get("pnl_usd")) is None:
        return False, "outcome_pnl_usd_bad_type"
    if o.get("r_multiple") is not None and _as_float(o.get("r_multiple")) is None:
        return False, "outcome_r_multiple_bad_type"

    return True, "ok"

# --------------------------- main ---------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-lines", type=int, default=0, help="Max JSONL lines to scan (0 = no cap)")
    ap.add_argument("--db-sample", type=int, default=5, help="How many newest DB rows to spot-check (default 5, max 50)")
    args = ap.parse_args()

    max_lines = args.max_lines if args.max_lines and args.max_lines > 0 else None
    db_sample = int(args.db_sample or 5)
    if db_sample < 1:
        db_sample = 1
    if db_sample > 50:
        db_sample = 50

    mem_jsonl = PATHS.memory_entries_path
    mem_db = PATHS.memory_index_path

    print("=== AI Memory Contract LOCK Check v1.1 ===")
    print(f"memory_entries.jsonl : {mem_jsonl}")
    print(f"memory_index.sqlite  : {mem_db}")

    if not _exists_nonempty(mem_jsonl):
        _fail("memory_entries.jsonl missing or empty. Rebuild memory first.")
    if not _exists_nonempty(mem_db):
        _fail("memory_index.sqlite missing or empty. Rebuild memory first.")

    # ---------------- JSONL validation ----------------

    seen_entry_ids: Set[str] = set()
    trade_ids: Set[str] = set()

    ok_n = 0
    bad_n = 0
    dup_trade_ids = 0

    first_bad: Optional[str] = None
    last_ok_entry_id: Optional[str] = None

    for ev in iter_jsonl(mem_jsonl, max_lines=max_lines):
        ok, reason = _validate_row(ev)
        if not ok:
            bad_n += 1
            if first_bad is None:
                first_bad = reason
            _fail(f"memory_entries.jsonl schema violation: {first_bad}")

        # identity
        eid = str(ev.get("entry_id") or "").strip()
        if eid in seen_entry_ids:
            _fail(f"duplicate entry_id in memory_entries.jsonl (identity must be unique): {eid}")
        seen_entry_ids.add(eid)
        last_ok_entry_id = eid

        # trade_id is allowed to repeat (history)
        tid = str(ev.get("trade_id") or "").strip()
        if tid:
            if tid in trade_ids:
                dup_trade_ids += 1
            trade_ids.add(tid)

        ok_n += 1

    if ok_n < 1:
        _fail("No readable memory_entry rows found in memory_entries.jsonl")

    print(f"[JSONL] ok_rows={ok_n} bad_rows={bad_n} unique_entry_ids={len(seen_entry_ids)} unique_trade_ids={len(trade_ids)} dup_trade_id_rows={dup_trade_ids}")
    if dup_trade_ids > 0:
        print("[JSONL] note: duplicate trade_id rows detected (expected for history-safe memory).")

    # ---------------- SQLite validation ----------------

    conn = _connect(mem_db)
    try:
        cols = _table_columns(conn, "memory_entries")
        missing_cols = sorted(list(SQL_REQUIRED_COLS - cols))
        if missing_cols:
            _fail(f"SQLite table memory_entries missing required columns: {missing_cols}")

        pk_cols = _pk_column_names(conn, "memory_entries")
        if "entry_id" not in pk_cols:
            _fail(f"SQLite primary key is not entry_id. pk_cols={sorted(list(pk_cols))} (must include entry_id for history-safe identity)")

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS n FROM memory_entries;")
        db_n = int(cur.fetchone()["n"])

        if db_n != ok_n:
            _fail(f"Row count mismatch: sqlite={db_n} jsonl_ok_rows={ok_n} (must match exactly)")

        # Cheap corruption detector: raw_json must exist and decode.
        cur.execute("SELECT entry_id, trade_id, raw_json FROM memory_entries ORDER BY ts_ms DESC LIMIT ?;", (db_sample,))
        rows = cur.fetchall()
        for r in rows:
            tj = r["raw_json"]
            if not isinstance(tj, str) or len(tj.strip()) < 10:
                _fail(f"SQLite raw_json missing/short for entry_id={r['entry_id']} trade_id={r['trade_id']}")
            try:
                obj = json.loads(tj)
            except Exception:
                _fail(f"SQLite raw_json not valid JSON for entry_id={r['entry_id']} trade_id={r['trade_id']}")

            # Raw JSON must match row identity (hard invariant)
            if str(obj.get("entry_id") or "").strip() != str(r["entry_id"] or "").strip():
                _fail(f"SQLite raw_json entry_id mismatch for entry_id={r['entry_id']}")
            if str(obj.get("trade_id") or "").strip() != str(r["trade_id"] or "").strip():
                _fail(f"SQLite raw_json trade_id mismatch for entry_id={r['entry_id']}")

        # Spot-check: last OK JSONL entry_id exists in DB (helps catch “JSONL written but DB insert skipped” bugs)
        if last_ok_entry_id:
            cur.execute("SELECT COUNT(*) AS n FROM memory_entries WHERE entry_id = ?;", (last_ok_entry_id,))
            n = int(cur.fetchone()["n"])
            if n != 1:
                _fail(f"SQLite missing last JSONL entry_id={last_ok_entry_id} (JSONL/DB drift)")

    finally:
        conn.close()

    print(f"[SQL] required_cols_ok=1 pk_entry_id_ok=1 row_count={db_n}")

    print("\nPASS ✅ MemoryEntry substrate is LOCKED and schema-stable (history-safe).")
    print("Phase 6+ can consume memory without silent drift poisoning learning.")

if __name__ == "__main__":
    main()
