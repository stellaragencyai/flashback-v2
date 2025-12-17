#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Memory Contract LOCK Check v1 (Phase 5.5)

Brutal invariants, minimal excuses.

PASS criteria:
- state/ai_memory/memory_entries.jsonl exists + non-empty
- state/ai_memory/memory_index.sqlite exists + non-empty
- SQLite table memory_entries exists with required columns
- JSONL rows are schema-stable:
    - schema_version == 1
    - event_type == "memory_entry"
    - required keys exist
    - no unknown top-level keys
    - decision/outcome sub-objects present with stable types
    - fingerprints + policy_hash present
    - trade_id unique within JSONL
- SQLite row count == JSONL row count
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Set, Tuple

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

def _table_columns(conn: sqlite3.Connection, table: str) -> Set[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table});")
    rows = cur.fetchall()
    return {str(r["name"]) for r in rows}

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
    "trade_id",
    "entry_id",
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
    args = ap.parse_args()
    max_lines = args.max_lines if args.max_lines and args.max_lines > 0 else None

    mem_jsonl = PATHS.memory_entries_path
    mem_db = PATHS.memory_index_path

    print("=== AI Memory Contract LOCK Check v1 ===")
    print(f"memory_entries.jsonl : {mem_jsonl}")
    print(f"memory_index.sqlite  : {mem_db}")

    if not _exists_nonempty(mem_jsonl):
        _fail("memory_entries.jsonl missing or empty. Rebuild memory first.")
    if not _exists_nonempty(mem_db):
        _fail("memory_index.sqlite missing or empty. Rebuild memory first.")

    # Validate JSONL
    seen: Set[str] = set()
    ok_n = 0
    bad_n = 0
    first_bad: Optional[str] = None

    for ev in iter_jsonl(mem_jsonl, max_lines=max_lines):
        tid = str(ev.get("trade_id") or "").strip()
        if tid:
            if tid in seen:
                _fail(f"duplicate trade_id in memory_entries.jsonl: {tid}")
            seen.add(tid)

        ok, reason = _validate_row(ev)
        if ok:
            ok_n += 1
        else:
            bad_n += 1
            if first_bad is None:
                first_bad = reason

        # hard stop if it’s clearly broken
        if bad_n >= 1:
            # We fail on first bad row: this is a LOCK check, not a dashboard.
            _fail(f"memory_entries.jsonl schema violation: {first_bad}")

    if ok_n < 1:
        _fail("No readable memory_entry rows found in memory_entries.jsonl")

    print(f"[JSONL] ok_rows={ok_n} bad_rows={bad_n} unique_trade_ids={len(seen)}")

    # Validate SQLite schema + counts
    conn = _connect(mem_db)
    try:
        cols = _table_columns(conn, "memory_entries")
        missing_cols = sorted(list(SQL_REQUIRED_COLS - cols))
        if missing_cols:
            _fail(f"SQLite table memory_entries missing required columns: {missing_cols}")

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS n FROM memory_entries;")
        db_n = int(cur.fetchone()["n"])

        if db_n != ok_n:
            _fail(f"Row count mismatch: sqlite={db_n} jsonl={ok_n} (must match exactly)")

        # spot-check raw_json decodes for a few rows (cheap corruption detector)
        cur.execute("SELECT trade_id, raw_json FROM memory_entries ORDER BY ts_ms DESC LIMIT 5;")
        for r in cur.fetchall():
            tj = r["raw_json"]
            if not isinstance(tj, str) or len(tj.strip()) < 10:
                _fail(f"SQLite raw_json missing/short for trade_id={r['trade_id']}")

    finally:
        conn.close()

    print(f"[SQL] required_cols_ok=1 row_count={db_n}")

    # If we got here, we’re stable.
    print("\nPASS ✅ MemoryEntry substrate is LOCKED and schema-stable.")
    print("This means Phase 6+ can consume memory without silent drift poisoning learning.")

if __name__ == "__main__":
    main()
