#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — MemoryEntry Builder v1.2 (Phase 5)

Fixes vs v1.1:
- Decisions map indexes BOTH:
    - trade_id
    - client_trade_id / clientTradeId
  so we can join outcomes even if upstream used different IDs.

Keeps:
- Decision schema alignment
- SQLite identity = trade_id
- Fail-soft
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional

from app.ai.ai_memory_contract import (
    ContractPaths,
    append_jsonl,
    extract_fingerprints_from_setup,
    get_ts_ms,
    iter_jsonl,
    normalize_symbol,
    normalize_timeframe,
    validate_decision_record,
    validate_outcome_enriched,
)

PATHS = ContractPaths.default()


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    return conn


def _init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS memory_entries (
            trade_id TEXT PRIMARY KEY,
            entry_id TEXT NOT NULL,
            ts_ms INTEGER NOT NULL,

            account_label TEXT,
            symbol TEXT,
            timeframe TEXT,
            strategy TEXT,
            setup_type TEXT,
            policy_hash TEXT,

            allow INTEGER,
            size_multiplier REAL,
            decision TEXT,
            tier_used TEXT,
            gates_reason TEXT,

            memory_id TEXT,
            setup_fingerprint TEXT,
            memory_fingerprint TEXT,

            pnl_usd REAL,
            r_multiple REAL,
            win INTEGER,
            exit_reason TEXT,
            pnl_kind TEXT,

            raw_json TEXT
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_mem_symbol_tf ON memory_entries(symbol, timeframe);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_mem_policy ON memory_entries(policy_hash);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_mem_memory_id ON memory_entries(memory_id);")
    conn.commit()


def _sha256_hex(obj: Any) -> str:
    try:
        s = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        s = str(obj)
    h = hashlib.sha256()
    h.update(s.encode("utf-8", errors="ignore"))
    return h.hexdigest()


def _entry_id(trade_id: str, ts_ms: int) -> str:
    return f"{trade_id}::{ts_ms}"


def _decision_allow(decision: Dict[str, Any]) -> bool:
    d = str(decision.get("decision") or "").strip().upper()
    if d == "ALLOW":
        return True
    if d.startswith("BLOCK"):
        return False
    if decision.get("allow") is True:
        return True
    if decision.get("allow") is False:
        return False
    return False


def _gates_reason(decision: Dict[str, Any]) -> Optional[str]:
    g = decision.get("gates") if isinstance(decision.get("gates"), dict) else {}
    r = g.get("reason")
    if r is None:
        return None
    s = str(r).strip()
    return s or None


def _policy_hash_from(ev: Dict[str, Any]) -> Optional[str]:
    pol = ev.get("policy") if isinstance(ev.get("policy"), dict) else {}
    v = pol.get("policy_hash")
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _memory_id_from(
    *,
    memory_fingerprint: str,
    policy_hash: str,
    account_scope: str,
    symbol_scope: str,
    timeframe: str,
) -> str:
    return _sha256_hex(
        {
            "memory_fingerprint": memory_fingerprint,
            "policy_hash": policy_hash,
            "account_scope": account_scope,
            "symbol_scope": symbol_scope,
            "timeframe": timeframe,
        }
    )


def _load_decisions_map(*, max_lines: Optional[int] = None) -> Dict[str, Dict[str, Any]]:
    """
    Loads the last valid decision per key.
    IMPORTANT: We index decisions by multiple keys because upstream
    may use trade_id OR client_trade_id as the "join id".
    """
    out: Dict[str, Dict[str, Any]] = {}
    for ev in iter_jsonl(PATHS.decisions_path, max_lines=max_lines):
        ok, _ = validate_decision_record(ev)
        if not ok:
            continue

        tid = str(ev.get("trade_id") or "").strip()
        cid = str(ev.get("client_trade_id") or ev.get("clientTradeId") or "").strip()

        if tid:
            out[tid] = ev
        if cid:
            out[cid] = ev

    return out


def _extract_exit_reason(enriched: Dict[str, Any]) -> Optional[str]:
    outcome = enriched.get("outcome") if isinstance(enriched.get("outcome"), dict) else {}
    payload = outcome.get("payload") if isinstance(outcome.get("payload"), dict) else {}
    v = payload.get("exit_reason")
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _extract_pnl_kind(enriched: Dict[str, Any]) -> Optional[str]:
    outcome = enriched.get("outcome") if isinstance(enriched.get("outcome"), dict) else {}
    payload = outcome.get("payload") if isinstance(outcome.get("payload"), dict) else {}
    extra = payload.get("extra") if isinstance(payload.get("extra"), dict) else {}
    v = extra.get("pnl_kind")
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _build_memory_entry(enriched: Dict[str, Any], decision: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    ok, _ = validate_outcome_enriched(enriched)
    if not ok:
        return None

    setup = enriched.get("setup") if isinstance(enriched.get("setup"), dict) else {}
    trade_id = str(enriched.get("trade_id") or "").strip()
    if not trade_id:
        return None

    ts_ms = get_ts_ms(enriched)

    symbol = normalize_symbol(enriched.get("symbol") or setup.get("symbol")) or "UNKNOWN"
    timeframe = normalize_timeframe(enriched.get("timeframe") or setup.get("timeframe")) or "unknown"
    strategy = str(enriched.get("strategy") or setup.get("strategy") or "").strip() or "unknown"
    setup_type = enriched.get("setup_type") or setup.get("setup_type")

    policy_hash = _policy_hash_from(enriched) or _policy_hash_from(setup) or str(decision.get("policy_hash") or "").strip() or None
    if not policy_hash:
        return None

    sfp, mfp = extract_fingerprints_from_setup(setup)
    if not mfp:
        return None

    stats = enriched.get("stats") if isinstance(enriched.get("stats"), dict) else {}
    pnl_usd = stats.get("pnl_usd")
    r_multiple = stats.get("r_multiple")
    win_raw = stats.get("win")

    win: Optional[bool] = None
    if win_raw is not None:
        win = bool(win_raw)

    allow = _decision_allow(decision)

    try:
        size_multiplier = float(decision.get("size_multiplier")) if decision.get("size_multiplier") is not None else 1.0
    except Exception:
        size_multiplier = 1.0

    mem_obj = decision.get("memory") if isinstance(decision.get("memory"), dict) else {}
    mem_id = mem_obj.get("memory_id")
    if mem_id is None or str(mem_id).strip() == "":
        mem_id = _memory_id_from(
            memory_fingerprint=str(mfp),
            policy_hash=str(policy_hash),
            account_scope="global",
            symbol_scope=str(symbol),
            timeframe=str(timeframe),
        )

    entry: Dict[str, Any] = {
        "schema_version": 1,
        "event_type": "memory_entry",
        "entry_id": _entry_id(trade_id, ts_ms),
        "ts_ms": ts_ms,
        "trade_id": trade_id,
        "account_label": str(enriched.get("account_label") or setup.get("account_label") or "").strip() or "main",
        "symbol": symbol,
        "timeframe": timeframe,
        "strategy": strategy,
        "setup_type": setup_type,
        "policy_hash": policy_hash,
        "setup_fingerprint": sfp,
        "memory_fingerprint": mfp,
        "memory_id": mem_id,
        "decision": {
            "allow": allow,
            "decision": decision.get("decision"),
            "tier_used": decision.get("tier_used"),
            "size_multiplier": size_multiplier,
            "gates_reason": _gates_reason(decision),
        },
        "outcome": {
            "pnl_usd": float(pnl_usd) if pnl_usd is not None else None,
            "r_multiple": float(r_multiple) if r_multiple is not None else None,
            "win": win,
            "exit_reason": _extract_exit_reason(enriched),
            "pnl_kind": _extract_pnl_kind(enriched),
        },
    }

    return entry


def _insert_entry(conn: sqlite3.Connection, entry: Dict[str, Any]) -> None:
    d = entry.get("decision") if isinstance(entry.get("decision"), dict) else {}
    o = entry.get("outcome") if isinstance(entry.get("outcome"), dict) else {}

    def _i(b: Any) -> Optional[int]:
        if b is None:
            return None
        return 1 if bool(b) else 0

    conn.execute(
        """
        INSERT OR REPLACE INTO memory_entries (
            trade_id, entry_id, ts_ms,
            account_label, symbol, timeframe, strategy, setup_type, policy_hash,
            allow, size_multiplier, decision, tier_used, gates_reason,
            memory_id, setup_fingerprint, memory_fingerprint,
            pnl_usd, r_multiple, win, exit_reason, pnl_kind,
            raw_json
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
        """,
        (
            entry.get("trade_id"),
            entry.get("entry_id"),
            int(entry.get("ts_ms") or 0),
            entry.get("account_label"),
            entry.get("symbol"),
            entry.get("timeframe"),
            entry.get("strategy"),
            entry.get("setup_type"),
            entry.get("policy_hash"),
            _i(d.get("allow")),
            float(d.get("size_multiplier") or 1.0),
            d.get("decision"),
            d.get("tier_used"),
            d.get("gates_reason"),
            entry.get("memory_id"),
            entry.get("setup_fingerprint"),
            entry.get("memory_fingerprint"),
            o.get("pnl_usd"),
            o.get("r_multiple"),
            _i(o.get("win")),
            o.get("exit_reason"),
            o.get("pnl_kind"),
            json.dumps(entry, ensure_ascii=False),
        ),
    )


def rebuild(*, max_decision_lines: Optional[int] = None, max_outcome_lines: Optional[int] = None) -> Dict[str, Any]:
    PATHS.memory_entries_path.parent.mkdir(parents=True, exist_ok=True)

    if PATHS.memory_entries_path.exists():
        PATHS.memory_entries_path.unlink()
    if PATHS.memory_index_path.exists():
        PATHS.memory_index_path.unlink()

    decisions = _load_decisions_map(max_lines=max_decision_lines)

    conn = _connect(PATHS.memory_index_path)
    _init_db(conn)

    stats = {
        "decisions_indexed_keys": len(decisions),
        "outcomes_scanned": 0,
        "entries_written": 0,
        "skipped_no_decision": 0,
        "skipped_bad_outcome": 0,
        "skipped_bad_policy_or_fp": 0,
    }

    for enriched in iter_jsonl(PATHS.outcomes_path, max_lines=max_outcome_lines):
        stats["outcomes_scanned"] += 1

        ok, _ = validate_outcome_enriched(enriched)
        if not ok:
            stats["skipped_bad_outcome"] += 1
            continue

        tid = str(enriched.get("trade_id") or "").strip()
        if not tid:
            stats["skipped_bad_outcome"] += 1
            continue

        decision = decisions.get(tid)
        if not decision:
            stats["skipped_no_decision"] += 1
            continue

        entry = _build_memory_entry(enriched, decision)
        if not entry:
            stats["skipped_bad_policy_or_fp"] += 1
            continue

        append_jsonl(PATHS.memory_entries_path, entry)
        _insert_entry(conn, entry)
        stats["entries_written"] += 1

        if stats["entries_written"] % 250 == 0:
            conn.commit()

    conn.commit()
    conn.close()
    return stats


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rebuild", action="store_true", help="Rebuild entries + index from scratch (default).")
    ap.add_argument("--max-decisions", type=int, default=0, help="Max decision lines to scan (0 = no cap).")
    ap.add_argument("--max-outcomes", type=int, default=0, help="Max outcomes lines to scan (0 = no cap).")
    args = ap.parse_args()

    max_dec = args.max_decisions if args.max_decisions and args.max_decisions > 0 else None
    max_out = args.max_outcomes if args.max_outcomes and args.max_outcomes > 0 else None

    stats = rebuild(max_decision_lines=max_dec, max_outcome_lines=max_out)

    print("=== MemoryEntry Builder v1.2 ===")
    print(f"memory_entries_path : {PATHS.memory_entries_path}")
    print(f"memory_index_path   : {PATHS.memory_index_path}")
    for k, v in stats.items():
        print(f"{k}: {v}")
    print("DONE")


if __name__ == "__main__":
    main()
