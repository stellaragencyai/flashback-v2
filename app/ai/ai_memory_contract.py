#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Memory Contract v1 (Phase 5)

Defines:
- Canonical contract paths
- Normalization helpers
- Validation helpers
- JSONL read/write utilities
- Fingerprinting helpers (setup_fingerprint / memory_fingerprint)

This module MUST be import-stable.
No heavy imports. No Bybit calls. Fail-soft.

v1.1 FIX
--------
- Backward compatible: if setup.payload.features.memory_fingerprint is missing,
  derive it deterministically from setup_context using the same principles as
  ai_events_spine:
    • stable JSON hashing (sha256)
    • timeframe normalized
    • feature filter excludes volatile blobs + recursive fingerprint keys

v1.2 ADD
--------
- Locks Phase 5 "read semantics" foundations:
    • schema/version constants + forward-compat rules
    • memory_entry validator (contract-level)
    • immutable vs mutable field guidance
    • SQLite query helpers (best-effort, no extra deps)
    • canary flags for A/B testing and rollback
    • lifecycle caps (prevents silent bloat)
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

# ----------------------------- ROOT ----------------------------------------

try:
    from app.core.config import settings  # type: ignore
    ROOT: Path = settings.ROOT  # type: ignore
except Exception:  # pragma: no cover
    ROOT = Path(__file__).resolve().parents[2]

# ----------------------------- JSON ----------------------------------------

try:
    import orjson  # type: ignore
    _HAS_ORJSON = True
except Exception:  # pragma: no cover
    _HAS_ORJSON = False

# ----------------------------- SCHEMA --------------------------------------

# Event + schema versioning
MEMORY_ENTRY_EVENT_TYPE = "memory_entry"
MEMORY_ENTRY_SCHEMA_VERSION = 1

# Forward-compat policy:
# - Readers should accept rows with schema_version >= 1
# - Unknown fields must be ignored (do not fail hard)
MIN_ACCEPTED_SCHEMA_VERSION = 1

# Read-only / immutable contract guidance:
# These fields MUST NOT change for an existing trade_id once written.
# If they change, you are mixing identities and poisoning learning.
MEMORY_ENTRY_IMMUTABLE_KEYS: Tuple[str, ...] = (
    "schema_version",
    "event_type",
    "trade_id",
    "entry_id",
    "ts_ms",
    "account_label",
    "symbol",
    "timeframe",
    "strategy",
    "setup_type",
    "policy_hash",
    "setup_fingerprint",
    "memory_fingerprint",
    "memory_id",
)

# Mutable keys (safe to update if you truly must; prefer append-only pipelines):
# Example: outcome fields can be updated if you later enrich final status.
MEMORY_ENTRY_MUTABLE_KEYS: Tuple[str, ...] = (
    "decision",
    "outcome",
)

# ----------------------------- LIFECYCLE / CAPS ----------------------------

# These are contract-level defaults. Actual enforcement can live in workers,
# but the constants belong here so every component agrees on the boundaries.

# Keep memory bounded: you can change these later, but don't pretend infinite is fine.
DEFAULT_MAX_MEMORY_ENTRIES_JSONL: int = 2_000_000  # soft cap; worker may rotate/archive
DEFAULT_MAX_MEMORY_DB_ROWS: int = 2_000_000        # soft cap; worker may prune by ts_ms

# If a memory read is used for execution gating, do not scan infinite history.
DEFAULT_QUERY_LOOKBACK_DAYS: int = 180

# ----------------------------- TIME ----------------------------------------

def now_ms() -> int:
    return int(time.time() * 1000)

def get_ts_ms(obj: Dict[str, Any], default: Optional[int] = None) -> int:
    v = obj.get("ts_ms", obj.get("ts"))
    try:
        iv = int(v)
        if 0 < iv < 10_000_000_000:
            return iv * 1000
        return iv
    except Exception:
        return default if default is not None else now_ms()

# ----------------------------- NORMALIZATION -------------------------------

def normalize_symbol(sym: Any) -> Optional[str]:
    try:
        s = str(sym).strip().upper()
        return s or None
    except Exception:
        return None

def normalize_timeframe(tf: Any) -> Optional[str]:
    try:
        s = str(tf).strip().lower()
        if not s:
            return None
        if s.endswith(("m", "h", "d", "w")):
            return s
        n = int(float(s))
        return f"{n}m" if n > 0 else None
    except Exception:
        return None

# ----------------------------- JSONL IO ------------------------------------

def iter_jsonl(path: Path, *, max_lines: Optional[int] = None) -> Iterable[Dict[str, Any]]:
    """
    Streaming JSONL iterator. Fail-soft: skips parse errors.
    """
    if not path.exists():
        return
    n = 0
    if _HAS_ORJSON:
        with path.open("rb") as f:
            for raw in f:
                if max_lines and n >= max_lines:
                    break
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    obj = orjson.loads(raw)  # type: ignore[name-defined]
                except Exception:
                    continue
                if isinstance(obj, dict):
                    yield obj
                    n += 1
    else:  # pragma: no cover
        import json as _json2
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                if max_lines and n >= max_lines:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = _json2.loads(line)
                except Exception:
                    continue
                if isinstance(obj, dict):
                    yield obj
                    n += 1

def append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    """
    Append one JSON row to JSONL. Fail-soft.
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        if _HAS_ORJSON:
            with path.open("ab") as f:
                f.write(orjson.dumps(row))  # type: ignore[name-defined]
                f.write(b"\n")
        else:  # pragma: no cover
            import json as _json2
            with path.open("a", encoding="utf-8") as f:
                f.write(_json2.dumps(row))
                f.write("\n")
    except Exception:
        return

# ----------------------------- CONTRACT PATHS -------------------------------

@dataclass(frozen=True)
class ContractPaths:
    setups_path: Path
    outcomes_path: Path
    decisions_path: Path
    memory_entries_path: Path
    memory_index_path: Path

    @classmethod
    def default(cls) -> "ContractPaths":
        state = ROOT / "state"
        return cls(
            setups_path=state / "ai_events" / "setups.jsonl",
            outcomes_path=state / "ai_events" / "outcomes.jsonl",
            decisions_path=state / "ai_decisions.jsonl",
            memory_entries_path=state / "ai_memory" / "memory_entries.jsonl",
            memory_index_path=state / "ai_memory" / "memory_index.sqlite",
        )

# ----------------------------- VALIDATORS ----------------------------------

def validate_setup_record(ev: Dict[str, Any]) -> Tuple[bool, str]:
    if ev.get("event_type") != "setup_context":
        return False, "bad_event_type"
    if not str(ev.get("trade_id") or "").strip():
        return False, "missing_trade_id"
    if not normalize_symbol(ev.get("symbol")):
        return False, "missing_symbol"
    if not normalize_timeframe(ev.get("timeframe")):
        return False, "missing_timeframe"
    policy = ev.get("policy")
    if not isinstance(policy, dict) or not str(policy.get("policy_hash") or "").strip():
        return False, "missing_policy_hash"
    payload = ev.get("payload")
    if not isinstance(payload, dict) or not isinstance(payload.get("features"), dict):
        return False, "missing_payload_features"
    return True, "ok"

def validate_outcome_enriched(ev: Dict[str, Any]) -> Tuple[bool, str]:
    if ev.get("event_type") != "outcome_enriched":
        return False, "not_outcome_enriched"
    if not str(ev.get("trade_id") or "").strip():
        return False, "missing_trade_id"
    if not isinstance(ev.get("setup"), dict):
        return False, "missing_setup"
    if not isinstance(ev.get("outcome"), dict):
        return False, "missing_outcome"
    return True, "ok"

def validate_decision_record(ev: Dict[str, Any]) -> Tuple[bool, str]:
    # Decision schema here refers to decision rows in state/ai_decisions.jsonl
    if not str(ev.get("trade_id") or "").strip():
        return False, "missing_trade_id"
    if not isinstance(ev.get("allow"), bool):
        return False, "missing_allow"
    try:
        float(ev.get("size_multiplier"))
    except Exception:
        return False, "bad_size_multiplier"
    return True, "ok"

def validate_memory_entry(ev: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Contract-level validator for memory_entry rows.
    Must be strict enough to prevent garbage learning, but not brittle.

    Requirements:
      - event_type == "memory_entry"
      - schema_version >= MIN_ACCEPTED_SCHEMA_VERSION
      - trade_id, entry_id, ts_ms present
      - symbol/timeframe normalized
      - policy_hash + memory_id + memory_fingerprint present
      - decision.allow bool + decision.size_multiplier float
      - outcome object present (can contain None values)
    """
    try:
        if ev.get("event_type") != MEMORY_ENTRY_EVENT_TYPE:
            return False, "bad_event_type"
        sv = int(ev.get("schema_version") or 0)
        if sv < MIN_ACCEPTED_SCHEMA_VERSION:
            return False, "bad_schema_version"

        if not str(ev.get("trade_id") or "").strip():
            return False, "missing_trade_id"
        if not str(ev.get("entry_id") or "").strip():
            return False, "missing_entry_id"
        ts = ev.get("ts_ms")
        try:
            _ = int(ts)
        except Exception:
            return False, "missing_ts_ms"

        if not normalize_symbol(ev.get("symbol")):
            return False, "missing_symbol"
        if not normalize_timeframe(ev.get("timeframe")):
            return False, "missing_timeframe"

        if not str(ev.get("policy_hash") or "").strip():
            return False, "missing_policy_hash"
        if not str(ev.get("memory_id") or "").strip():
            return False, "missing_memory_id"
        if not str(ev.get("memory_fingerprint") or "").strip():
            return False, "missing_memory_fingerprint"

        d = ev.get("decision")
        if not isinstance(d, dict):
            return False, "missing_decision"
        if not isinstance(d.get("allow"), bool):
            return False, "missing_decision_allow"
        try:
            float(d.get("size_multiplier"))
        except Exception:
            return False, "bad_decision_size_multiplier"

        o = ev.get("outcome")
        if not isinstance(o, dict):
            return False, "missing_outcome_obj"

        return True, "ok"
    except Exception:
        return False, "exception"

# ----------------------------- FINGERPRINTING -------------------------------

def _stable_json(obj: Any) -> str:
    """
    Deterministic JSON string. Must be stable across runs.
    """
    try:
        return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        try:
            return str(obj)
        except Exception:
            return ""

def _filter_features_for_fingerprint(features: Dict[str, Any]) -> Dict[str, Any]:
    """
    Conservative filter for stable fingerprint inputs.

    Must match the spirit of ai_events_spine:
      - remove volatile blobs / prices / timestamps / orderbook/trades
      - remove recursive fingerprint keys
    """
    if not isinstance(features, dict):
        return {}

    f = dict(features)

    # volatile / frequently-changing / large blobs
    for k in (
        "ts", "timestamp", "updated_ms",
        "price", "last", "mark", "index",
        "best_bid", "best_ask",
        "orderbook", "trades",
    ):
        f.pop(k, None)

    # critical: avoid recursion/poisoning
    f.pop("setup_fingerprint", None)
    f.pop("memory_fingerprint", None)

    return f

def _compute_memory_fingerprint_from_setup(setup: Dict[str, Any]) -> Optional[str]:
    """
    Backfill memory_fingerprint deterministically from a setup_context event.

    Identity (matches ai_events_spine logic):
      sha256(stable_json({
        symbol, account_label, strategy, setup_type, timeframe, features(filtered)
      }))
    """
    if not isinstance(setup, dict):
        return None

    payload = setup.get("payload") if isinstance(setup.get("payload"), dict) else {}
    features = payload.get("features") if isinstance(payload.get("features"), dict) else {}
    if not isinstance(features, dict):
        features = {}

    sym = normalize_symbol(setup.get("symbol"))
    acct = str(setup.get("account_label") or "").strip() or None
    strategy = str(setup.get("strategy") or setup.get("strategy_name") or "").strip() or None
    setup_type = setup.get("setup_type")
    tf = normalize_timeframe(setup.get("timeframe"))

    # fallback timeframe from payload.extra.timeframe (common in your pipeline)
    if tf is None:
        extra = payload.get("extra") if isinstance(payload.get("extra"), dict) else {}
        tf = normalize_timeframe(extra.get("timeframe"))

    # we need enough signal to be deterministic and useful
    if not sym or not acct or not strategy or not tf:
        return None

    core = {
        "symbol": sym,
        "account_label": acct,
        "strategy": strategy,
        "setup_type": str(setup_type) if setup_type is not None else None,
        "timeframe": tf,
        "features": _filter_features_for_fingerprint(features),
    }

    h = hashlib.sha256()
    h.update(_stable_json(core).encode("utf-8", errors="ignore"))
    return h.hexdigest()

def extract_fingerprints_from_setup(setup: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (setup_fingerprint, memory_fingerprint).

    Backward compatible:
    - If memory_fingerprint missing, attempt deterministic backfill from setup_context.
    """
    payload = setup.get("payload") if isinstance(setup.get("payload"), dict) else {}
    feats = payload.get("features") if isinstance(payload.get("features"), dict) else {}

    sfp = feats.get("setup_fingerprint") if isinstance(feats, dict) else None
    mfp = feats.get("memory_fingerprint") if isinstance(feats, dict) else None

    sfp_s = str(sfp).strip() if sfp else None
    mfp_s = str(mfp).strip() if mfp else None

    if not mfp_s:
        mfp_s = _compute_memory_fingerprint_from_setup(setup)

    return (sfp_s, mfp_s)

# ----------------------------- CANARY CONTROLS ------------------------------

def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "on")

def canary_enabled() -> bool:
    """
    Global canary switch.
    If false: memory-driven execution MUST NOT happen.
    """
    return _env_bool("FB_CANARY_ENABLED", default=False)

def canary_accounts() -> List[str]:
    """
    Comma-separated allowlist. Example:
      FB_CANARY_ACCOUNTS=flashback07,flashback09
    Empty => no accounts are canary.
    """
    raw = os.getenv("FB_CANARY_ACCOUNTS", "")
    items = [x.strip() for x in raw.split(",") if x.strip()]
    return items

def is_canary_account(account_label: Optional[str]) -> bool:
    if not account_label:
        return False
    acct = str(account_label).strip()
    allow = set(canary_accounts())
    return acct in allow

# ----------------------------- SQLITE READ HELPERS --------------------------

def _connect_readonly_sqlite(db_path: Path) -> sqlite3.Connection:
    """
    Best-effort readonly SQLite connection.
    Uses uri mode to request read-only access.
    """
    # If file doesn't exist, caller handles.
    uri = f"file:{db_path.as_posix()}?mode=ro"
    return sqlite3.connect(uri, uri=True)

def sqlite_query_one(db_path: Path, sql: str, params: Sequence[Any] = ()) -> Optional[Dict[str, Any]]:
    """
    Returns first row as dict (column->value) or None.
    Fail-soft.
    """
    try:
        if not db_path.exists():
            return None
        conn = _connect_readonly_sqlite(db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.execute(sql, params)
        row = cur.fetchone()
        conn.close()
        if row is None:
            return None
        return dict(row)
    except Exception:
        return None

def sqlite_query_many(db_path: Path, sql: str, params: Sequence[Any] = (), limit: int = 200) -> List[Dict[str, Any]]:
    """
    Returns up to `limit` rows as dicts.
    Fail-soft.
    """
    out: List[Dict[str, Any]] = []
    try:
        if not db_path.exists():
            return out
        conn = _connect_readonly_sqlite(db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.execute(sql, params)
        n = 0
        for row in cur:
            out.append(dict(row))
            n += 1
            if limit and n >= limit:
                break
        conn.close()
        return out
    except Exception:
        return out
def read_memory_rows_by_symbol_tf(db_path: Path, symbol: str, timeframe: str, *, limit: int = 200) -> List[Dict[str, Any]]:
    """
    Read memory_entries rows by (symbol, timeframe) (SQLite index).
    """
    sym = normalize_symbol(symbol)
    tf = normalize_timeframe(timeframe)
    if not sym or not tf:
        return []
    return sqlite_query_many(
        db_path,
        "SELECT * FROM memory_entries WHERE symbol = ? AND timeframe = ? ORDER BY ts_ms DESC",
        (sym, tf),
        limit=limit,
    )

# ----------------------------- EXPORTS -------------------------------------

__all__ = [
    # paths / IO
    "ContractPaths",
    "iter_jsonl",
    "append_jsonl",
    # time / normalize
    "now_ms",
    "get_ts_ms",
    "normalize_symbol",
    "normalize_timeframe",
    # validators
    "validate_setup_record",
    "validate_outcome_enriched",
    "validate_decision_record",
    "validate_memory_entry",
    # schema
    "MEMORY_ENTRY_EVENT_TYPE",
    "MEMORY_ENTRY_SCHEMA_VERSION",
    "MIN_ACCEPTED_SCHEMA_VERSION",
    "MEMORY_ENTRY_IMMUTABLE_KEYS",
    "MEMORY_ENTRY_MUTABLE_KEYS",
    # lifecycle caps
    "DEFAULT_MAX_MEMORY_ENTRIES_JSONL",
    "DEFAULT_MAX_MEMORY_DB_ROWS",
    "DEFAULT_QUERY_LOOKBACK_DAYS",
    # fingerprinting
    "extract_fingerprints_from_setup",
    # canary
    "canary_enabled",
    "canary_accounts",
    "is_canary_account",
    # sqlite reads
    "sqlite_query_one",
    "sqlite_query_many",
    "read_memory_rows_by_memory_id",
    "read_memory_rows_by_symbol_tf",
]
