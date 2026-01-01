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

MEMORY_ENTRY_EVENT_TYPE = "memory_entry"
MEMORY_ENTRY_SCHEMA_VERSION = 1

MIN_ACCEPTED_SCHEMA_VERSION = 1

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

MEMORY_ENTRY_MUTABLE_KEYS: Tuple[str, ...] = (
    "decision",
    "outcome",
)

# ----------------------------- LIFECYCLE / CAPS ----------------------------

DEFAULT_MAX_MEMORY_ENTRIES_JSONL: int = 2_000_000
DEFAULT_MAX_MEMORY_DB_ROWS: int = 2_000_000
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


# ----------------------------- SHAPE TOLERANCE -----------------------------


def _as_dict(x: Any) -> Dict[str, Any]:
    return x if isinstance(x, dict) else {}


def _get_setup_like(obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Accept either:
      - setup_context record itself
      - outcome_enriched envelope with {"setup": {...}}
      - random wrappers that include a "setup" dict
    """
    if not isinstance(obj, dict):
        return {}
    setup = obj.get("setup")
    if isinstance(setup, dict):
        return setup
    return obj


def _get_payload_dict(setup_like: Dict[str, Any]) -> Dict[str, Any]:
    """
    Tolerate payload nesting:
      payload
      payload.payload
    """
    p = setup_like.get("payload")
    if isinstance(p, dict):
        inner = p.get("payload")
        if isinstance(inner, dict):
            return inner
        return p
    return {}


def _get_features_dict(setup_like: Dict[str, Any]) -> Dict[str, Any]:
    """
    Tolerate features path drift across modules:
      payload.features
      payload.payload.features
    """
    payload = _get_payload_dict(setup_like)

    feats = payload.get("features")
    if isinstance(feats, dict):
        return feats

    p2 = payload.get("payload")
    if isinstance(p2, dict):
        feats2 = p2.get("features")
        if isinstance(feats2, dict):
            return feats2

    return {}


def _get_extra_dict(setup_like: Dict[str, Any]) -> Dict[str, Any]:
    """
    Tolerate payload.extra nesting:
      payload.extra
      payload.payload.extra
    """
    payload = _get_payload_dict(setup_like)

    extra = payload.get("extra")
    if isinstance(extra, dict):
        return extra

    p2 = payload.get("payload")
    if isinstance(p2, dict):
        extra2 = p2.get("extra")
        if isinstance(extra2, dict):
            return extra2

    return {}


def _infer_account_label(setup_like: Dict[str, Any]) -> Optional[str]:
    v = setup_like.get("account_label") or setup_like.get("label") or setup_like.get("account")
    s = str(v).strip() if v is not None else ""
    return s or None


def _infer_strategy(setup_like: Dict[str, Any]) -> Optional[str]:
    v = setup_like.get("strategy") or setup_like.get("strategy_name")
    s = str(v).strip() if v is not None else ""
    return s or None


def _infer_timeframe(setup_like: Dict[str, Any]) -> Optional[str]:
    tf = normalize_timeframe(setup_like.get("timeframe"))
    if tf:
        return tf
    extra = _get_extra_dict(setup_like)
    tf2 = normalize_timeframe(extra.get("timeframe"))
    if tf2:
        return tf2
    return None


# ----------------------------- JSONL IO ------------------------------------


def _trim_json_bytes(raw: bytes) -> bytes:
    """
    Defensive cleanup for JSONL lines.

    Handles:
    - UTF-8 BOM at start
    - trailing NULs
    """
    if not raw:
        return raw

    raw = raw.strip()

    if raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:].lstrip()

    raw = raw.rstrip(b"\x00").rstrip()
    return raw


def iter_jsonl(path: Path, *, max_lines: Optional[int] = None) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        return
    n = 0

    if _HAS_ORJSON:
        with path.open("rb") as f:
            for raw in f:
                if max_lines and n >= max_lines:
                    break

                raw = _trim_json_bytes(raw)
                if not raw:
                    continue

                obj: Any = None
                try:
                    obj = orjson.loads(raw)  # type: ignore[name-defined]
                except Exception:
                    try:
                        last = raw.rfind(b"}")
                        if last != -1:
                            raw2 = raw[: last + 1]
                            obj = orjson.loads(raw2)  # type: ignore[name-defined]
                    except Exception:
                        obj = None

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
                    try:
                        last = line.rfind("}")
                        if last != -1:
                            obj = _json2.loads(line[: last + 1])
                        else:
                            continue
                    except Exception:
                        continue
                if isinstance(obj, dict):
                    yield obj
                    n += 1


def append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    """
    Generic JSONL appender.

    NOTE:
    - MUST NOT be used as a "decision writer". Decisions must go through
      app.core.ai_decision_logger.append_decision (single-writer law).
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)

        if _HAS_ORJSON:
            line = orjson.dumps(row) + b"\n"  # type: ignore[name-defined]
        else:  # pragma: no cover
            line = (json.dumps(row) + "\n").encode("utf-8", errors="ignore")

        import os as _os

        fd = _os.open(str(path), _os.O_APPEND | _os.O_CREAT | _os.O_WRONLY, 0o666)
        try:
            _os.write(fd, line)
        finally:
            _os.close(fd)
    except Exception:
        return


# ----------------------------- CONTRACT PATHS -------------------------------

# ----------------------------- CONTRACT PATHS -------------------------------

from dataclasses import dataclass

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

        try:
            _ = int(ev.get("ts_ms"))
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
    try:
        return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        try:
            return str(obj)
        except Exception:
            return ""


def _filter_features_for_fingerprint(features: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(features, dict):
        return {}

    f = dict(features)

    for k in (
        "ts",
        "timestamp",
        "updated_ms",
        "price",
        "last",
        "mark",
        "index",
        "best_bid",
        "best_ask",
        "orderbook",
        "trades",
    ):
        f.pop(k, None)

    f.pop("setup_fingerprint", None)
    f.pop("memory_fingerprint", None)

    return f


def _compute_memory_fingerprint_from_setup(setup: Dict[str, Any]) -> Optional[str]:
    """
    Compute a trade_id-free fingerprint from a setup-like object.
    """
    if not isinstance(setup, dict):
        return None

    setup_like = _get_setup_like(setup)
    features = _get_features_dict(setup_like)

    sym = normalize_symbol(setup_like.get("symbol"))
    acct = _infer_account_label(setup_like)
    strategy = _infer_strategy(setup_like)
    setup_type = setup_like.get("setup_type")
    tf = _infer_timeframe(setup_like)

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
    Extract (setup_fingerprint, memory_fingerprint) from setup-like records.
    If memory_fingerprint is missing, compute it deterministically.
    """
    setup_like = _get_setup_like(setup)
    feats = _get_features_dict(setup_like)

    sfp = feats.get("setup_fingerprint") if isinstance(feats, dict) else None
    mfp = feats.get("memory_fingerprint") if isinstance(feats, dict) else None

    sfp_s = str(sfp).strip() if sfp else None
    mfp_s = str(mfp).strip() if mfp else None

    if not mfp_s:
        mfp_s = _compute_memory_fingerprint_from_setup(setup_like)

    return (sfp_s, mfp_s)


# ----------------------------- CANARY CONTROLS ------------------------------


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def canary_enabled() -> bool:
    return _env_bool("FB_CANARY_ENABLED", default=False)


def canary_accounts() -> List[str]:
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
    uri = f"file:{db_path.as_posix()}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def sqlite_query_one(db_path: Path, sql: str, params: Sequence[Any] = ()) -> Optional[Dict[str, Any]]:
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


def read_memory_rows_by_memory_id(db_path: Path, memory_id: str, *, limit: int = 200) -> List[Dict[str, Any]]:
    mid = str(memory_id or "").strip()
    if not mid:
        return []
    return sqlite_query_many(
        db_path,
        "SELECT * FROM memory_entries WHERE memory_id = ? ORDER BY ts_ms DESC",
        (mid,),
        limit=limit,
    )


def read_memory_rows_by_symbol_tf(db_path: Path, symbol: str, timeframe: str, *, limit: int = 200) -> List[Dict[str, Any]]:
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
    "ContractPaths",
    "iter_jsonl",
    "append_jsonl",
    "now_ms",
    "get_ts_ms",
    "normalize_symbol",
    "normalize_timeframe",
    "validate_setup_record",
    "validate_outcome_enriched",
    "validate_decision_record",
    "validate_memory_entry",
    "MEMORY_ENTRY_EVENT_TYPE",
    "MEMORY_ENTRY_SCHEMA_VERSION",
    "MIN_ACCEPTED_SCHEMA_VERSION",
    "MEMORY_ENTRY_IMMUTABLE_KEYS",
    "MEMORY_ENTRY_MUTABLE_KEYS",
    "DEFAULT_MAX_MEMORY_ENTRIES_JSONL",
    "DEFAULT_MAX_MEMORY_DB_ROWS",
    "DEFAULT_QUERY_LOOKBACK_DAYS",
    "extract_fingerprints_from_setup",
    "canary_enabled",
    "canary_accounts",
    "is_canary_account",
    "sqlite_query_one",
    "sqlite_query_many",
    "read_memory_rows_by_memory_id",
    "read_memory_rows_by_symbol_tf",
]
