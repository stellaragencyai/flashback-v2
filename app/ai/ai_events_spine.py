#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Events Spine (disk-logging + inbox-draining version, v3.0.0)

What this file is (now):
- The canonical consumer that drains the per-account AI events inbox:
      state/ai_events_inbox_<ACCOUNT_LABEL>.jsonl
  using a byte-offset cursor file:
      state/ai_events_inbox_<ACCOUNT_LABEL>.cursor

- Appends every accepted event to the per-lane ledger:
      state/ai_events/<ACCOUNT_LABEL>/spine_events.jsonl

- Also performs Phase 4 glue:
  - setup_context -> pending_setups registry (alias-aware)
  - outcome_record -> reconciliation against pending_setups
  - outcome_enriched -> memory emission + memory snapshot update
  - decision enforcement (pilot_decide / fallback decision)

Producer contract:
- Other workers should call publish_ai_event(event)
  which ONLY enqueues into the correct inbox file (lane-safe).
  The spine drains and processes.

CLI:
- Default: loop forever, draining and heartbeating.
- --once: drain once and exit (useful for tests).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Set, List

import orjson

from app.core.bus_types import (  # type: ignore
    ai_events_bus,
    memory_bus,
    SetupRecord,
    OutcomeRecord,
)

# ---------------------------------------------------------------------------
# Logging (robust) & heartbeat
# ---------------------------------------------------------------------------

try:
    from app.core.log import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging
    import sys

    def get_logger(name: str) -> "logging.Logger":  # type: ignore
        logger_ = logging.getLogger(name)
        if not logger_.handlers:
            handler = logging.StreamHandler(sys.stdout)
            fmt = logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
            handler.setFormatter(fmt)
            logger_.addHandler(handler)
        logger_.setLevel(logging.INFO)
        return logger_

log = get_logger("ai_events_spine")

try:
    from app.core.flashback_common import record_heartbeat  # type: ignore
except Exception:  # pragma: no cover
    def record_heartbeat(name: str) -> None:  # type: ignore[override]
        return None

try:
    from app.core.config import settings  # type: ignore
    ROOT: Path = settings.ROOT  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[2]


def _now_ms() -> int:
    return int(time.time() * 1000)


def _env_bool(key: str, default: str = "false") -> bool:
    v = str(os.getenv(key, default)).strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def _safe_str(x: Any) -> str:
    try:
        return ("" if x is None else str(x)).strip()
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# Lane identity + directories
# ---------------------------------------------------------------------------

ACCOUNT_LABEL: str = _safe_str(os.getenv("ACCOUNT_LABEL") or "main") or "main"
IS_MAIN: bool = ACCOUNT_LABEL.lower() in ("main", "global")


STATE_DIR: Path = ROOT / "state"

# AI_EVENTS_DIR supports env override; if relative, resolve under STATE_DIR.
_env_events_dir = _safe_str(os.getenv("AI_EVENTS_DIR"))
if _env_events_dir:
    _tmp = Path(_env_events_dir)
    AI_EVENTS_DIR = _tmp if _tmp.is_absolute() else (STATE_DIR / _tmp)
else:
    # canonical per-lane path
    AI_EVENTS_DIR = (STATE_DIR / "ai_events") if IS_MAIN else (STATE_DIR / "ai_events" / ACCOUNT_LABEL)

AI_EVENTS_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)

# Memory dir supports env override; if relative, resolve under STATE_DIR.
_env_mem_dir = _safe_str(os.getenv("AI_MEMORY_DIR"))
if _env_mem_dir:
    _tmpm = Path(_env_mem_dir)
    AI_MEMORY_DIR = _tmpm if _tmpm.is_absolute() else (STATE_DIR / _tmpm)
else:
    AI_MEMORY_DIR = (STATE_DIR / "ai_memory") if IS_MAIN else (STATE_DIR / "ai_memory" / ACCOUNT_LABEL)
AI_MEMORY_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Canonical file outputs (lane-scoped)
# ---------------------------------------------------------------------------

SPINE_EVENTS_PATH: Path = AI_EVENTS_DIR / "spine_events.jsonl"         # all accepted events ledger
SETUPS_PATH: Path = AI_EVENTS_DIR / "setups.jsonl"                    # setup_context (optional convenience)
OUTCOMES_PATH: Path = AI_EVENTS_DIR / "outcomes.jsonl"                # legacy enriched/flat outcomes
OUTCOMES_RAW_PATH: Path = AI_EVENTS_DIR / "outcomes_raw.jsonl"        # raw outcome_record events

PENDING_REGISTRY_PATH: Path = AI_EVENTS_DIR / "pending_setups.json"

MEMORY_SNAPSHOT_PATH: Path = AI_MEMORY_DIR / "memory_snapshot.json"
MEMORY_RECORDS_PATH: Path = AI_MEMORY_DIR / "memory_records.jsonl"

# ---------------------------------------------------------------------------
# Inbox + cursor (lane-scoped, but overridable)
# ---------------------------------------------------------------------------

# Inbox path: main keeps legacy name ai_events_inbox.jsonl; subs use ai_events_inbox_<label>.jsonl
_env_inbox = _safe_str(os.getenv("AI_EVENTS_INBOX_PATH"))
if _env_inbox:
    _p = Path(_env_inbox)
    AI_EVENTS_INBOX_PATH = _p if _p.is_absolute() else (STATE_DIR / _p)
else:
    AI_EVENTS_INBOX_PATH = (STATE_DIR / "ai_events_inbox.jsonl") if IS_MAIN else (STATE_DIR / f"ai_events_inbox_{ACCOUNT_LABEL}.jsonl")

# Cursor path: main keeps legacy name ai_events_inbox.cursor; subs use ai_events_inbox_<label>.cursor
_env_cursor = _safe_str(os.getenv("AI_EVENTS_INBOX_CURSOR_PATH"))
if _env_cursor:
    _c = Path(_env_cursor)
    AI_EVENTS_INBOX_CURSOR_PATH = _c if _c.is_absolute() else (STATE_DIR / _c)
else:
    AI_EVENTS_INBOX_CURSOR_PATH = (STATE_DIR / "ai_events_inbox.cursor") if IS_MAIN else (STATE_DIR / f"ai_events_inbox_{ACCOUNT_LABEL}.cursor")

# Badlines quarantine (so one garbage line doesn't kill the lane forever)
AI_EVENTS_INBOX_BADLINES_PATH: Path = (
    (STATE_DIR / "ai_events_inbox.bad.jsonl") if IS_MAIN else (STATE_DIR / f"ai_events_inbox_{ACCOUNT_LABEL}.bad.jsonl")
)

# ---------------------------------------------------------------------------
# Config / policy stamping
# ---------------------------------------------------------------------------

CONFIG_DIR: Path = ROOT / "config"
STRATEGIES_PATH: Path = CONFIG_DIR / "strategies.yaml"
EXIT_PROFILES_PATH: Path = CONFIG_DIR / "exit_profiles.yaml"
RISK_PROFILES_PATH: Path = CONFIG_DIR / "risk_profiles.yaml"

_POLICY_CACHE: Dict[str, Any] = {"policy": None, "loaded_ms": 0}
_POLICY_CACHE_TTL_MS = 10_000

# ---------------------------------------------------------------------------
# Phase 4: Memory store (bounded + reversible)
# ---------------------------------------------------------------------------

MEMORY_SCHEMA_VERSION = 1

MEM_MAX_RECORDS = 50_000
MEM_MAX_AGE_DAYS = 180
MEM_MAX_NOTES_LEN = 512

PEND_MAX_COUNT = 5_000
PEND_MAX_AGE_DAYS = 14

# ---------------------------------------------------------------------------
# Phase 4: Decisions (enforced)
# ---------------------------------------------------------------------------

AI_DECISIONS_PATH: Path = (STATE_DIR / "ai_decisions.jsonl")
AI_EVENTS_ENFORCE_DECISION: bool = _env_bool("AI_EVENTS_ENFORCE_DECISION", "true")
AI_DECISION_TAIL_BYTES: int = int(os.getenv("AI_DECISION_TAIL_BYTES", "1048576") or "1048576")  # default 1MB


# ---------------------------------------------------------------------------
# IO helpers
# ---------------------------------------------------------------------------

def _append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("ab") as f:
            f.write(orjson.dumps(payload))
            f.write(b"\n")
    except Exception as e:
        try:
            log.warning("[ai_events] Failed to append event to %s: %r", path, e)
        except Exception:
            pass


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def _read_cursor(path: Path) -> int:
    try:
        if not path.exists():
            return 0
        raw = path.read_text(encoding="utf-8", errors="ignore").strip()
        if not raw:
            return 0
        v = int(raw)
        return max(0, v)
    except Exception:
        return 0


def _write_cursor(path: Path, pos: int) -> None:
    try:
        _atomic_write_text(path, str(max(0, int(pos))))
    except Exception:
        return


# ---------------------------------------------------------------------------
# Timeframe normalization + fingerprinting
# ---------------------------------------------------------------------------

def _normalize_timeframe(tf: Any) -> Optional[str]:
    if tf is None:
        return None
    try:
        s = str(tf).strip().lower()
    except Exception:
        return None
    if not s:
        return None

    if s.endswith(("m", "h", "d", "w")):
        return s

    try:
        n = int(float(s))
        if n > 0:
            return f"{n}m"
    except Exception:
        return None

    return None


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
        "ts", "timestamp", "updated_ms",
        "price", "last", "mark", "index",
        "best_bid", "best_ask",
        "orderbook", "trades",
    ):
        f.pop(k, None)

    f.pop("setup_fingerprint", None)
    f.pop("memory_fingerprint", None)

    return f


def _compute_setup_fingerprint(
    *,
    trade_id: str,
    symbol: str,
    account_label: str,
    strategy: str,
    setup_type: Optional[str],
    timeframe: Optional[str],
    features: Dict[str, Any],
) -> str:
    core = {
        "trade_id": str(trade_id),
        "symbol": str(symbol).upper(),
        "account_label": str(account_label),
        "strategy": str(strategy),
        "setup_type": str(setup_type) if setup_type is not None else None,
        "timeframe": timeframe,
        "features": _filter_features_for_fingerprint(features),
    }
    h = hashlib.sha256()
    h.update(_stable_json(core).encode("utf-8", errors="ignore"))
    return h.hexdigest()


def _compute_memory_fingerprint(
    *,
    symbol: str,
    account_label: str,
    strategy: str,
    setup_type: Optional[str],
    timeframe: Optional[str],
    features: Dict[str, Any],
) -> str:
    core = {
        "symbol": str(symbol).upper(),
        "account_label": str(account_label),
        "strategy": str(strategy),
        "setup_type": str(setup_type) if setup_type is not None else None,
        "timeframe": timeframe,
        "features": _filter_features_for_fingerprint(features),
    }
    h = hashlib.sha256()
    h.update(_stable_json(core).encode("utf-8", errors="ignore"))
    return h.hexdigest()


def _ensure_setup_fingerprint(event: Dict[str, Any]) -> None:
    if not isinstance(event, dict):
        return
    if event.get("event_type") != "setup_context":
        return

    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    features = payload.get("features") if isinstance(payload.get("features"), dict) else {}
    if not isinstance(features, dict):
        features = {}

    tf = _normalize_timeframe(event.get("timeframe"))
    if tf is None:
        extra = payload.get("extra") if isinstance(payload.get("extra"), dict) else {}
        tf = _normalize_timeframe(extra.get("timeframe"))

    if tf is not None:
        event["timeframe"] = tf
        extra = payload.get("extra") if isinstance(payload.get("extra"), dict) else {}
        extra["timeframe"] = tf
        payload["extra"] = extra
        event["payload"] = payload

    tid = _safe_str(event.get("trade_id"))
    sym = _safe_str(event.get("symbol"))
    acct = _safe_str(event.get("account_label") or "main") or "main"
    strat = _safe_str(event.get("strategy") or event.get("strategy_name") or "unknown") or "unknown"
    stype = event.get("setup_type")
    tf_final = tf

    if not features.get("setup_fingerprint"):
        fp = _compute_setup_fingerprint(
            trade_id=tid,
            symbol=sym,
            account_label=acct,
            strategy=strat,
            setup_type=str(stype) if stype is not None else None,
            timeframe=tf_final,
            features=features,
        )
        features["setup_fingerprint"] = fp

    if not features.get("memory_fingerprint"):
        mfp = _compute_memory_fingerprint(
            symbol=sym,
            account_label=acct,
            strategy=strat,
            setup_type=str(stype) if stype is not None else None,
            timeframe=tf_final,
            features=features,
        )
        features["memory_fingerprint"] = mfp

    payload["features"] = features
    event["payload"] = payload


# ---------------------------------------------------------------------------
# Policy stamping (versions + hash)
# ---------------------------------------------------------------------------

def _safe_read_bytes(path: Path) -> bytes:
    try:
        return path.read_bytes()
    except Exception:
        return b""


def _extract_yaml_version(yaml_bytes: bytes) -> Optional[int]:
    try:
        txt = yaml_bytes.decode("utf-8", errors="ignore")
        for line in txt.splitlines():
            s = line.strip()
            if s.startswith("version:"):
                rest = s.split("version:", 1)[1].strip()
                rest = rest.strip('"').strip("'")
                try:
                    return int(rest)
                except Exception:
                    return None
        return None
    except Exception:
        return None


def _compute_policy() -> Dict[str, Any]:
    strategies_b = _safe_read_bytes(STRATEGIES_PATH)
    exits_b = _safe_read_bytes(EXIT_PROFILES_PATH)
    risks_b = _safe_read_bytes(RISK_PROFILES_PATH)

    strategies_version = _extract_yaml_version(strategies_b)
    exit_profiles_version = _extract_yaml_version(exits_b)
    risk_profiles_version = _extract_yaml_version(risks_b)

    h = hashlib.sha256()
    h.update(b"strategies.yaml\n")
    h.update(strategies_b)
    h.update(b"\nexit_profiles.yaml\n")
    h.update(exits_b)
    h.update(b"\nrisk_profiles.yaml\n")
    h.update(risks_b)

    policy_hash = h.hexdigest()

    return {
        "strategies_version": strategies_version,
        "exit_profiles_version": exit_profiles_version,
        "risk_profiles_version": risk_profiles_version,
        "policy_hash": policy_hash,
        "paths": {
            "strategies": str(STRATEGIES_PATH),
            "exit_profiles": str(EXIT_PROFILES_PATH),
            "risk_profiles": str(RISK_PROFILES_PATH),
        },
    }


def _get_policy_cached() -> Dict[str, Any]:
    now = _now_ms()
    cached = _POLICY_CACHE.get("policy")
    loaded_ms = int(_POLICY_CACHE.get("loaded_ms") or 0)
    if cached and (now - loaded_ms) < _POLICY_CACHE_TTL_MS:
        return cached  # type: ignore[return-value]
    policy = _compute_policy()
    _POLICY_CACHE["policy"] = policy
    _POLICY_CACHE["loaded_ms"] = now
    return policy


def _stamp_policy(event: Dict[str, Any]) -> None:
    try:
        if not isinstance(event, dict):
            return
        if "policy" in event:
            return
        event["policy"] = _get_policy_cached()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Phase 4: Decision enforcement helpers
# ---------------------------------------------------------------------------

def _decisions_file_has_trade_id(trade_id: str) -> bool:
    try:
        tid = _safe_str(trade_id)
        if not tid:
            return False
        if not AI_DECISIONS_PATH.exists():
            return False

        size = AI_DECISIONS_PATH.stat().st_size
        read_n = min(max(0, AI_DECISION_TAIL_BYTES), size)

        with AI_DECISIONS_PATH.open("rb") as f:
            if read_n < size:
                f.seek(size - read_n)
            chunk = f.read(read_n)

        for raw in chunk.splitlines():
            raw = raw.strip()
            if not raw:
                continue
            try:
                d = orjson.loads(raw)
            except Exception:
                continue
            if not isinstance(d, dict):
                continue
            if tid in (
                _safe_str(d.get("trade_id")),
                _safe_str(d.get("client_trade_id")),
                _safe_str(d.get("source_trade_id")),
            ):
                return True

        return False
    except Exception:
        return False


def _write_fallback_decision(setup_event: Dict[str, Any], reason: str) -> None:
    try:
        from app.core.ai_decision_logger import append_decision  # type: ignore
    except Exception:
        return

    try:
        tid = _safe_str(setup_event.get("trade_id"))
        sym = _safe_str(setup_event.get("symbol")).upper()
        acct = _safe_str(setup_event.get("account_label") or "main") or "main"
        tf = _normalize_timeframe(setup_event.get("timeframe")) or "unknown"
        pol = setup_event.get("policy") if isinstance(setup_event.get("policy"), dict) else {}
        policy_hash = _safe_str(pol.get("policy_hash")) or None

        payload: Dict[str, Any] = {
            "schema_version": 1,
            "ts": _now_ms(),
            "decision": "BLOCKED_BY_GATES",
            "tier_used": "NONE",
            "memory": None,
            "gates": {"reason": reason},
            "proposed_action": None,
            "trade_id": tid,
            "symbol": sym,
            "account_label": acct,
            "timeframe": tf,
        }
        if policy_hash:
            payload["policy_hash"] = policy_hash

        append_decision(payload)
    except Exception:
        return


def _ensure_decision_for_setup(setup_event: Dict[str, Any]) -> None:
    if not AI_EVENTS_ENFORCE_DECISION:
        return
    if not isinstance(setup_event, dict):
        return

    tid = _safe_str(setup_event.get("trade_id"))
    if not tid:
        return

    if _decisions_file_has_trade_id(tid):
        return

    try:
        from app.bots.ai_pilot import pilot_decide  # type: ignore
        pilot_decide(setup_event)
    except Exception as e:
        log.warning("[phase4] pilot_decide failed for trade_id=%s: %r (writing fallback)", tid, e)
        _write_fallback_decision(setup_event, reason="decision_missing")


# ---------------------------------------------------------------------------
# Pending registry + eviction + alias-aware reconciliation
# ---------------------------------------------------------------------------

def _load_pending() -> Dict[str, Any]:
    if not PENDING_REGISTRY_PATH.exists():
        return {}
    try:
        txt = PENDING_REGISTRY_PATH.read_text(encoding="utf-8", errors="ignore")
        data = json.loads(txt or "{}")
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _prune_pending(reg: Dict[str, Any]) -> Dict[str, Any]:
    try:
        now = _now_ms()
        max_age_ms = int(PEND_MAX_AGE_DAYS * 24 * 60 * 60 * 1000)

        items: List[Tuple[str, int, Any]] = []
        for k, v in reg.items():
            ts = None
            if isinstance(v, dict):
                ts = v.get("ts")
            try:
                ts_i = int(ts) if ts is not None else 0
            except Exception:
                ts_i = 0
            items.append((k, ts_i, v))

        items = [it for it in items if (now - it[1]) <= max_age_ms or it[1] == 0]
        items.sort(key=lambda x: x[1], reverse=True)
        items = items[:PEND_MAX_COUNT]
        return {k: v for (k, _ts, v) in items}
    except Exception:
        return reg


def _save_pending(reg: Dict[str, Any]) -> None:
    try:
        reg2 = _prune_pending(reg)
        _atomic_write_text(PENDING_REGISTRY_PATH, json.dumps(reg2, indent=2, sort_keys=True))
    except Exception as e:
        log.warning("[ai_events] Failed to save pending registry: %r", e)


def _extract_setup_alias_keys(setup_event: Dict[str, Any]) -> Set[str]:
    keys: Set[str] = set()
    if not isinstance(setup_event, dict):
        return keys

    tid = _safe_str(setup_event.get("trade_id"))
    if tid:
        keys.add(tid)
        if ":" in tid:
            keys.add(tid.split(":", 1)[1])

    payload = setup_event.get("payload") if isinstance(setup_event.get("payload"), dict) else {}
    extra = payload.get("extra") if isinstance(payload.get("extra"), dict) else {}
    feats = payload.get("features") if isinstance(payload.get("features"), dict) else {}

    for k in ("client_trade_id", "source_trade_id", "orderLinkId", "orderId"):
        v = _safe_str(extra.get(k))
        if v:
            keys.add(v)
            if ":" in v:
                keys.add(v.split(":", 1)[1])

    for k in ("client_trade_id", "source_trade_id", "orderLinkId", "orderId", "trade_id"):
        v = _safe_str(feats.get(k))
        if v:
            keys.add(v)
            if ":" in v:
                keys.add(v.split(":", 1)[1])

    return {k for k in keys if k}


def _find_pending_setup(pending: Dict[str, Any], trade_id: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    tid = _safe_str(trade_id)
    if not tid:
        return None, None

    hit = pending.get(tid)
    if isinstance(hit, dict):
        return hit, tid

    for k, v in pending.items():
        if not isinstance(v, dict):
            continue
        aliases = _extract_setup_alias_keys(v)
        if tid in aliases:
            return v, k

    return None, None


def _remove_pending_setup(pending: Dict[str, Any], setup_event: Dict[str, Any]) -> None:
    try:
        for k in _extract_setup_alias_keys(setup_event):
            pending.pop(k, None)
    except Exception:
        return


def _merge_setup_and_outcome(setup_event: Dict[str, Any], outcome_event: Dict[str, Any]) -> Dict[str, Any]:
    try:
        setup_payload = setup_event.get("payload", {}) or {}
        outcome_payload = outcome_event.get("payload", {}) or {}

        features = setup_payload.get("features", {}) or {}
        pnl_usd = outcome_payload.get("pnl_usd", 0.0)

        risk_usd = features.get("risk_usd")
        r_multiple = None
        if risk_usd is not None:
            try:
                r_multiple = float(pnl_usd) / float(risk_usd) if float(risk_usd) != 0 else None
            except Exception:
                r_multiple = None

        win: Optional[bool] = None
        if r_multiple is not None:
            win = r_multiple > 0

        enriched: Dict[str, Any] = {
            "event_type": "outcome_enriched",
            "ts": _now_ms(),
            "trade_id": setup_event.get("trade_id") or outcome_event.get("trade_id"),
            "symbol": setup_event.get("symbol") or outcome_event.get("symbol"),
            "account_label": setup_event.get("account_label") or outcome_event.get("account_label"),
            "strategy": setup_event.get("strategy") or outcome_event.get("strategy"),
            "setup_type": setup_event.get("setup_type"),
            "timeframe": setup_event.get("timeframe") or outcome_event.get("timeframe"),
            "ai_profile": setup_event.get("ai_profile"),
            "policy": setup_event.get("policy") or outcome_event.get("policy"),
            "setup": setup_event,
            "outcome": outcome_event,
            "extra": {
                "is_terminal": True,
                "final_status": "CLOSED",
                "setup_fingerprint": (features.get("setup_fingerprint") if isinstance(features, dict) else None),
                "memory_fingerprint": (features.get("memory_fingerprint") if isinstance(features, dict) else None),
            },
            "stats": {
                "pnl_usd": float(pnl_usd),
                "r_multiple": float(r_multiple) if r_multiple is not None else None,
                "win": win,
                "is_terminal": True,
            },
        }
        return enriched
    except Exception as e:
        log.warning("[ai_events] Failed to merge setup/outcome: %r", e)
        return outcome_event


# ---------------------------------------------------------------------------
# Phase 4: Memory store
# ---------------------------------------------------------------------------

def _load_memory_snapshot() -> Dict[str, Any]:
    if not MEMORY_SNAPSHOT_PATH.exists():
        return {}
    try:
        txt = MEMORY_SNAPSHOT_PATH.read_text(encoding="utf-8", errors="ignore")
        data = json.loads(txt or "{}")
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _prune_memory_snapshot(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    try:
        now = _now_ms()
        max_age_ms = int(MEM_MAX_AGE_DAYS * 24 * 60 * 60 * 1000)

        items = []
        for mid, rec in snapshot.items():
            updated = 0
            if isinstance(rec, dict):
                lifecycle = rec.get("lifecycle") if isinstance(rec.get("lifecycle"), dict) else {}
                try:
                    updated = int(lifecycle.get("updated_ts") or rec.get("ts") or 0)
                except Exception:
                    updated = 0
            items.append((mid, updated, rec))

        items = [it for it in items if (now - it[1]) <= max_age_ms or it[1] == 0]
        items.sort(key=lambda x: x[1], reverse=True)
        items = items[:MEM_MAX_RECORDS]
        return {mid: rec for (mid, _u, rec) in items}
    except Exception:
        return snapshot


def _save_memory_snapshot(snapshot: Dict[str, Any]) -> None:
    try:
        snap2 = _prune_memory_snapshot(snapshot)
        _atomic_write_text(MEMORY_SNAPSHOT_PATH, json.dumps(snap2, indent=2, sort_keys=True))
    except Exception as e:
        log.warning("[ai_memory] Failed to save snapshot: %r", e)


def _compute_memory_id(memory_fingerprint: str, policy_hash: str, account_scope: str, symbol_scope: str, timeframe: str) -> str:
    h = hashlib.sha256()
    h.update(
        _stable_json(
            {
                "memory_fingerprint": memory_fingerprint,
                "policy_hash": policy_hash,
                "account_scope": account_scope,
                "symbol_scope": symbol_scope,
                "timeframe": timeframe,
            }
        ).encode("utf-8", errors="ignore")
    )
    return h.hexdigest()


def _upsert_memory_record(
    *,
    snapshot: Dict[str, Any],
    memory_fingerprint: str,
    setup_fingerprint: str,
    policy_hash: str,
    timeframe: str,
    account_scope: str,
    symbol_scope: str,
    pnl: float,
    r_val: Optional[float],
    win_val: Optional[bool],
) -> Dict[str, Any]:
    now = _now_ms()
    memory_id = _compute_memory_id(memory_fingerprint, policy_hash, account_scope, symbol_scope, timeframe)

    rec = snapshot.get(memory_id) if isinstance(snapshot.get(memory_id), dict) else None
    if rec is None:
        rec = {
            "event_type": "memory_record",
            "ts": now,
            "schema_version": MEMORY_SCHEMA_VERSION,
            "memory_id": memory_id,
            "memory_fingerprint": memory_fingerprint,
            "setup_fingerprint": setup_fingerprint,
            "policy_hash": policy_hash,
            "timeframe": timeframe,
            "symbol_scope": symbol_scope,
            "account_scope": account_scope,
            "stats": {
                "n": 0,
                "wins": 0,
                "losses": 0,
                "pnl_usd_sum": 0.0,
                "r_sum": 0.0,
                "r_mean": None,
                "last_seen_ts": now,
            },
            "lifecycle": {
                "created_ts": now,
                "updated_ts": now,
                "read_only": True,
                "mutability": "merge_stats_only",
            },
            "tags": [],
            "notes": "",
        }

    stats = rec.get("stats") if isinstance(rec.get("stats"), dict) else {}
    n = int(stats.get("n") or 0) + 1
    wins = int(stats.get("wins") or 0)
    losses = int(stats.get("losses") or 0)

    if win_val is True:
        wins += 1
    elif win_val is False:
        losses += 1

    pnl_sum = float(stats.get("pnl_usd_sum") or 0.0) + float(pnl)

    r_sum = float(stats.get("r_sum") or 0.0)
    if r_val is not None:
        r_sum += float(r_val)

    r_mean = (r_sum / float(n)) if n > 0 else None

    stats["n"] = n
    stats["wins"] = wins
    stats["losses"] = losses
    stats["pnl_usd_sum"] = float(pnl_sum)
    stats["r_sum"] = float(r_sum)
    stats["r_mean"] = float(r_mean) if r_mean is not None else None
    stats["last_seen_ts"] = now

    rec["stats"] = stats

    lifecycle = rec.get("lifecycle") if isinstance(rec.get("lifecycle"), dict) else {}
    lifecycle["updated_ts"] = now
    rec["lifecycle"] = lifecycle

    notes = rec.get("notes")
    if isinstance(notes, str) and len(notes) > MEM_MAX_NOTES_LEN:
        rec["notes"] = notes[:MEM_MAX_NOTES_LEN]

    snapshot[memory_id] = rec
    return rec


def _emit_memory_from_enriched(enriched: Dict[str, Any]) -> None:
    try:
        if enriched.get("event_type") != "outcome_enriched":
            return

        setup = enriched.get("setup") if isinstance(enriched.get("setup"), dict) else {}
        policy = enriched.get("policy") if isinstance(enriched.get("policy"), dict) else {}
        policy_hash = _safe_str(policy.get("policy_hash"))

        setup_payload = setup.get("payload") if isinstance(setup.get("payload"), dict) else {}
        features = setup_payload.get("features") if isinstance(setup_payload.get("features"), dict) else {}

        setup_fp = _safe_str(features.get("setup_fingerprint"))
        mem_fp = _safe_str(features.get("memory_fingerprint"))

        tf = _normalize_timeframe(enriched.get("timeframe") or setup.get("timeframe")) or "unknown"

        symbol = _safe_str(enriched.get("symbol") or setup.get("symbol")).upper() or "UNKNOWN"
        account_scope = "global"

        if not mem_fp or not policy_hash:
            return

        stats_src = enriched.get("stats") if isinstance(enriched.get("stats"), dict) else {}
        pnl = float(stats_src.get("pnl_usd") or 0.0)
        r = stats_src.get("r_multiple")
        r_val = float(r) if r is not None else None
        win = stats_src.get("win")
        win_val = bool(win) if win is not None else None

        snapshot = _load_memory_snapshot()

        recA = _upsert_memory_record(
            snapshot=snapshot,
            memory_fingerprint=mem_fp,
            setup_fingerprint=setup_fp,
            policy_hash=policy_hash,
            timeframe=tf,
            account_scope=account_scope,
            symbol_scope=symbol,
            pnl=pnl,
            r_val=r_val,
            win_val=win_val,
        )
        _append_jsonl(MEMORY_RECORDS_PATH, recA)
        try:
            memory_bus.append(recA)
        except Exception:
            pass

        recB = _upsert_memory_record(
            snapshot=snapshot,
            memory_fingerprint=mem_fp,
            setup_fingerprint=setup_fp,
            policy_hash=policy_hash,
            timeframe=tf,
            account_scope=account_scope,
            symbol_scope="ANY",
            pnl=pnl,
            r_val=r_val,
            win_val=win_val,
        )
        _append_jsonl(MEMORY_RECORDS_PATH, recB)
        try:
            memory_bus.append(recB)
        except Exception:
            pass

        _save_memory_snapshot(snapshot)

    except Exception as e:
        log.warning("[ai_memory] Failed to emit memory: %r", e)


# ---------------------------------------------------------------------------
# Inbox routing helpers (lane-safe) for producers
# ---------------------------------------------------------------------------

_LABEL_SAFE_RE = re.compile(r"^[a-z0-9_]+$")


def _normalize_label(v: str) -> str:
    v = (v or "").strip().lower()
    if v in ("", "main", "global"):
        return ""
    return v


def _inbox_path_for_label(label: str) -> Path:
    lab = _normalize_label(label)
    if not lab:
        return (STATE_DIR / "ai_events_inbox.jsonl")
    return (STATE_DIR / f"ai_events_inbox_{lab}.jsonl")


def _resolve_inbox_target_for_event(event: Dict[str, Any]) -> Path:
    try:
        evt_lab = _normalize_label(str(event.get("account_label") or event.get("label") or ""))
        if evt_lab and _LABEL_SAFE_RE.match(evt_lab):
            return _inbox_path_for_label(evt_lab)
        return AI_EVENTS_INBOX_PATH
    except Exception:
        return AI_EVENTS_INBOX_PATH


def publish_ai_event(event: Dict[str, Any]) -> None:
    """
    Producer-safe enqueue.
    Writes to the appropriate inbox file. Does NOT do merges/reconciliation.
    The spine process drains + processes.
    """
    if not isinstance(event, dict):
        return
    if "event_type" not in event:
        return
    if "ts" not in event:
        event["ts"] = _now_ms()

    try:
        target = _resolve_inbox_target_for_event(event)
        _append_jsonl(target, event)
    except Exception:
        return


# ---------------------------------------------------------------------------
# Consumer: processing events drained from inbox
# ---------------------------------------------------------------------------

def _handle_event(event: Dict[str, Any]) -> None:
    if not isinstance(event, dict):
        return
    etype = event.get("event_type")
    if not etype:
        return

    # Stamp + fingerprint for setup events (consumer side)
    _stamp_policy(event)
    _ensure_setup_fingerprint(event)

    # Ledger append first (truth source)
    _append_jsonl(SPINE_EVENTS_PATH, event)

    if etype == "setup_context":
        _append_jsonl(SETUPS_PATH, event)
        _ensure_decision_for_setup(event)

        trade_id = event.get("trade_id")
        if trade_id:
            try:
                pending = _load_pending()
                for k in _extract_setup_alias_keys(event):
                    pending[str(k)] = event
                _save_pending(pending)
            except Exception as e:
                log.warning("[ai_events] Failed to update pending registry for trade_id=%r: %r", trade_id, e)

    elif etype == "outcome_record":
        _append_jsonl(OUTCOMES_RAW_PATH, event)

        trade_id = event.get("trade_id")
        if trade_id:
            try:
                pending = _load_pending()
                setup_evt, _matched_key = _find_pending_setup(pending, str(trade_id))
            except Exception:
                setup_evt = None

            if setup_evt:
                enriched = _merge_setup_and_outcome(setup_evt, event)
                _append_jsonl(OUTCOMES_PATH, enriched)
                _emit_memory_from_enriched(enriched)

                try:
                    pending2 = _load_pending()
                    _remove_pending_setup(pending2, setup_evt)
                    _save_pending(pending2)
                except Exception as e:
                    log.warning("[ai_events] Failed to remove trade_id=%r from pending registry: %r", trade_id, e)
            else:
                _append_jsonl(OUTCOMES_PATH, event)
        else:
            _append_jsonl(OUTCOMES_PATH, event)

    # best-effort bus publish (for in-proc dashboards/tests)
    try:
        ai_events_bus.append(event)
    except Exception:
        pass


def _drain_inbox_once(drain_max: int = 10_000) -> int:
    """
    Drains up to drain_max events from the inbox starting at cursor byte position.
    Cursor is a byte offset into the inbox file.
    Bad JSON lines are quarantined into AI_EVENTS_INBOX_BADLINES_PATH.
    """
    inbox = AI_EVENTS_INBOX_PATH
    if not inbox.exists():
        return 0

    cursor_path = AI_EVENTS_INBOX_CURSOR_PATH
    pos = _read_cursor(cursor_path)

    drained = 0
    bad = 0

    try:
        with inbox.open("rb") as f:
            f.seek(max(0, pos))
            while drained < drain_max:
                line_pos = f.tell()
                raw = f.readline()
                if not raw:
                    break

                raw_stripped = raw.strip()
                if not raw_stripped:
                    _write_cursor(cursor_path, f.tell())
                    pos = f.tell()
                    continue

                try:
                    evt = orjson.loads(raw_stripped)
                    if not isinstance(evt, dict):
                        raise ValueError("non-dict event")
                except Exception as e:
                    bad += 1
                    _append_jsonl(AI_EVENTS_INBOX_BADLINES_PATH, {
                        "ts": _now_ms(),
                        "account_label": ACCOUNT_LABEL,
                        "error": repr(e),
                        "raw": raw_stripped.decode("utf-8", errors="replace")[:4000],
                        "inbox": str(inbox),
                        "offset": int(line_pos),
                    })
                    _write_cursor(cursor_path, f.tell())
                    pos = f.tell()
                    continue

                _handle_event(evt)
                drained += 1
                _write_cursor(cursor_path, f.tell())
                pos = f.tell()

    except Exception as e:
        log.warning("[ai_events_spine] drain failed: %r", e)
        return drained

    if bad:
        log.warning("[ai_events_spine] quarantined %d bad inbox lines for %s", bad, ACCOUNT_LABEL)

    return drained


# ---------------------------------------------------------------------------
# Builders (kept for compatibility)
# ---------------------------------------------------------------------------

def build_setup_context(
    *,
    trade_id: str,
    symbol: str,
    account_label: str,
    strategy: str,
    features: Dict[str, Any],
    setup_type: Optional[str] = None,
    timeframe: Optional[str] = None,
    ai_profile: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> SetupRecord:
    tf = _normalize_timeframe(timeframe)

    payload: SetupRecord = {
        "event_type": "setup_context",
        "ts": _now_ms(),
        "trade_id": trade_id,
        "symbol": symbol,
        "account_label": account_label,
        "strategy": strategy,
        "payload": {"features": features or {}},
    }

    if setup_type is not None:
        payload["setup_type"] = setup_type
    if tf is not None:
        payload["timeframe"] = tf
    if ai_profile is not None:
        payload["ai_profile"] = ai_profile

    if extra or tf is not None:
        payload_extra = dict(extra or {})
        if tf is not None:
            payload_extra["timeframe"] = tf
        payload["payload"]["extra"] = payload_extra

    _stamp_policy(payload)  # type: ignore[arg-type]
    _ensure_setup_fingerprint(payload)  # type: ignore[arg-type]
    return payload


def build_outcome_record(
    *,
    trade_id: str,
    symbol: str,
    account_label: str,
    strategy: str,
    pnl_usd: float,
    r_multiple: Optional[float] = None,
    win: Optional[bool] = None,
    exit_reason: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> OutcomeRecord:
    payload: OutcomeRecord = {
        "event_type": "outcome_record",
        "ts": _now_ms(),
        "trade_id": trade_id,
        "symbol": symbol,
        "account_label": account_label,
        "strategy": strategy,
        "payload": {
            "pnl_usd": float(pnl_usd),
            "r_multiple": float(r_multiple) if r_multiple is not None else None,
            "win": bool(win) if win is not None else None,
            "exit_reason": exit_reason,
        },
    }

    if extra:
        payload["payload"]["extra"] = extra

    _stamp_policy(payload)  # type: ignore[arg-type]
    return payload


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Flashback AI Events Spine (inbox drainer + disk ledger)")
    ap.add_argument("--once", action="store_true", help="Drain once then exit")
    ap.add_argument("--sleep", type=float, default=float(os.getenv("SPINE_SLEEP_SECS", "1.0") or "1.0"),
                    help="Loop sleep seconds (default 1.0)")
    ap.add_argument("--drain-max", type=int, default=int(os.getenv("SPINE_DRAIN_MAX", "10000") or "10000"),
                    help="Max events to drain per tick")
    ap.add_argument("--heartbeat-secs", type=float, default=float(os.getenv("SPINE_HEARTBEAT_SECS", "10") or "10"),
                    help="Heartbeat interval seconds (default 10)")
    return ap.parse_args()


def main() -> None:
    args = _parse_args()

    log.info(
        "AI Events Spine started v3.0.0 | label=%s | inbox=%s | cursor=%s | events_dir=%s",
        ACCOUNT_LABEL,
        str(AI_EVENTS_INBOX_PATH),
        str(AI_EVENTS_INBOX_CURSOR_PATH),
        str(AI_EVENTS_DIR),
    )

    last_hb = 0.0

    def tick() -> int:
        return _drain_inbox_once(drain_max=int(args.drain_max))

    if args.once:
        drained = tick()
        now = time.time()
        if (now - last_hb) >= float(args.heartbeat_secs):
            try:
                record_heartbeat("ai_events_spine")
            except Exception:
                pass
        log.info("AI Events Spine --once drained=%d", drained)
        return

    while True:
        try:
            drained = tick()
            if drained:
                log.info("drained=%d (label=%s)", drained, ACCOUNT_LABEL)
        except Exception as e:
            log.warning("[ai_events_spine] tick exception: %r", e)

        now = time.time()
        if (now - last_hb) >= float(args.heartbeat_secs):
            last_hb = now
            try:
                record_heartbeat("ai_events_spine")
            except Exception:
                pass

        time.sleep(float(args.sleep))


if __name__ == "__main__":
    main()
