#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback - AI Events Spine (disk-logging version, v2.8.2 Phase4->5 learning hardened)

v2.8.2 Patch Summary (2025-12-20)
---------------------------------
CRITICAL: memory_fingerprint stability and real aggregation.

Problem (observed):
- memory_fingerprint still drifted because nested OHLC-ish debug fields leaked through scrubber:
  e.g. signal.debug.last_close

Fix:
- Expand recursive scrubber drops for OHLC-ish keys (close/open/high/low variants)
- Add suffix rule for *_close/_open/_high/_low to avoid future whack-a-mole
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
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



# Centralized Spine API paths/helpers (Phase 8+ hygiene)
try:
    from app.core.spine_api import (
        STATE_DIR as _STATE_DIR,
        AI_EVENTS_DIR as _AI_EVENTS_DIR,
        AI_MEMORY_DIR as _AI_MEMORY_DIR,
        AI_DECISIONS_PATH as _AI_DECISIONS_PATH,
    )
except Exception:  # pragma: no cover
    _STATE_DIR = None  # type: ignore
    _AI_EVENTS_DIR = None  # type: ignore
    _AI_MEMORY_DIR = None  # type: ignore
    _AI_DECISIONS_PATH = None  # type: ignore

def _now_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------------------------------------------------------
# Paths for AI event logs
# ---------------------------------------------------------------------------

STATE_DIR: Path = (_STATE_DIR if _STATE_DIR is not None else (ROOT / "state"))
AI_EVENTS_DIR: Path = (_AI_EVENTS_DIR if _AI_EVENTS_DIR is not None else (STATE_DIR / "ai_events"))

STATE_DIR.mkdir(parents=True, exist_ok=True)
AI_EVENTS_DIR.mkdir(parents=True, exist_ok=True)

SETUPS_PATH: Path = AI_EVENTS_DIR / "setups.jsonl"
OUTCOMES_PATH: Path = AI_EVENTS_DIR / "outcomes.jsonl"           # enriched-only guardrail
OUTCOMES_RAW_PATH: Path = AI_EVENTS_DIR / "outcomes_raw.jsonl"   # raw execution outcomes
OUTCOMES_ORPHANS_PATH: Path = AI_EVENTS_DIR / "outcomes_orphans.jsonl"

PENDING_REGISTRY_PATH: Path = AI_EVENTS_DIR / "pending_setups.json"

CONFIG_DIR: Path = ROOT / "config"
STRATEGIES_PATH: Path = CONFIG_DIR / "strategies.yaml"
EXIT_PROFILES_PATH: Path = CONFIG_DIR / "exit_profiles.yaml"
RISK_PROFILES_PATH: Path = CONFIG_DIR / "risk_profiles.yaml"

# ---------------------------------------------------------------------------
# Phase 4: Memory store (bounded + reversible)
# ---------------------------------------------------------------------------

AI_MEMORY_DIR: Path = (_AI_MEMORY_DIR if _AI_MEMORY_DIR is not None else (STATE_DIR / "ai_memory"))
AI_MEMORY_DIR.mkdir(parents=True, exist_ok=True)

MEMORY_SNAPSHOT_PATH: Path = AI_MEMORY_DIR / "memory_snapshot.json"
MEMORY_RECORDS_PATH: Path = AI_MEMORY_DIR / "memory_records.jsonl"

MEMORY_SCHEMA_VERSION = 1

MEM_MAX_RECORDS = 50_000
MEM_MAX_AGE_DAYS = 180
MEM_MAX_NOTES_LEN = 512

PEND_MAX_COUNT = 5_000
PEND_MAX_AGE_DAYS = 14

# ---------------------------------------------------------------------------
# Phase 4: Decisions (enforced)
# ---------------------------------------------------------------------------

AI_DECISIONS_PATH: Path = (_AI_DECISIONS_PATH if _AI_DECISIONS_PATH is not None else (STATE_DIR / "ai_decisions.jsonl"))
AI_EVENTS_ENFORCE_DECISION: bool = str(os.getenv("AI_EVENTS_ENFORCE_DECISION", "true")).strip().lower() in (
    "1", "true", "yes", "y", "on"
)
AI_DECISION_TAIL_BYTES: int = int(os.getenv("AI_DECISION_TAIL_BYTES", "1048576") or "1048576")  # default 1MB


def _append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    try:
        with path.open("ab") as f:
            f.write(orjson.dumps(payload))
            f.write(b"\n")
    except Exception as e:
        try:
            log.warning("[ai_events] Failed to append event to %s: %r", path, e)
        except Exception:
            pass


def _atomic_write_text(path: Path, text: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


# ---------------------------------------------------------------------------
# Timeframe normalization + stable json
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


def _safe_str(x: Any) -> str:
    try:
        return ("" if x is None else str(x)).strip()
    except Exception:
        return ""


def _coerce_setup_type(st: Any) -> str:
    s = _safe_str(st)
    return s if s else "unknown"


# ---------------------------------------------------------------------------
# Fingerprint feature filtering (Phase 5 critical)
# ---------------------------------------------------------------------------

# Exact keys to drop anywhere in the features tree
_FP_DROP_KEYS: Set[str] = {
    # identity / ids
    "trade_id",
    "client_trade_id",
    "source_trade_id",
    "order_id",
    "orderid",
    "orderlinkid",
    "sub_uid",
    "uid",
    # time
    "ts",
    "ts_ms",
    "ts_open_ms",
    "timestamp",
    "updated_ms",
    "time",
    # sizing / account state
    "qty",
    "size",
    "equity_usd",
    "risk_usd",
    "risk_pct",
    # price derived (changes constantly)
    "price",
    "last",
    "mark",
    "index",
    "best_bid",
    "best_ask",
    "stop_price",
    "take_profit_price",
    # OHLC-ish / debug price fields (also volatile)
    "open",
    "high",
    "low",
    "close",
    "last_close",
    "ohlc",
    "ohlcv",
    "hlc3",
    # execution / modes / policy artifacts
    "trade_mode",
    "automation_mode",
    "train_mode",
    "execution_lock_active",
    "execution_global_breaker_on",
    "decision_size_multiplier",
    "decision_enforced",
    "decision_reason",
    "decision_code",
    "trade_id_source",
    "forced_trade_id",
    "sig_trade_id_present",
    # duplicates of fields already stored in fingerprint core
    "symbol",
    "account_label",
    "strategy_name",
    "timeframe",
    "setup_type",
    "setup_fingerprint",
    "memory_fingerprint",
}

# Substring drops for keys that show up with varying naming styles
_FP_DROP_SUBSTRINGS: List[str] = [
    "order",       # orderId/orderLinkId/etc
    "client",      # client_trade_id variants
    "source",      # source_trade_id variants
    "uuid",        # any uuid-ish
    "guid",        # guid-ish
]


def _scrub_for_fingerprint(obj: Any, *, depth: int = 0, max_depth: int = 12) -> Any:
    """
    Recursively remove volatile keys from arbitrary JSON-like objects.
    Keeps structure but strips fields that cause per-trade uniqueness.
    """
    if depth > max_depth:
        return None

    if isinstance(obj, dict):
        out: Dict[str, Any] = {}
        for k, v in obj.items():
            try:
                ks = str(k)
            except Exception:
                continue
            kl = ks.strip().lower()

            # explicit drop
            if kl in _FP_DROP_KEYS:
                continue

            # suffix drop (catches last_close, prev_close, session_close, etc.)
            if kl.endswith(("_close", "_open", "_high", "_low")):
                continue

            # substring drop for id-ish keys
            if any(sub in kl for sub in _FP_DROP_SUBSTRINGS):
                # keep "trend_dir" etc; only drop obvious id-ish substrings
                if kl.startswith(("order", "client", "source")) or kl.endswith(("id", "uid")):
                    continue

            out[ks] = _scrub_for_fingerprint(v, depth=depth + 1, max_depth=max_depth)
        return out

    if isinstance(obj, list):
        return [_scrub_for_fingerprint(x, depth=depth + 1, max_depth=max_depth) for x in obj]

    # primitives
    return obj


def _filter_features_for_fingerprint(features: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(features, dict):
        return {}
    try:
        scrubbed = _scrub_for_fingerprint(features)
        return scrubbed if isinstance(scrubbed, dict) else {}
    except Exception:
        # fail-soft
        f = dict(features)
        # old minimal behavior fallback
        for k in ("setup_fingerprint", "memory_fingerprint"):
            f.pop(k, None)
        return f


def _extract_side(event: Dict[str, Any]) -> Optional[str]:
    """
    Try to recover trade side in a consistent way.
    We want side in the fingerprint core to prevent long/short collisions.
    """
    if not isinstance(event, dict):
        return None
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    feats = payload.get("features") if isinstance(payload.get("features"), dict) else {}
    extra = payload.get("extra") if isinstance(payload.get("extra"), dict) else {}

    side = _safe_str(feats.get("side")) or _safe_str(extra.get("side"))
    if not side and isinstance(feats.get("signal"), dict):
        side = _safe_str(feats["signal"].get("side"))  # type: ignore[index]
    if not side:
        return None

    s = side.lower()
    if s in ("buy", "long"):
        return "buy"
    if s in ("sell", "short"):
        return "sell"
    return s


# ---------------------------------------------------------------------------
# Fingerprinting
# ---------------------------------------------------------------------------

def _compute_setup_fingerprint(
    *,
    trade_id: str,
    symbol: str,
    account_label: str,
    strategy: str,
    setup_type: Optional[str],
    timeframe: Optional[str],
    side: Optional[str],
    features: Dict[str, Any],
) -> str:
    core = {
        "trade_id": str(trade_id),
        "symbol": str(symbol).upper(),
        "account_label": str(account_label),
        "strategy": str(strategy),
        "setup_type": str(setup_type) if setup_type is not None else None,
        "timeframe": timeframe,
        "side": side,
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
    side: Optional[str],
    features: Dict[str, Any],
) -> str:
    core = {
        "symbol": str(symbol).upper(),
        "account_label": str(account_label),
        "strategy": str(strategy),
        "setup_type": str(setup_type) if setup_type is not None else None,
        "timeframe": timeframe,
        "side": side,
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
    side = _extract_side(event)
    tf_final = tf

    if not features.get("setup_fingerprint"):
        fp = _compute_setup_fingerprint(
            trade_id=tid,
            symbol=sym,
            account_label=acct,
            strategy=strat,
            setup_type=str(stype) if stype is not None else None,
            timeframe=tf_final,
            side=side,
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
            side=side,
            features=features,
        )
        features["memory_fingerprint"] = mfp

    payload["features"] = features
    event["payload"] = payload


# ---------------------------------------------------------------------------
# Outcome fingerprinting (integrity-critical)
# ---------------------------------------------------------------------------

def _ensure_outcome_fingerprints(event: Dict[str, Any]) -> None:
    """
    Guarantee that outcome_record/outcome_enriched written to outcomes.jsonl carries a canonical setup_fingerprint.

    Rules:
    - If we can recover setup_fingerprint/memory_fingerprint from embedded setup/payload/extra, use it.
    - If missing (or orphan/test outcome), synthesize a deterministic setup_fingerprint so integrity checks do not fail.
    - Mark synthetic fingerprints via extra.synthetic_setup_fingerprint = True for downstream filtering.
    """
    try:
        if not isinstance(event, dict):
            return
        et = event.get("event_type")
        if et not in ("outcome_record", "outcome_enriched"):
            return

        # -------------------------
        # outcome_enriched
        # -------------------------
        if et == "outcome_enriched":
            sp = event.get("setup_fingerprint")
            mp = event.get("memory_fingerprint")

            setup = event.get("setup") if isinstance(event.get("setup"), dict) else {}
            try:
                setup_payload = setup.get("payload") if isinstance(setup.get("payload"), dict) else {}
                feats = setup_payload.get("features") if isinstance(setup_payload.get("features"), dict) else {}
                if not sp and isinstance(feats, dict):
                    sp = feats.get("setup_fingerprint")
                if not mp and isinstance(feats, dict):
                    mp = feats.get("memory_fingerprint")
            except Exception:
                pass

            extra = event.get("extra") if isinstance(event.get("extra"), dict) else {}
            if not sp and isinstance(extra, dict):
                sp = extra.get("setup_fingerprint")
            if not mp and isinstance(extra, dict):
                mp = extra.get("memory_fingerprint")

            # promote to canonical
            if sp and not event.get("setup_fingerprint"):
                event["setup_fingerprint"] = sp
            if mp and not event.get("memory_fingerprint"):
                event["memory_fingerprint"] = mp

            # keep in extra
            if isinstance(extra, dict):
                if sp and not extra.get("setup_fingerprint"):
                    extra["setup_fingerprint"] = sp
                if mp and not extra.get("memory_fingerprint"):
                    extra["memory_fingerprint"] = mp
                event["extra"] = extra

            # synthesize if still missing
            if not event.get("setup_fingerprint"):
                tid = _safe_str(event.get("trade_id"))
                sym = _safe_str(event.get("symbol"))
                acct = _safe_str(event.get("account_label") or "main") or "main"
                strat = _safe_str(event.get("strategy") or "unknown") or "unknown"
                tf = _normalize_timeframe(event.get("timeframe")) or "unknown"

                fp = _compute_setup_fingerprint(
                    trade_id=tid,
                    symbol=sym,
                    account_label=acct,
                    strategy=strat,
                    setup_type=str(event.get("setup_type")) if event.get("setup_type") is not None else None,
                    timeframe=tf,
                    side=None,
                    features={},
                )
                event["setup_fingerprint"] = fp
                extra = event.get("extra") if isinstance(event.get("extra"), dict) else {}
                extra["setup_fingerprint"] = fp
                extra["synthetic_setup_fingerprint"] = True
                event["extra"] = extra

            return

        # -------------------------
        # outcome_record (raw)
        # -------------------------
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        extra = payload.get("extra") if isinstance(payload.get("extra"), dict) else {}

        sp = event.get("setup_fingerprint") or payload.get("setup_fingerprint") or (
            extra.get("setup_fingerprint") if isinstance(extra, dict) else None
        )
        mp = event.get("memory_fingerprint") or payload.get("memory_fingerprint") or (
            extra.get("memory_fingerprint") if isinstance(extra, dict) else None
        )

        if sp:
            event["setup_fingerprint"] = sp
            payload["setup_fingerprint"] = sp
        if mp:
            event["memory_fingerprint"] = mp
            payload["memory_fingerprint"] = mp

        if not event.get("setup_fingerprint"):
            tid = _safe_str(event.get("trade_id"))
            sym = _safe_str(event.get("symbol"))
            acct = _safe_str(event.get("account_label") or "main") or "main"
            strat = _safe_str(event.get("strategy") or "unknown") or "unknown"
            tf = _normalize_timeframe(event.get("timeframe"))
            if tf is None and isinstance(extra, dict):
                tf = _normalize_timeframe(extra.get("timeframe"))
            tf = tf or "unknown"

            fp = _compute_setup_fingerprint(
                trade_id=tid,
                symbol=sym,
                account_label=acct,
                strategy=strat,
                setup_type=None,
                timeframe=tf,
                side=None,
                features={},
            )
            event["setup_fingerprint"] = fp
            payload["setup_fingerprint"] = fp
            if isinstance(extra, dict):
                extra["setup_fingerprint"] = fp
                extra["synthetic_setup_fingerprint"] = True

        payload["extra"] = extra if isinstance(extra, dict) else {}
        event["payload"] = payload
    except Exception:
        return


# ---------------------------------------------------------------------------
# Policy stamping (versions + hash)
# ---------------------------------------------------------------------------

_POLICY_CACHE: Dict[str, Any] = {"policy": None, "loaded_ms": 0}
_POLICY_CACHE_TTL_MS = 10_000


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
        tid = str(trade_id).strip()
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
                str(d.get("trade_id") or ""),
                str(d.get("client_trade_id") or ""),
                str(d.get("source_trade_id") or ""),
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
        policy_hash = str(pol.get("policy_hash") or "").strip() or None

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
        from app.bots.ai_pilot import pilot_decide
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
        txt = PENDING_REGISTRY_PATH.read_text(encoding="utf-8")
        data = json.loads(txt or "{}")
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _prune_pending(reg: Dict[str, Any]) -> Dict[str, Any]:
    try:
        now = _now_ms()
        max_age_ms = int(PEND_MAX_AGE_DAYS * 24 * 60 * 60 * 1000)

        items = []
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
        _atomic_write_text(
            PENDING_REGISTRY_PATH,
            json.dumps(reg2, indent=2, sort_keys=True),
        )
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
    """
    Returns (setup_event, matched_key).
    1) direct key
    2) alias scan: find any setup whose alias set contains trade_id
    """
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

        final_status = "CLOSED"

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
                "final_status": final_status,
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

        try:
            if "setup_fingerprint" not in enriched:
                enriched["setup_fingerprint"] = (enriched.get("extra") or {}).get("setup_fingerprint")
            if "memory_fingerprint" not in enriched:
                enriched["memory_fingerprint"] = (enriched.get("extra") or {}).get("memory_fingerprint")
        except Exception:
            pass

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
        txt = MEMORY_SNAPSHOT_PATH.read_text(encoding="utf-8")
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
        _atomic_write_text(
            MEMORY_SNAPSHOT_PATH,
            json.dumps(snap2, indent=2, sort_keys=True),
        )
    except Exception as e:
        log.warning("[ai_memory] Failed to save snapshot: %r", e)


def _compute_memory_id(memory_fingerprint: str, policy_hash: str, account_scope: str, symbol_scope: str, timeframe: str) -> str:
    h = hashlib.sha256()
    h.update(_stable_json({
        "memory_fingerprint": memory_fingerprint,
        "policy_hash": policy_hash,
        "account_scope": account_scope,
        "symbol_scope": symbol_scope,
        "timeframe": timeframe,
    }).encode("utf-8", errors="ignore"))
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
        policy_hash = str(policy.get("policy_hash") or "").strip()

        setup_payload = setup.get("payload") if isinstance(setup.get("payload"), dict) else {}
        features = setup_payload.get("features") if isinstance(setup_payload.get("features"), dict) else {}

        setup_fp = str((features.get("setup_fingerprint") if isinstance(features, dict) else "") or "").strip()
        mem_fp = str((features.get("memory_fingerprint") if isinstance(features, dict) else "") or "").strip()

        tf = _normalize_timeframe(enriched.get("timeframe") or setup.get("timeframe")) or "unknown"

        symbol = str(enriched.get("symbol") or setup.get("symbol") or "").strip().upper()
        if not symbol:
            symbol = "UNKNOWN"

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
# Publisher
# ---------------------------------------------------------------------------

def publish_ai_event(event: Dict[str, Any]) -> None:
    if not isinstance(event, dict):
        return
    if "event_type" not in event:
        return
    if "ts" not in event:
        event["ts"] = _now_ms()

    _stamp_policy(event)
    _ensure_setup_fingerprint(event)

    etype = event.get("event_type")
    # Guardrail: drop taxonomy-unknown setup_context at the source.
    # If this ever hits disk, training data is contaminated.
    try:
        if str(etype or "").strip().lower() == "setup_context":
            st = (
                event.get("setup_type")
                or ((event.get("payload") or {}).get("setup_type") if isinstance(event.get("payload"), dict) else None)
                or (
                    (((event.get("payload") or {}).get("features") or {}).get("setup_type"))
                    if isinstance((event.get("payload") or {}).get("features"), dict)
                    else None
                )
            )
            if str(st or "").strip().lower() == "unknown":
                try:
                    log.warning(
                        "spine_drop_unknown_setup: trade_id=%s symbol=%s account=%s",
                        event.get("trade_id"),
                        event.get("symbol"),
                        event.get("account_label"),
                    )
                except Exception:
                    pass
                return
    except Exception:
        # Fail-soft: never crash publisher
        pass


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
                setup_evt, _matched_key = (None, None)

            if setup_evt:
                enriched = _merge_setup_and_outcome(setup_evt, event)
                _ensure_outcome_fingerprints(enriched)
                _append_jsonl(OUTCOMES_PATH, enriched)
                _emit_memory_from_enriched(enriched)

                try:
                    _remove_pending_setup(pending, setup_evt)
                    _save_pending(pending)
                except Exception as e:
                    log.warning("[ai_events] Failed to remove trade_id=%r from pending registry: %r", trade_id, e)
            else:
                # orphan: never write raw outcome_record into outcomes.jsonl
                _ensure_outcome_fingerprints(event)
                _append_jsonl(OUTCOMES_ORPHANS_PATH, event)

        else:
            # missing trade_id => orphan by definition
            _append_jsonl(OUTCOMES_ORPHANS_PATH, event)

    try:
        ai_events_bus.append(event)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Builders
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
    # Enforce valid-shape contract: setup_type + timeframe must exist & be non-empty
    st = _coerce_setup_type(setup_type)

    tf = _normalize_timeframe(timeframe)
    if tf is None and isinstance(extra, dict):
        tf = _normalize_timeframe(extra.get("timeframe"))
    if tf is None:
        tf = "unknown"

    feats = features if isinstance(features, dict) else {}
    payload: SetupRecord = {
        "event_type": "setup_context",
        "ts": _now_ms(),
        "trade_id": trade_id,
        "symbol": symbol,
        "account_label": account_label,
        "strategy": strategy,
        "setup_type": st,
        "timeframe": tf,
        "payload": {"features": feats or {}},
    }

    if ai_profile is not None:
        payload["ai_profile"] = ai_profile

    if extra or tf is not None:
        payload_extra = dict(extra or {})
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
    strategy: str = "",
    pnl_usd: float = 0.0,
    r_multiple: Optional[float] = None,
    win: Optional[bool] = None,
    exit_reason: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
    # --- backward-compat inputs (legacy producers) ---
    strategy_name: Optional[str] = None,
    timeframe: Optional[str] = None,
    # swallow anything else without exploding the pipeline
    **_ignored: Any,
) -> OutcomeRecord:
    """
    Backward-compatible OutcomeRecord builder.

    Canonical fields:
      - strategy (preferred)
    Compatibility:
      - strategy_name -> strategy (only if strategy not provided)
      - timeframe is stored top-level and also echoed into payload.extra["timeframe"]
      - extra dict is preserved
      - unknown kwargs are ignored safely
    """
    strat = _safe_str(strategy) or _safe_str(strategy_name) or "unknown"

    payload: OutcomeRecord = {
        "event_type": "outcome_record",
        "ts": _now_ms(),
        "trade_id": trade_id,
        "symbol": symbol,
        "account_label": account_label,
        "strategy": strat,
        "payload": {
            "pnl_usd": float(pnl_usd),
            "r_multiple": float(r_multiple) if r_multiple is not None else None,
            "win": bool(win) if win is not None else None,
            "exit_reason": exit_reason,
        },
    }

    # Optional timeframe
    tf = _normalize_timeframe(timeframe)
    if tf is not None:
        payload["timeframe"] = tf

    # Preserve/merge extra
    ex = dict(extra or {})
    if tf is not None and "timeframe" not in ex:
        ex["timeframe"] = tf

    if ex:
        payload["payload"]["extra"] = ex

    _stamp_policy(payload)  # type: ignore[arg-type]
    return payload
def _env_bool(name: str, default: str = "true") -> bool:
    raw = str(os.getenv(name, default)).strip().lower()
    return raw in ("1", "true", "yes", "y", "on")


def run_once_tick() -> None:
    """Phase 8: deterministic single tick for health refresh + orchestration."""
    try:
        record_heartbeat("ai_events_spine")
    except Exception:
        pass

    # Refresh memory snapshot mtime safely (schema stays dict-of-records)
    try:
        if _env_bool("AI_EVENTS_SPINE_TICK_MEMORY_SNAPSHOT", "true"):
            snap = _load_memory_snapshot()
            _save_memory_snapshot(snap)
    except Exception as e:
        try:
            log.warning("[ai_events_spine] tick memory_snapshot failed: %r", e)
        except Exception:
            pass

    # Prune pending registry (keeps it bounded + refreshes file mtime)
    try:
        if _env_bool("AI_EVENTS_SPINE_TICK_PENDING", "true"):
            reg = _load_pending()
            _save_pending(reg)
    except Exception as e:
        try:
            log.warning("[ai_events_spine] tick pending failed: %r", e)
        except Exception:
            pass


def loop(interval_sec: float = 10.0) -> None:
    log.info("AI Events Spine loop started (disk logger + heartbeat, v2.8.2).")
    while True:
        run_once_tick()
        time.sleep(max(0.5, float(interval_sec)))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="Run one ops tick (heartbeat + snapshot refresh) and exit")
    ap.add_argument("--interval", type=float, default=float(os.getenv("AI_EVENTS_SPINE_INTERVAL", "10") or "10"), help="Loop interval seconds")
    args = ap.parse_args()

    if args.once:
        run_once_tick()
        return

    loop(interval_sec=float(args.interval))


if __name__ == "__main__":
    main()
