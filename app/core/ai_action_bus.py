#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Action Bus

Central append-only log for AI "actions" emitted by ai_pilot or other agents.

IMPORTANT:
- This bus writes *flat* action dicts (NOT an envelope), because your current
  pipeline (ai_action_router + existing jsonl history) already uses flat rows.
- Includes lightweight dedupe to avoid poisoning state/ai_actions.jsonl with
  repeated replays (common during testing / restarts / retries).

Dedupe key (when available):
    (account_label, type, trade_id)

Where:
- trade_id is pulled from action["setup_context"]["trade_id"] if present, else action["trade_id"]
- account_label is pulled from action["account_label"] (or provided label)
- type is pulled from action["type"]

Integrity:
- Any trade-bearing action missing required join/risk fields is downgraded to "noop"
  and annotated in extra, so it cannot poison learning datasets.

Controls (env):
- AI_ACTIONS_PATH                 : override file path (default state/ai_actions.jsonl)
- AI_ACTIONS_DEDUPE               : true/false (default true)
- AI_ACTIONS_DEDUPE_MAX_KEYS      : max in-memory keys (default 50000)
- AI_ACTIONS_DEDUPE_SEED_TAIL     : seed dedupe cache from last N lines (default 2000)
"""

from __future__ import annotations

import os
import time
from collections import OrderedDict, deque
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import orjson

from app.core.ai_action_schema import is_heartbeat, is_trade_bearing, missing_trade_fields

# ---------------------------------------------------------------------------
# ROOT & path resolution
# ---------------------------------------------------------------------------

try:
    from app.core.config import settings
except Exception:  # pragma: no cover
    class _DummySettings:  # type: ignore
        ROOT: Path = Path(__file__).resolve().parents[2]
    settings = _DummySettings()  # type: ignore

ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])

_DEFAULT_PATH = "state/ai_actions.jsonl"
_RAW_ACTIONS_PATH = os.getenv("AI_ACTIONS_PATH", _DEFAULT_PATH).strip() or _DEFAULT_PATH

ACTION_LOG_PATH: Path = Path(_RAW_ACTIONS_PATH)
if not ACTION_LOG_PATH.is_absolute():
    ACTION_LOG_PATH = ROOT / ACTION_LOG_PATH

ACTION_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Logging (robust)
# ---------------------------------------------------------------------------

try:
    from app.core.log import get_logger
except Exception:  # pragma: no cover
    import logging, sys

    def get_logger(name: str) -> "logging.Logger":  # type: ignore
        logger_ = logging.getLogger(name)
        if not logger_.handlers:
            handler = logging.StreamHandler(sys.stdout)
            fmt = logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
            handler.setFormatter(fmt)
            logger_.addHandler(handler)
        logger_.setLevel(logging.INFO)
        return logger_

logger = get_logger("ai_action_bus")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _extract_trade_id(a: Dict[str, Any]) -> str:
    """
    Pull trade_id from setup_context.trade_id first, then action.trade_id.
    """
    setup = a.get("setup_context") or {}
    if isinstance(setup, dict):
        tid = str(setup.get("trade_id") or "").strip()
        if tid:
            return tid
        # allow aliases for debugging, but do not treat as canonical
        alias = str(setup.get("client_trade_id") or setup.get("source_trade_id") or "").strip()
        if alias:
            return alias

    return str(a.get("trade_id") or a.get("client_trade_id") or a.get("source_trade_id") or "").strip()


# ---------------------------------------------------------------------------
# Dedupe (lightweight)
# ---------------------------------------------------------------------------

_DEDUPE_ENABLED = os.getenv("AI_ACTIONS_DEDUPE", "true").strip().lower() not in ("0", "false", "no", "off")
_DEDUPE_MAX = int(os.getenv("AI_ACTIONS_DEDUPE_MAX_KEYS", "50000").strip() or "50000")
_DEDUPE_SEED_TAIL = int(os.getenv("AI_ACTIONS_DEDUPE_SEED_TAIL", "2000").strip() or "2000")

# OrderedDict used as a simple LRU-ish set (keys only)
_DEDUPE_CACHE: "OrderedDict[str, int]" = OrderedDict()
_DEDUPE_SEEDED = False


def _extract_dedupe_key(a: Dict[str, Any]) -> Optional[str]:
    """
    Return a stable dedupe key for an action, or None if insufficient fields exist.
    """
    try:
        acct = str(a.get("account_label") or "").strip()
        typ = str(a.get("type") or "").strip().lower()
        tid = _extract_trade_id(a)

        if not (acct and typ and tid):
            return None

        return f"{acct}|{typ}|{tid}"
    except Exception:
        return None


def _dedupe_remember(key: str, ts_ms: int) -> None:
    if not _DEDUPE_ENABLED:
        return
    _DEDUPE_CACHE[key] = ts_ms
    _DEDUPE_CACHE.move_to_end(key, last=True)
    while len(_DEDUPE_CACHE) > _DEDUPE_MAX:
        _DEDUPE_CACHE.popitem(last=False)


def _dedupe_seen(key: str) -> bool:
    if not _DEDUPE_ENABLED:
        return False
    if key in _DEDUPE_CACHE:
        _DEDUPE_CACHE.move_to_end(key, last=True)
        return True
    return False


def _seed_dedupe_from_tail() -> None:
    """
    Best-effort: parse last N lines from the actions file to prime the dedupe cache
    so restarts don't instantly re-emit duplicates.
    """
    global _DEDUPE_SEEDED
    if _DEDUPE_SEEDED or not _DEDUPE_ENABLED:
        _DEDUPE_SEEDED = True
        return

    _DEDUPE_SEEDED = True

    try:
        if not ACTION_LOG_PATH.exists():
            return

        data = ACTION_LOG_PATH.read_bytes()
        lines = data.splitlines()
        tail = deque(lines, maxlen=max(1, _DEDUPE_SEED_TAIL))

        seeded = 0
        for raw in tail:
            try:
                if not raw:
                    continue
                obj = orjson.loads(raw)
                if not isinstance(obj, dict):
                    continue
                k = _extract_dedupe_key(obj)
                if not k:
                    continue
                _dedupe_remember(k, int(obj.get("ts_ms") or _now_ms()))
                seeded += 1
            except Exception:
                continue

        if seeded:
            logger.info("dedupe seeded from tail: %s keys", min(seeded, _DEDUPE_MAX))
    except Exception as e:
        logger.warning("dedupe seed failed: %r", e)


# ---------------------------------------------------------------------------
# Bus health helpers
# ---------------------------------------------------------------------------

def ai_actions_age_sec() -> Optional[float]:
    """
    Return age in seconds of the AI actions log (based on mtime), or None.
    """
    try:
        if not ACTION_LOG_PATH.exists():
            return None
        mtime = ACTION_LOG_PATH.stat().st_mtime
        now = time.time()
        if mtime <= 0 or now <= mtime:
            return None
        return float(now - mtime)
    except Exception:
        return None


def _downgrade_to_noop(a: Dict[str, Any], *, missing: Dict[str, bool]) -> Dict[str, Any]:
    """
    Convert a broken trade-bearing action into a safe noop, preserving evidence in extra.
    """
    extra = a.get("extra")
    if not isinstance(extra, dict):
        extra = {}
    extra = dict(extra)
    extra.setdefault("phase8", {})
    if isinstance(extra["phase8"], dict):
        extra["phase8"]["dropped_trade_action_missing_fields"] = sorted(missing.keys())
        extra["phase8"]["original_type"] = str(a.get("type") or "")
        extra["phase8"]["original_action_id"] = str(a.get("action_id") or "")
        extra["phase8"]["original_trade_id"] = _extract_trade_id(a)
    a["extra"] = extra

    a["type"] = "noop"
    a.setdefault("reason", "downgraded_to_noop_missing_required_trade_fields")
    return a


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def append_actions(
    actions: Iterable[Dict[str, Any]],
    *,
    source: str = "ai_pilot",
    label: Optional[str] = None,
    dry_run: Optional[bool] = None,
) -> int:
    """
    Append a batch of *flat* actions to the AI action log with dedupe protection.

    Returns number of rows written.
    """
    _seed_dedupe_from_tail()

    batch_ts = _now_ms()
    buf: List[bytes] = []
    written = 0
    skipped = 0
    downgraded = 0

    for raw in actions:
        if not isinstance(raw, dict):
            logger.warning("append_actions: skipping non-dict action: %r", raw)
            continue

        a = dict(raw)

        # normalize + stamp
        a.setdefault("ts_ms", batch_ts)
        if label is not None:
            a.setdefault("account_label", label)
        a.setdefault("account_label", a.get("account_label") or "unknown")
        a["source"] = a.get("source") or source
        if dry_run is not None:
            a["dry_run"] = bool(dry_run)
        else:
            a.setdefault("dry_run", True)

        # Enforce Phase 8 integrity: trade-bearing actions must be joinable.
        if not is_heartbeat(a) and is_trade_bearing(a):
            # If producer only put trade_id inside setup_context, materialize it flat too.
            tid = _extract_trade_id(a)
            if tid and not a.get("trade_id"):
                a["trade_id"] = tid

            missing = missing_trade_fields(a)
            if missing:
                a = _downgrade_to_noop(a, missing=missing)
                downgraded += 1

        k = _extract_dedupe_key(a)
        if k and _dedupe_seen(k):
            skipped += 1
            continue

        try:
            buf.append(orjson.dumps(a) + b"\n")
            written += 1
            if k:
                _dedupe_remember(k, int(a.get("ts_ms") or batch_ts))
        except Exception as e:
            logger.warning("append_actions: failed to encode action %r: %r", a, e)
            continue

    if not buf:
        if skipped:
            logger.info("append_actions: skipped %s duplicate actions (wrote 0)", skipped)
        return 0

    try:
        with ACTION_LOG_PATH.open("ab") as f:
            for b in buf:
                f.write(b)
    except Exception as e:
        logger.error("append_actions: failed to write %d actions: %r", len(buf), e)
        return 0

    if skipped or downgraded:
        logger.info("append_actions: wrote %s actions, skipped %s duplicates, downgraded %s broken trade-actions",
                    written, skipped, downgraded)

    return written


def append_action(
    action: Dict[str, Any],
    *,
    source: str = "ai_pilot",
    label: Optional[str] = None,
    dry_run: Optional[bool] = None,
) -> int:
    """
    Convenience wrapper for a single action.
    """
    if not isinstance(action, dict):
        logger.warning("append_action: ignoring non-dict action: %r", action)
        return 0
    return append_actions([action], source=source, label=label, dry_run=dry_run)
