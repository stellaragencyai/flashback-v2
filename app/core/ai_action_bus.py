#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Action Bus

Purpose
-------
Central log for AI "actions" (decisions / recommendations) emitted by
ai_pilot or future AI agents.

File format
-----------
JSON Lines (one object per line), so it's easy to tail/grep or load into
pandas.

Location
--------
- Controlled by AI_ACTIONS_PATH env var (preferred)
- Also accepts legacy aliases:
    AI_ACTION_LOG_PATH, AI_ACTION_BUS_PATH
- Defaults to: "state/ai_actions.jsonl" under ROOT

Each line looks like:

    {
      "ts_ms": 1763752000123,
      "source": "ai_pilot",
      "label": "main",
      "dry_run": true,
      "action": {
        "type": "advice_only",
        "reason": "sample_policy",
        "symbol": "BTCUSDT",
        "side": "Buy",
        "qty": 1,
        "confidence": 0.92,
        "setup_fingerprint": "..."
      }
    }

Nothing here places orders. This is a log / bus only.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import orjson

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

# Prefer canonical env var; support legacy aliases to avoid silent misrouting.
_RAW_ACTIONS_PATH = (
    os.getenv("AI_ACTIONS_PATH")
    or os.getenv("AI_ACTION_LOG_PATH")
    or os.getenv("AI_ACTION_BUS_PATH")
    or _DEFAULT_PATH
)

ACTION_LOG_PATH: Path = Path(_RAW_ACTIONS_PATH)
if not ACTION_LOG_PATH.is_absolute():
    ACTION_LOG_PATH = ROOT / ACTION_LOG_PATH

# Export aliases expected by various callers
ACTION_BUS_PATH: Path = ACTION_LOG_PATH
AI_ACTION_LOG_PATH: Path = ACTION_LOG_PATH

# Ensure directory exists (import-safe)
try:
    ACTION_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
except Exception:
    pass


# ---------------------------------------------------------------------------
# Logging (robust)
# ---------------------------------------------------------------------------

try:
    from app.core.log import get_logger
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

logger = get_logger("ai_action_bus")


def _now_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------------------------------------------------------
# Bus health helpers
# ---------------------------------------------------------------------------

def ai_actions_age_sec() -> Optional[float]:
    """
    Return age in seconds of the AI actions log (based on mtime), or None.
    For health checks / dashboards only.
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


# ---------------------------------------------------------------------------
# Public API (guaranteed contract)
# ---------------------------------------------------------------------------

def ensure_bus() -> None:
    """
    Ensure the bus file exists. Fail-soft, no exceptions bubble up.
    """
    try:
        ACTION_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        if not ACTION_LOG_PATH.exists():
            ACTION_LOG_PATH.write_text("", encoding="utf-8")
    except Exception:
        return


def append_actions(
    actions: Iterable[Dict[str, Any]],
    *,
    source: str = "ai_pilot",
    label: Optional[str] = None,
    dry_run: Optional[bool] = None,
) -> int:
    """
    Append a batch of actions to the AI action log.

    Returns number of actions written.
    """
    ensure_bus()

    buf: List[bytes] = []
    ts = _now_ms()

    for a in actions:
        if not isinstance(a, dict):
            logger.warning("append_actions: skipping non-dict action: %r", a)
            continue

        env_label = label or str(a.get("label", "") or a.get("account_label", "") or "unknown")
        env_dry = dry_run if dry_run is not None else bool(a.get("dry_run", True))

        row = {
            "ts_ms": ts,
            "source": source,
            "label": env_label,
            "dry_run": env_dry,
            "action": a,
        }

        try:
            buf.append(orjson.dumps(row) + b"\n")
        except Exception as e:
            logger.warning("append_actions: failed to encode action %r: %r", a, e)
            continue

    if not buf:
        return 0

    try:
        with ACTION_LOG_PATH.open("ab") as f:
            for b in buf:
                f.write(b)
    except Exception as e:
        logger.error("append_actions: failed to write %d actions: %r", len(buf), e)
        return 0

    return len(buf)


def append_action(
    action: Dict[str, Any],
    *,
    source: str = "ai_pilot",
    label: Optional[str] = None,
    dry_run: Optional[bool] = None,
) -> int:
    """
    Convenience wrapper for a single action.
    Returns 1 if written, else 0.
    """
    if not isinstance(action, dict):
        logger.warning("append_action: ignoring non-dict action: %r", action)
        return 0
    return append_actions([action], source=source, label=label, dry_run=dry_run)


def publish_action(action: Dict[str, Any]) -> bool:
    """
    Compatibility API: publish_action(action_dict) -> bool

    This is what ai_action_router and many legacy callers expect.
    We write an ENVELOPE row (not a flat action), so router can normalize safely.
    """
    try:
        n = append_action(action)
        return n > 0
    except Exception:
        return False


def read_actions(since_ts_ms: int = 0, limit: int = 500) -> List[Dict[str, Any]]:
    """
    Read envelope rows from the bus (best-effort).
    Returns a list of dicts (each is one JSONL row).
    """
    out: List[Dict[str, Any]] = []
    try:
        if not ACTION_LOG_PATH.exists():
            return out

        with ACTION_LOG_PATH.open("rb") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = orjson.loads(line)
                except Exception:
                    continue
                if not isinstance(row, dict):
                    continue

                ts = row.get("ts_ms", 0)
                try:
                    ts_i = int(ts)
                except Exception:
                    ts_i = 0

                if ts_i < int(since_ts_ms):
                    continue

                out.append(row)
                if len(out) >= int(limit):
                    break

        return out
    except Exception:
        return out


__all__ = [
    "ROOT",
    "ACTION_LOG_PATH",
    "ACTION_BUS_PATH",
    "AI_ACTION_LOG_PATH",
    "ensure_bus",
    "publish_action",
    "read_actions",
    "append_action",
    "append_actions",
    "ai_actions_age_sec",
]
