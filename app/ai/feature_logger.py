#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Feature Logger v3 (trade_id-based setup memory)

Purpose
-------
Centralized logger for "features at open" for each trade.

Called by executor_v2 via:

    log_features_at_open(
        trade_id=...,
        ts_open_ms=...,
        symbol=...,
        sub_uid=...,
        strategy_name=...,
        setup_type=...,
        mode=...,
        features={ ... }
    )

Each call appends ONE JSONL row to:

    state/features/setups.jsonl

This becomes the main feature store used by:
    - AI Setup Memory
    - expectancy stats
    - later model training jobs

Design goals
------------
- Append-only, crash-tolerant.
- Stable schema (versioned).
- Trade-centric: everything keyed by trade_id so we can join with AI events.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

# ---------------------------------------------------------------------------
# ROOT / paths
# ---------------------------------------------------------------------------

try:
    from app.core.config import settings  # type: ignore[attr-defined]
    ROOT: Path = settings.ROOT  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

FEATURES_DIR: Path = ROOT / "state" / "features"
FEATURES_DIR.mkdir(parents=True, exist_ok=True)

SETUPS_FEATURES_PATH: Path = FEATURES_DIR / "setups.jsonl"


# ---------------------------------------------------------------------------
# Logging helper (minimal, no dependencies on app.core.log)
# ---------------------------------------------------------------------------

try:
    from app.core.log import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging, sys

    def get_logger(name: str) -> "logging.Logger":  # type: ignore
        logger_ = logging.getLogger(name)
        if not logger_.handlers:
            handler = logging.StreamHandler(sys.stdout)
            fmt = logging.Formatter(
                "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
            )
            handler.setFormatter(fmt)
            logger_.addHandler(handler)
        logger_.setLevel(logging.INFO)
        return logger_

log = get_logger("feature_logger")


# ---------------------------------------------------------------------------
# Core writer
# ---------------------------------------------------------------------------

def _append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    """
    Append one JSON object as a line to the given path.
    """
    try:
        with path.open("ab") as f:
            f.write(json.dumps(row, separators=(",", ":")).encode("utf-8"))
            f.write(b"\n")
    except Exception as e:
        try:
            log.warning("feature_logger: failed to append to %s: %r", path, e)
        except Exception:
            # worst case: silent failure, but don't crash caller
            pass


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def log_features_at_open(
    *,
    trade_id: str,
    ts_open_ms: int,
    symbol: str,
    sub_uid: Optional[str],
    strategy_name: str,
    setup_type: str,
    mode: str,
    features: Dict[str, Any],
) -> None:
    """
    Persist a single "setup features at open" record.

    Parameters
    ----------
    trade_id      : unique trade id (same as orderLinkId / AI event trade_id)
    ts_open_ms    : timestamp in ms at decision / entry time
    symbol        : e.g. "BTCUSDT"
    sub_uid       : unified-sub UID as string, or None for main
    strategy_name : human-readable name from strategies.yaml
    setup_type    : e.g. "breakout", "pullback", "range_fade", etc.
    mode          : "PAPER", "LIVE_CANARY", "LIVE_FULL", etc.
    features      : dict of numeric / categorical features

    Notes
    -----
    - Does **not** raise on failure; errors are logged and swallowed.
    - Schema is intentionally explicit + versioned: "setup_features_v1".
    """
    try:
        # Basic normalization
        trade_id_str = str(trade_id)
        symbol_str = str(symbol)
        strat_str = str(strategy_name)
        setup_type_str = str(setup_type or "unknown")
        mode_str = str(mode or "UNKNOWN")

        # Stable, versioned schema
        row: Dict[str, Any] = {
            "schema_version": "setup_features_v1",
            "trade_id": trade_id_str,
            "ts_open_ms": int(ts_open_ms),
            "symbol": symbol_str,
            "sub_uid": str(sub_uid) if sub_uid not in (None, "") else None,
            "strategy_name": strat_str,
            "setup_type": setup_type_str,
            "mode": mode_str,
            "features": features or {},
        }

        _append_jsonl(SETUPS_FEATURES_PATH, row)

    except Exception as e:
        try:
            log.warning(
                "feature_logger: failed to log features for trade_id=%r symbol=%r: %r",
                trade_id,
                symbol,
                e,
            )
        except Exception:
            # Hard stop: we never crash executor_v2 over feature logging.
            pass
