#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Position Bus (WS/REST-normalized mirror over positions)

Purpose
-------
A thin abstraction layer that everyone else (TP/SL Manager, guards, executor,
AI snapshot builder, etc.) can query for "current positions" without caring
whether the data comes from:

  - A WS-fed snapshot file (written by ws_switchboard), or
  - A direct REST call to Bybit (fallback).

Design
------
- Snapshot file:   state/positions_bus.json
- Structure (v2, normalized):
    {
      "version": 2,
      "updated_ms": 1763752000123,
      "labels": {
        "main": {
          "category": "linear",
          "positions": [
            {
              "symbol": "BTCUSDT",
              "side": "Buy",
              "size": 0.25,
              "avgPrice": 43200.5,
              "stopLoss": 0.0,
              "sub_uid": "12345",
              "account_label": "main",
              "category": "linear"
            },
            ...
          ]
        },
        "flashback03": {
          "category": "linear",
          "positions": [ ...normalized rows... ]
        }
      }
    }

Older snapshots with version=1 and raw Bybit rows are still readable; we
normalize rows when returning them to callers.

Callers typically use:
    from app.core.position_bus import (
        get_positions_for_label,
        get_position_map_for_label,
        get_positions_snapshot,
        get_positions_for_current_label,
        get_snapshot,
    )
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import orjson

# Tolerant imports so tests / alt layouts still work
try:
    from app.core.config import settings
    from app.core.flashback_common import list_open_positions
except ImportError:  # pragma: no cover
    from core.config import settings  # type: ignore
    from core.flashback_common import list_open_positions  # type: ignore


ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])
STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

POS_SNAPSHOT_PATH: Path = STATE_DIR / "positions_bus.json"

# Max age (seconds) before snapshot is considered stale
_POSITION_BUS_MAX_AGE_SECONDS: int = int(os.getenv("POSITION_BUS_MAX_AGE_SECONDS", "3"))

# Whether REST fallback is allowed to *write* the snapshot
_POSITION_BUS_ALLOW_REST_WRITE: bool = (
    os.getenv("POSITION_BUS_ALLOW_REST_WRITE", "true").strip().lower()
    in ("1", "true", "yes")
)

# Logical label for "this" account/process (main, flashback10, etc.)
ACCOUNT_LABEL: str = os.getenv("ACCOUNT_LABEL", "main").strip() or "main"

_CANONICAL_VERSION: int = 2  # normalized schema version


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(x: Any) -> float:
    """Safely convert anything to float, fallback = 0.0."""
    try:
        return float(x)
    except Exception:
        return 0.0


def _normalize_entry(
    row: Dict[str, Any],
    label: str,
    category: str = "linear",
) -> Optional[Dict[str, Any]]:
    """
    Normalize a raw position row (WS v1, WS v2, REST) into canonical schema.
    """
    try:
        symbol = str(row.get("symbol", "")).upper()
        if not symbol:
            return None

        side = str(row.get("side", "")).title()  # "Buy" / "Sell" or empty

        size = _safe_float(
            row.get("size")
            or row.get("positionValue")
            or 0
        )
        avg_price = _safe_float(
            row.get("avgPrice")
            or row.get("entryPrice")
            or row.get("avg_entry_price")
            or 0
        )

        sl_raw = (
            row.get("stopLoss")
            or row.get("stopLossPrice")
            or row.get("slPrice")
            or row.get("stop_loss")
            or 0
        )
        stop_loss = _safe_float(sl_raw)

        sub_uid = (
            row.get("sub_uid")
            or row.get("subAccountId")
            or row.get("accountId")
            or row.get("subId")
            or ""
        )
        sub_uid = str(sub_uid)

        acct_label = str(row.get("account_label") or label or ACCOUNT_LABEL).strip()
        cat = str(row.get("category") or category or "linear").lower()

        return {
            "symbol": symbol,
            "side": side,
            "size": size,
            "avgPrice": avg_price,
            "stopLoss": stop_loss,
            "sub_uid": sub_uid,
            "account_label": acct_label,
            "category": cat,
        }
    except Exception:
        return None


def _load_snapshot_raw() -> Optional[Dict[str, Any]]:
    """
    Load the entire snapshot dict from positions_bus.json, or None if missing/invalid.
    """
    try:
        if not POS_SNAPSHOT_PATH.exists():
            return None
        data = orjson.loads(POS_SNAPSHOT_PATH.read_bytes())
        if not isinstance(data, dict):
            return None
        return data
    except Exception:
        return None


def _snapshot_age_seconds(snap: Dict[str, Any]) -> Optional[float]:
    """
    Return age in seconds if possible, else None.
    """
    try:
        updated_ms = int(snap.get("updated_ms"))
    except Exception:
        return None
    now_ms = _now_ms()
    if updated_ms <= 0 or now_ms <= updated_ms:
        return None
    return (now_ms - updated_ms) / 1000.0


def _save_snapshot(
    labels_positions: Dict[str, Dict[str, Any]],
) -> None:
    """
    Save a complete snapshot to disk, using canonical schema version.
    """
    snap = {
        "version": _CANONICAL_VERSION,
        "updated_ms": _now_ms(),
        "labels": labels_positions,
    }
    try:
        POS_SNAPSHOT_PATH.write_bytes(orjson.dumps(snap))
    except Exception:
        pass


def get_snapshot() -> Tuple[Optional[Dict[str, Any]], Optional[float]]:
    """
    Return (snapshot_dict, age_seconds).

    If the file is missing or invalid, returns (None, None).
    """
    snap = _load_snapshot_raw()
    if snap is None:
        return None, None
    age = _snapshot_age_seconds(snap)
    return snap, age


def _extract_label_positions_raw(
    snap: Dict[str, Any],
    label: str,
    category: str,
) -> List[Dict[str, Any]]:
    """
    Given a snapshot, return the *raw* positions list for the given label+category.
    """
    labels = snap.get("labels") or {}
    entry = labels.get(label)
    if not isinstance(entry, dict):
        return []
    entry_cat = str(entry.get("category", "")).lower()
    if entry_cat and entry_cat != category.lower():
        return []
    positions = entry.get("positions") or []
    if not isinstance(positions, list):
        return []
    return positions


def _rest_fetch_positions(category: str) -> List[Dict[str, Any]]:
    """
    Fallback to REST: use flashback_common.list_open_positions() and return its result.
    """
    try:
        rows = list_open_positions()
        if not isinstance(rows, list):
            return []
        return rows
    except Exception:
        return []


def _rest_refresh_snapshot_for_label(
    label: str,
    category: str,
    existing_snap: Optional[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Call REST to fetch positions for label+category (currently MAIN only),
    merge into a snapshot dict, and return (normalized_positions, updated_snapshot).
    """
    raw_positions = _rest_fetch_positions(category=category)

    norm_positions: List[Dict[str, Any]] = []
    for row in raw_positions:
        norm = _normalize_entry(row, label=label, category=category)
        if norm:
            norm_positions.append(norm)

    if existing_snap is None:
        labels_block: Dict[str, Dict[str, Any]] = {}
    else:
        labels_block = dict(existing_snap.get("labels") or {})

    labels_block[label] = {
        "category": category,
        "positions": norm_positions,
    }

    new_snap = {
        "version": _CANONICAL_VERSION,
        "updated_ms": _now_ms(),
        "labels": labels_block,
    }
    return norm_positions, new_snap


def get_positions_for_label(
    label: Optional[str] = "main",
    category: str = "linear",
    max_age_seconds: Optional[int] = None,
    allow_rest_fallback: bool = True,
) -> List[Dict[str, Any]]:
    """
    Main entry point: return normalized positions for a given label + category.
    """
    effective_label = label if isinstance(label, str) else None
    if not effective_label:
        effective_label = ACCOUNT_LABEL
    label = effective_label

    if max_age_seconds is None:
        max_age_seconds = _POSITION_BUS_MAX_AGE_SECONDS

    # Snapshot path
    snap, age = get_snapshot()
    if snap is not None and age is not None and age <= max_age_seconds:
        raw_positions = _extract_label_positions_raw(
            snap,
            label=label,
            category=category,
        )
        if raw_positions:
            norm_positions: List[Dict[str, Any]] = []
            for row in raw_positions:
                norm = _normalize_entry(row, label=label, category=category)
                if norm:
                    norm_positions.append(norm)
            if norm_positions:
                return norm_positions

    # REST fallback
    if not allow_rest_fallback:
        return []

    if label.lower() != "main":
        return []

    norm_positions, new_snap = _rest_refresh_snapshot_for_label(
        label="main",
        category=category,
        existing_snap=snap,
    )

    if _POSITION_BUS_ALLOW_REST_WRITE:
        _save_snapshot(new_snap.get("labels") or {})

    return norm_positions


def get_position_map_for_label(
    label: Optional[str] = "main",
    category: str = "linear",
    max_age_seconds: Optional[int] = None,
    allow_rest_fallback: bool = True,
    key_field: str = "symbol",
) -> Dict[str, Dict[str, Any]]:
    """
    Convenience wrapper: return a dict keyed by `key_field` (default "symbol").
    """
    rows = get_positions_for_label(
        label=label,
        category=category,
        max_age_seconds=max_age_seconds,
        allow_rest_fallback=allow_rest_fallback,
    )
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        try:
            key = str(row.get(key_field))
        except Exception:
            continue
        if not key:
            continue
        out[key] = row
    return out


def get_positions_for_current_label(
    category: str = "linear",
    max_age_seconds: Optional[int] = None,
    allow_rest_fallback: bool = True,
) -> List[Dict[str, Any]]:
    """
    Convenience: use ACCOUNT_LABEL as the label.
    """
    return get_positions_for_label(
        label=ACCOUNT_LABEL,
        category=category,
        max_age_seconds=max_age_seconds,
        allow_rest_fallback=allow_rest_fallback,
    )


def get_positions_snapshot(
    label: Optional[str] = None,
    category: str = "linear",
    max_age_seconds: Optional[int] = None,
    allow_rest_fallback: bool = True,
) -> List[Dict[str, Any]]:
    """
    Compatibility alias used by tp_sl_manager and other modules.
    Returns normalized rows in canonical schema.
    """
    effective_label = label or ACCOUNT_LABEL
    return get_positions_for_label(
        label=effective_label,
        category=category,
        max_age_seconds=max_age_seconds,
        allow_rest_fallback=allow_rest_fallback,
    )
