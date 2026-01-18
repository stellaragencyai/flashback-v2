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
Legacy global snapshot file:
  - state/positions_bus.json

Per-label snapshot file (preferred in multi-subaccount orchestrator mode):
  - state/positions_bus_<label>.json

Structure (v2, normalized):
    {
      "version": 2,
      "updated_ms": 1763752000123,
      "labels": {
        "<label>": {
          "category": "linear",
          "positions": [ ... normalized rows ... ]
        }
      }
    }

Older snapshots with version=1 and raw Bybit rows are still readable; we
normalize rows when returning them to callers.
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

# Legacy global snapshot path (back-compat)
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

# Mirror of flashback_common.EXEC_DRY_RUN (avoid importing to keep this module stable)
EXEC_DRY_RUN: bool = os.getenv("EXEC_DRY_RUN", "false").strip().lower() in ("1", "true", "yes", "y", "on")

# Optional override. Supports either:
#  - absolute path to a json file
#  - template with "{label}" placeholder (recommended)
# Special values treated as AUTO:
#  - "auto", "(auto)"
POSITIONS_BUS_PATH_ENV: str = os.getenv("POSITIONS_BUS_PATH", "").strip()

_CANONICAL_VERSION: int = 2  # normalized schema version


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(x: Any) -> float:
    """Safely convert anything to float, fallback = 0.0."""
    try:
        return float(x)
    except Exception:
        return 0.0


def _is_auto_bus_value(v: str) -> bool:
    s = (v or "").strip().lower()
    return s in ("auto", "(auto)")


def _resolve_positions_bus_path(label: str) -> Path:
    """
    Resolve the positions bus file path for a given label.

    Priority:
      1) POSITIONS_BUS_PATH (supports "{label}" templating) unless it's AUTO
      2) state/positions_bus_<label>.json (preferred default)
      3) state/positions_bus.json (legacy fallback)
    """
    lab = (label or "").strip() or "main"

    if POSITIONS_BUS_PATH_ENV and not _is_auto_bus_value(POSITIONS_BUS_PATH_ENV):
        try:
            candidate = POSITIONS_BUS_PATH_ENV.format(label=lab)
        except Exception:
            candidate = POSITIONS_BUS_PATH_ENV
        return Path(candidate)

    if lab:
        return STATE_DIR / f"positions_bus_{lab}.json"

    return POS_SNAPSHOT_PATH


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

        acct_label = str(row.get("account_label") or label or "main").strip()
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


def _load_snapshot_raw_from_path(path: Path) -> Optional[Dict[str, Any]]:
    """
    Load snapshot dict from a given file path, or None if missing/invalid.
    """
    try:
        if not path.exists():
            return None
        data = orjson.loads(path.read_bytes())
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


def _save_snapshot_to_path(path: Path, snap: Dict[str, Any]) -> None:
    """
    Save snapshot dict to a given file path.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(orjson.dumps(snap))


def _save_snapshot_for_label(
    label: str,
    labels_positions: Dict[str, Dict[str, Any]],
) -> None:
    """
    Save a complete snapshot to disk using canonical schema version, writing to:
      - per-label file (preferred)
      - and also merging into legacy global file for compatibility
    """
    lab = (label or "").strip() or "main"

    snap = {
        "version": _CANONICAL_VERSION,
        "updated_ms": _now_ms(),
        "labels": labels_positions,
    }

    # Write per-label snapshot
    per_path = _resolve_positions_bus_path(lab)
    _save_snapshot_to_path(per_path, snap)

    # Also merge into legacy global file (best-effort)
    try:
        global_snap = _load_snapshot_raw_from_path(POS_SNAPSHOT_PATH)
        global_labels: Dict[str, Dict[str, Any]] = {}
        if isinstance(global_snap, dict):
            global_labels = dict(global_snap.get("labels") or {})
        # merge
        for k, v in (labels_positions or {}).items():
            global_labels[k] = v
        merged = {
            "version": _CANONICAL_VERSION,
            "updated_ms": _now_ms(),
            "labels": global_labels,
        }
        _save_snapshot_to_path(POS_SNAPSHOT_PATH, merged)
    except Exception:
        pass


def get_snapshot_for_label(
    label: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[float], Path]:
    """
    Return (snapshot_dict, age_seconds, path_used) for a label.
    Preference order:
      - per-label file
      - legacy global file
    """
    lab = (label or "").strip() or "main"

    per_path = _resolve_positions_bus_path(lab)
    snap = _load_snapshot_raw_from_path(per_path)
    if snap is not None:
        return snap, _snapshot_age_seconds(snap), per_path

    # fallback: global
    gsnap = _load_snapshot_raw_from_path(POS_SNAPSHOT_PATH)
    if gsnap is None:
        return None, None, per_path
    return gsnap, _snapshot_age_seconds(gsnap), POS_SNAPSHOT_PATH


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

    IMPORTANT SAFETY DEFAULT:
      - If label is None/empty, we default to "main" (NOT ACCOUNT_LABEL).
        This prevents legacy callers from accidentally managing a subaccount.
      - If you want the current process label, call get_positions_for_current_label()
        or pass label=ACCOUNT_LABEL explicitly.
    """
    effective_label = label if isinstance(label, str) else None
    if not effective_label or not effective_label.strip():
        effective_label = "main"
    lab = effective_label.strip()

    if max_age_seconds is None:
        max_age_seconds = _POSITION_BUS_MAX_AGE_SECONDS

    # Snapshot (label-scoped preference)
    snap, age, _path_used = get_snapshot_for_label(lab)

    # DRY_RUN: keep positions bus fresh even when empty (no WS/REST dependency)
    if EXEC_DRY_RUN:
        if snap is None or age is None or age > float(max_age_seconds):
            labels_block = {}
            if isinstance(snap, dict):
                labels_block = dict(snap.get("labels") or {})
            labels_block[lab] = {"category": category, "positions": []}
            _save_snapshot_for_label(lab, labels_block)
            return []

    if snap is not None and age is not None and age <= max_age_seconds:
        raw_positions = _extract_label_positions_raw(
            snap,
            label=lab,
            category=category,
        )
        if raw_positions:
            norm_positions: List[Dict[str, Any]] = []
            for row in raw_positions:
                norm = _normalize_entry(row, label=lab, category=category)
                if norm:
                    norm_positions.append(norm)
            if norm_positions:
                return norm_positions

    # REST fallback
    if not allow_rest_fallback:
        return []

    # NOTE: REST fallback is only supported for MAIN right now.
    if lab.lower() != "main":
        return []

    norm_positions, new_snap = _rest_refresh_snapshot_for_label(
        label="main",
        category=category,
        existing_snap=snap,
    )

    if _POSITION_BUS_ALLOW_REST_WRITE:
        _save_snapshot_for_label("main", new_snap.get("labels") or {})

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

    SAFETY:
      - If label is None, default is "main" (see get_positions_for_label()).
    """
    return get_positions_for_label(
        label=label,
        category=category,
        max_age_seconds=max_age_seconds,
        allow_rest_fallback=allow_rest_fallback,
    )
