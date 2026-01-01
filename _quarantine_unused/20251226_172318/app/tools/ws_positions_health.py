#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — WS / Positions Bus Health Check

Goals
-----
- Inspect state/positions_bus.json and verify:
    * File exists and is recent enough.
    * It has labels for expected accounts (main, subaccounts).
    * Each label has a positions list (even if empty).

Usage
-----
    python -m app.tools.ws_positions_health
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Set

import sys

try:
    import yaml  # type: ignore
except Exception:
    print("[ws_positions_health] ERROR: PyYAML not installed. Run `pip install pyyaml`.")
    sys.exit(1)

try:
    from app.core.config import settings
except ImportError:
    from core.config import settings  # type: ignore


ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])
STATE_DIR: Path = ROOT / "state"
CONFIG_DIR: Path = ROOT / "config"

POSITIONS_BUS_PATH: Path = STATE_DIR / "positions_bus.json"
SUBS_FILE: Path = CONFIG_DIR / "subaccounts.yaml"

# How old (seconds) before we consider the snapshot stale
DEFAULT_MAX_AGE_SEC = 15.0


def _load_yaml(path: Path) -> Any:
    if not path.exists():
        print(f"[ws_positions_health] WARNING: {path} does not exist.")
        return None
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)  # type: ignore


def _expected_labels_from_subaccounts(data: Any) -> Set[str]:
    """
    Derive expected account labels from subaccounts.yaml.

    We try (in order) per entry:
      - account_label
      - label
      - name
    For sub_uid=None, we assume "main" as label.
    """
    labels: Set[str] = set()
    if not isinstance(data, dict):
        return labels

    entries = data.get("accounts") or data.get("subaccounts")
    if not isinstance(entries, list):
        return labels

    for row in entries:
        if not isinstance(row, dict):
            continue
        sub_uid = row.get("sub_uid")
        label = (
            row.get("account_label")
            or row.get("label")
            or row.get("name")
        )
        if sub_uid is None:
            # unified main
            label = label or "main"
        if not label:
            continue
        labels.add(str(label))
    return labels


def _load_positions_bus(path: Path) -> Dict[str, Any] | None:
    if not path.exists():
        print(f"[ws_positions_health] ERROR: {path} does not exist.")
        return None
    try:
        raw = path.read_text(encoding="utf-8")
        return json.loads(raw)
    except Exception as e:
        print(f"[ws_positions_health] ERROR: failed to parse {path}: {e}")
        return None


def main() -> None:
    print("[ws_positions_health] ROOT:", ROOT)

    subs_raw = _load_yaml(SUBS_FILE)
    expected_labels = _expected_labels_from_subaccounts(subs_raw)
    if not expected_labels:
        print("[ws_positions_health] WARNING: Could not derive expected labels from subaccounts.yaml")
    else:
        print("[ws_positions_health] Expected account labels from subaccounts.yaml:", sorted(expected_labels))

    snapshot = _load_positions_bus(POSITIONS_BUS_PATH)
    if not snapshot:
        print("[ws_positions_health] ABORT: no valid positions_bus snapshot.")
        return

    updated_ms = snapshot.get("updated_ms") or snapshot.get("ts_ms")
    labels = snapshot.get("labels") or {}

    if not isinstance(labels, dict):
        print("[ws_positions_health] ERROR: positions_bus.json has no 'labels' dict.")
        return

    now_ms = int(time.time() * 1000)
    age_sec = None
    if isinstance(updated_ms, (int, float)):
        age_sec = (now_ms - int(updated_ms)) / 1000.0

    print(f"[ws_positions_health] Snapshot age: {age_sec:.2f}s" if age_sec is not None else "[ws_positions_health] Snapshot age: unknown")
    if age_sec is not None and age_sec > DEFAULT_MAX_AGE_SEC:
        print(f"[ws_positions_health] WARNING: positions_bus snapshot is stale (> {DEFAULT_MAX_AGE_SEC}s).")

    present_labels = set(labels.keys())
    print("[ws_positions_health] Labels present in positions_bus:", sorted(present_labels))

    if expected_labels:
        missing = expected_labels - present_labels
        extra = present_labels - expected_labels

        if missing:
            print("[ws_positions_health] WARNING: expected labels MISSING from positions_bus:")
            for lab in sorted(missing):
                print("  -", lab)
        else:
            print("[ws_positions_health] OK: all expected labels are present in positions_bus.")

        if extra:
            print("[ws_positions_health] INFO: labels in positions_bus not listed in subaccounts.yaml:")
            for lab in sorted(extra):
                print("  -", lab)

    # Per-label summary
    print()
    print("[ws_positions_health] Per-label positions summary:")
    for lab in sorted(present_labels):
        entry = labels.get(lab) or {}
        positions = entry.get("positions") or []
        category = entry.get("category", "?")
        print(f"  - {lab}: category={category}, open_positions={len(positions)}")

    print()
    print("[ws_positions_health] Completed positions bus health check.")


if __name__ == "__main__":
    main()
