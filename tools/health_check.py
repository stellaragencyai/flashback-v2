#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Health Check (v2.1, less neurotic)

Goals:
- Confirm config loads and core bus/heartbeat telemetry exists and is reasonably fresh.
- Do NOT fail just because:
    * a subaccount isn't currently running its switchboard
    * positions_bus hasn't updated (no positions + no WS position events)
- Provide strict mode toggles when you want production-level enforcement.

Env toggles:
- HEALTH_POSITIONS_BUS_MAX_AGE_SEC (default 10)
- HEALTH_HEARTBEAT_MAX_AGE_SEC     (default 60)
- HC_REQUIRE_ALL_HEARTBEATS        (default false)
- HC_REQUIRE_POSITIONS_BUS_FRESH   (default false)
- HC_REQUIRE_LABEL_PRESENT         (default false)  # require label exists in positions_bus labels
"""

from __future__ import annotations

import os
import sys
import time
import json
from pathlib import Path
from typing import Any, Dict, Tuple, Optional

try:
    from app.core.config import settings
    ROOT: Path = settings.ROOT  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[1]

STATE_DIR = ROOT / "state"
CONFIG_DIR = ROOT / "config"

SUBACCOUNTS_PATH = CONFIG_DIR / "subaccounts.yaml"
POSITIONS_BUS_PATH = STATE_DIR / "positions_bus.json"

HEALTH_POSITIONS_BUS_MAX_AGE_SEC = float(os.getenv("HEALTH_POSITIONS_BUS_MAX_AGE_SEC", "10"))
HEALTH_HEARTBEAT_MAX_AGE_SEC = float(os.getenv("HEALTH_HEARTBEAT_MAX_AGE_SEC", "60"))

REQUIRE_ALL_HEARTBEATS = os.getenv("HC_REQUIRE_ALL_HEARTBEATS", "false").strip().lower() in ("1", "true", "yes", "y")
REQUIRE_POSITIONS_BUS_FRESH = os.getenv("HC_REQUIRE_POSITIONS_BUS_FRESH", "false").strip().lower() in ("1", "true", "yes", "y")
REQUIRE_LABEL_PRESENT = os.getenv("HC_REQUIRE_LABEL_PRESENT", "false").strip().lower() in ("1", "true", "yes", "y")


def _now() -> float:
    return time.time()


def _file_age_seconds(path: Path) -> Optional[float]:
    try:
        if not path.exists():
            return None
        return max(0.0, _now() - path.stat().st_mtime)
    except Exception:
        return None


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _load_subaccounts_labels() -> Dict[str, Dict[str, Any]]:
    """
    subaccounts.yaml is a top-level mapping:
      main: {...}
      flashback01: {...}
      ...
      legacy: {...}  (ignored)
    """
    import yaml  # type: ignore

    if not SUBACCOUNTS_PATH.exists():
        raise FileNotFoundError(f"Missing config/subaccounts.yaml at {SUBACCOUNTS_PATH}")

    data = yaml.safe_load(SUBACCOUNTS_PATH.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError("subaccounts.yaml root must be a mapping")

    out: Dict[str, Dict[str, Any]] = {}
    for k, v in data.items():
        if k == "version" or k == "notes" or k == "legacy":
            continue
        if isinstance(v, dict):
            out[str(k)] = v
    return out


def _status(prefix: str, ok: bool, msg: str) -> None:
    tag = "OK" if ok else "FAIL"
    print(f"[{tag}] {prefix} {msg}")


def _warn(prefix: str, msg: str) -> None:
    print(f"[WARN] {prefix} {msg}")


def main() -> int:
    print(f"ROOT: {ROOT}")
    failures = 0

    # 1) Load subaccounts
    try:
        labels = _load_subaccounts_labels()
        print(f"[OK] subaccounts labels loaded: {len(labels)}")
    except Exception as e:
        print(f"[FAIL] subaccounts.yaml load failed: {e}")
        return 2

    # Determine enabled labels
    enabled_labels = []
    for label, cfg in labels.items():
        enabled = bool(cfg.get("enabled", True))
        if not enabled:
            print(f"[SKIP] {label}: enabled=false")
            continue
        enabled_labels.append(label)

    # 2) positions_bus freshness (soft by default)
    pos_age = _file_age_seconds(POSITIONS_BUS_PATH)
    pos_bus = _load_json(POSITIONS_BUS_PATH)
    pos_labels = pos_bus.get("labels") if isinstance(pos_bus, dict) else {}
    if not isinstance(pos_labels, dict):
        pos_labels = {}

    if pos_age is None:
        if REQUIRE_POSITIONS_BUS_FRESH:
            _status("positions_bus", False, f"missing: {POSITIONS_BUS_PATH}")
            failures += 1
        else:
            _warn("positions_bus", f"missing (soft): {POSITIONS_BUS_PATH}")
    else:
        fresh = pos_age <= HEALTH_POSITIONS_BUS_MAX_AGE_SEC
        if fresh:
            _status("positions_bus", True, f"fresh: age={pos_age:.2f}s")
        else:
            # Soft by default because positions_bus only updates on WS position events.
            if REQUIRE_POSITIONS_BUS_FRESH:
                _status("positions_bus", False, f"stale: age={pos_age:.2f}s > {HEALTH_POSITIONS_BUS_MAX_AGE_SEC:.2f}s")
                failures += 1
            else:
                _warn("positions_bus", f"stale (soft): age={pos_age:.2f}s > {HEALTH_POSITIONS_BUS_MAX_AGE_SEC:.2f}s")

    # 3) Heartbeats: strict only if you require all
    for label in enabled_labels:
        hb_path = STATE_DIR / f"ws_switchboard_heartbeat_{label}.txt"
        hb_age = _file_age_seconds(hb_path)

        if hb_age is None:
            if REQUIRE_ALL_HEARTBEATS:
                _status(f"{label}:", False, f"heartbeat missing: {hb_path}")
                failures += 1
            else:
                _warn(f"{label}:", f"heartbeat missing (soft): {hb_path}")
        else:
            if hb_age <= HEALTH_HEARTBEAT_MAX_AGE_SEC:
                _status(f"{label}:", True, f"heartbeat age={hb_age:.2f}s")
            else:
                if REQUIRE_ALL_HEARTBEATS:
                    _status(f"{label}:", False, f"heartbeat stale age={hb_age:.2f}s > {HEALTH_HEARTBEAT_MAX_AGE_SEC:.2f}s ({hb_path})")
                    failures += 1
                else:
                    _warn(f"{label}:", f"heartbeat stale (soft) age={hb_age:.2f}s > {HEALTH_HEARTBEAT_MAX_AGE_SEC:.2f}s ({hb_path})")

        # 4) Label present in positions_bus (WARN unless strict)
        if label not in pos_labels:
            if REQUIRE_LABEL_PRESENT:
                _status(f"{label}:", False, "not present in positions_bus labels")
                failures += 1
            else:
                _warn(f"{label}:", "not present in positions_bus labels (may be no open positions yet)")

    # Summary
    if failures > 0:
        print(f"\n[FAIL] Health check FAILED ❌  (failures={failures})")
        return 1

    print("\n[OK] Health check PASSED ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
