#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Flashback — Ops State (single source of truth)

Writes/reads a single JSON snapshot:
  state/ops_snapshot.json

All workers/tools should write here so you have ONE truth surface.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional

try:
    from app.core.config import settings
    ROOT: Path = Path(settings.ROOT)  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

OPS_PATH: Path = ROOT / "state" / "ops_snapshot.json"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}.{time.time_ns()}")

    try:
        tmp.write_text(data, encoding="utf-8")
    except Exception:
        # fallback
        path.write_text(data, encoding="utf-8")
        return

    for _ in range(5):
        try:
            os.replace(str(tmp), str(path))
            return
        except Exception:
            time.sleep(0.05)

    # last resort
    path.write_text(data, encoding="utf-8")
    try:
        if tmp.exists():
            tmp.unlink()
    except Exception:
        pass


def read_ops_snapshot() -> Dict[str, Any]:
    try:
        if not OPS_PATH.exists():
            return {"version": 1, "updated_ms": 0, "components": {}}
        data = json.loads(OPS_PATH.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return {"version": 1, "updated_ms": 0, "components": {}}
        data.setdefault("version", 1)
        data.setdefault("updated_ms", 0)
        data.setdefault("components", {})
        if not isinstance(data["components"], dict):
            data["components"] = {}
        return data
    except Exception:
        return {"version": 1, "updated_ms": 0, "components": {}}


def write_ops_snapshot(snapshot: Dict[str, Any]) -> None:
    if not isinstance(snapshot, dict):
        snapshot = {"version": 1, "updated_ms": 0, "components": {}}
    snapshot.setdefault("version", 1)
    snapshot["updated_ms"] = _now_ms()
    snapshot.setdefault("components", {})
    if not isinstance(snapshot["components"], dict):
        snapshot["components"] = {}
    _atomic_write_json(OPS_PATH, snapshot)


def write_component_status(
    component: str,
    account_label: str,
    ok: bool,
    details: Optional[Dict[str, Any]] = None,
    ts_ms: Optional[int] = None,
) -> None:
    """
    Standard status writer used by all workers and health checks.
    """
    snapshot = read_ops_snapshot()
    comps = snapshot.get("components")
    if not isinstance(comps, dict):
        comps = {}
        snapshot["components"] = comps

    key = f"{component}:{account_label}".strip(":")
    comps[key] = {
        "component": component,
        "account_label": account_label,
        "ok": bool(ok),
        "ts_ms": int(ts_ms or _now_ms()),
        "details": details or {},
    }

    write_ops_snapshot(snapshot)
