#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Fleet Manifest Builder (canonical)

Purpose
-------
Generate config/fleet_manifest.yaml from canonical configs:

  - config/strategies.yaml   (primary source for symbols/TFs/setup_types/mode/etc.)
  - config/subaccounts.yaml  (secondary source for enable_ai_stack sanity + uid cross-check)

Why
---
Your runtime stack (orchestrator/watchdog/cockpit supervisor) reads fleet_manifest.yaml directly.
If fleet_manifest drifts from strategies/subaccounts, your system becomes "correctly wrong".

Policy
------
- strategies.yaml and subaccounts.yaml are CANONICAL truth.
- fleet_manifest.yaml is GENERATED output only (do not hand-edit).

Safety
------
- MAIN is forced enabled=false in fleet_manifest to prevent accidental auto-trading.
  (Main can still be displayed in cockpit; executor must refuse OFF anyway.)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
import sys

try:
    import yaml  # type: ignore
except Exception as e:
    print(f"FATAL: PyYAML not installed or failed to import: {e}")
    print("Install with: pip install pyyaml")
    sys.exit(2)

ROOT = Path(__file__).resolve().parents[2]
CFG = ROOT / "config"

STRATEGIES_PATH = CFG / "strategies.yaml"
SUBACCOUNTS_PATH = CFG / "subaccounts.yaml"
OUT_PATH = CFG / "fleet_manifest.yaml"


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(str(path))
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"{path.name}: root must be a mapping")
    return data


def _index_subaccounts(sub_cfg: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    # Top-level mapping is canonical in your subaccounts.yaml
    # (ignore legacy container)
    out: Dict[str, Dict[str, Any]] = {}
    for k, v in sub_cfg.items():
        if not isinstance(v, dict):
            continue
        if k == "legacy":
            continue
        out[k] = v
    return out


def _bool(x: Any, default: bool = False) -> bool:
    if x is None:
        return default
    return bool(x)


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _coerce_uid(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        return None


def _promote_rules(strategy_row: Dict[str, Any]) -> Dict[str, Any]:
    # fleet_manifest in your current file flattens these keys at top level
    pr = strategy_row.get("promotion_rules")
    if not isinstance(pr, dict):
        return {"promotion_rules": None}

    out: Dict[str, Any] = {"promotion_rules": None}
    if _bool(pr.get("enabled"), False):
        out["promotion_rules"] = None  # keep null for backward compat with existing consumers
        # Flatten common fields if present
        for k in ("min_trades", "min_winrate", "min_avg_r", "min_expectancy_r", "max_drawdown_pct"):
            if k in pr:
                out[k] = pr.get(k)
    return out


def build_manifest() -> Dict[str, Any]:
    strat_cfg = _load_yaml(STRATEGIES_PATH)
    sub_cfg = _load_yaml(SUBACCOUNTS_PATH)

    subs_index = _index_subaccounts(sub_cfg)

    subs_list = strat_cfg.get("subaccounts") or []
    if not isinstance(subs_list, list):
        raise ValueError("strategies.yaml: top-level 'subaccounts' must be a list")

    fleet_rows: List[Dict[str, Any]] = []

    for row in subs_list:
        if not isinstance(row, dict):
            continue

        label = str(row.get("account_label") or "").strip()
        if not label:
            continue

        # Pull enable_ai_stack from subaccounts.yaml if present (fallback to strategies enable_ai_stack if any)
        sub_entry = subs_index.get(label) or {}
        enable_ai_stack = _bool(sub_entry.get("enable_ai_stack"), _bool(row.get("enable_ai_stack"), True))

        # Enabled: AND-gate strategies.enabled + subaccounts.enabled
        # BUT: force main enabled=false in fleet manifest as a safety invariant.
        enabled_strategy = _bool(row.get("enabled"), False)
        enabled_subacct = _bool(sub_entry.get("enabled"), True)  # default allow if missing
        enabled = enabled_strategy and enabled_subacct

        if label == "main":
            enabled = False
            enable_ai_stack = False  # main should not run the AI stack for auto entries

        sub_uid = _coerce_uid(row.get("sub_uid"))
        # Cross-check with subaccounts.yaml uid if present (do not fail hard, just prefer strategies.yaml)
        sub_uid_sub = _coerce_uid(sub_entry.get("sub_uid"))
        if sub_uid is None and sub_uid_sub is not None:
            sub_uid = sub_uid_sub

        out_row: Dict[str, Any] = {
            "account_label": label,
            "sub_uid": sub_uid,
            "enabled": enabled,
            "enable_ai_stack": enable_ai_stack,
            "strategy_name": row.get("strategy_name"),
            "role": row.get("role"),
            "automation_mode": row.get("automation_mode"),
            "ai_profile": row.get("ai_profile"),
            "risk_pct": _safe_float(row.get("risk_pct", row.get("risk_per_trade_pct", 0.0)), 0.0),
            "max_concurrent_positions": int(row.get("max_concurrent_positions", 0) or 0),
            "exit_profile": row.get("exit_profile"),
            "timeframes": row.get("timeframes") or [],
            "symbols": row.get("symbols") or [],
            "setup_types": row.get("setup_types") or [],
        }

        out_row.update(_promote_rules(row))
        fleet_rows.append(out_row)

    manifest = {
        "version": 1,
        "generated_from": {
            "strategies": "config/strategies.yaml",
            "subaccounts": "config/subaccounts.yaml",
        },
        "fleet": fleet_rows,
    }
    return manifest


def main() -> int:
    try:
        manifest = build_manifest()
    except Exception as e:
        print(f"FATAL: {e}")
        return 2

    header = (
        "# config/fleet_manifest.yaml\n"
        "#\n"
        "# GENERATED FILE — DO NOT EDIT BY HAND.\n"
        "# Built by: app/tools/build_fleet_manifest.py\n"
        "# Source of truth: config/strategies.yaml + config/subaccounts.yaml\n"
        "#\n"
    )

    OUT_PATH.write_text(
        header + yaml.safe_dump(manifest, sort_keys=False, default_flow_style=False),
        encoding="utf-8",
    )

    print(f"OK: wrote {OUT_PATH}")
    # Quick summary counts
    fleet = manifest.get("fleet") or []
    enabled = sum(1 for r in fleet if isinstance(r, dict) and r.get("enabled") is True)
    total = sum(1 for r in fleet if isinstance(r, dict))
    print(f"fleet: total={total} enabled={enabled}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
