#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Subaccount & Strategy Sanity Check

Goals
-----
- Validate consistency between:
    * config/subaccounts.yaml
    * config/strategies.yaml

Checks
------
1) No duplicate sub_uid in either file.
2) Every non-null sub_uid in subaccounts.yaml appears in strategies.yaml (or is explicitly manual-only).
3) Every non-null sub_uid in strategies.yaml appears in subaccounts.yaml.
4) automation_mode is one of: OFF, LEARN_DRY, LIVE_CANARY, LIVE_FULL.
5) Basic summary per sub_uid (name, role, automation_mode, risk_pct, exit_profile).

Usage
-----
    python -m app.tools.subaccount_sanity
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple

import sys

try:
    import yaml  # type: ignore
except Exception:
    print("[subaccount_sanity] ERROR: PyYAML is not installed. Run `pip install pyyaml`.")
    sys.exit(1)

try:
    from app.core.config import settings
except ImportError:
    from core.config import settings  # type: ignore


ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])
CONFIG_DIR: Path = ROOT / "config"

SUBS_FILE: Path = CONFIG_DIR / "subaccounts.yaml"
STRATS_FILE: Path = CONFIG_DIR / "strategies.yaml"

VALID_MODES = {"OFF", "LEARN_DRY", "LIVE_CANARY", "LIVE_FULL"}


def _load_yaml(path: Path) -> Any:
    if not path.exists():
        print(f"[subaccount_sanity] WARNING: {path} does not exist.")
        return None
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)  # type: ignore


def _extract_subs_from_subaccounts(data: Any) -> List[Dict[str, Any]]:
    """
    Expect either:
      - { accounts: [ {...}, {...} ] }
      - { subaccounts: [ {...} ] }
      - or directly a list at root.
    """
    if data is None:
        return []

    if isinstance(data, dict):
        if isinstance(data.get("accounts"), list):
            return data["accounts"]  # type: ignore
        if isinstance(data.get("subaccounts"), list):
            return data["subaccounts"]  # type: ignore
        # Fallback: maybe everything is at root
        if isinstance(data.get("sub_uid"), (str, int)) or isinstance(data.get("name"), str):
            return [data]  # single entry
        return []

    if isinstance(data, list):
        return data

    return []


def _extract_subs_from_strategies(data: Any) -> List[Dict[str, Any]]:
    """
    For strategies.yaml we expect a 'subaccounts' list at root.
    """
    if data is None:
        return []
    if isinstance(data, dict):
        subs = data.get("subaccounts")
        if isinstance(subs, list):
            return subs
    return []


def _subkey(sub_uid: Any) -> str:
    """
    Normalize sub_uid into a canonical string key.
    """
    if sub_uid is None:
        return "MAIN"
    return str(sub_uid)


def main() -> None:
    print("[subaccount_sanity] ROOT:", ROOT)

    subs_raw = _load_yaml(SUBS_FILE)
    strats_raw = _load_yaml(STRATS_FILE)

    subs_from_subs = _extract_subs_from_subaccounts(subs_raw)
    subs_from_strats = _extract_subs_from_strategies(strats_raw)

    print(f"[subaccount_sanity] Loaded {len(subs_from_subs)} entries from subaccounts.yaml")
    print(f"[subaccount_sanity] Loaded {len(subs_from_strats)} entries from strategies.yaml")

    # Build maps: sub_uid_str -> info
    subs_map_from_subs: Dict[str, Dict[str, Any]] = {}
    subs_map_from_strats: Dict[str, Dict[str, Any]] = {}

    dup_subs_file: List[str] = []
    dup_strats_file: List[str] = []

    # --- From subaccounts.yaml ---
    for row in subs_from_subs:
        if not isinstance(row, dict):
            continue
        sub_uid = row.get("sub_uid")
        key = _subkey(sub_uid)
        if key in subs_map_from_subs:
            dup_subs_file.append(key)
        else:
            subs_map_from_subs[key] = row

    # --- From strategies.yaml ---
    for row in subs_from_strats:
        if not isinstance(row, dict):
            continue
        sub_uid = row.get("sub_uid")
        key = _subkey(sub_uid)
        if key in subs_map_from_strats:
            dup_strats_file.append(key)
        else:
            subs_map_from_strats[key] = row

    # 1) Duplicate sub_uid checks
    if dup_subs_file:
        print("[subaccount_sanity] ERROR: Duplicate sub_uid in subaccounts.yaml:", dup_subs_file)
    else:
        print("[subaccount_sanity] OK: no duplicate sub_uid in subaccounts.yaml")

    if dup_strats_file:
        print("[subaccount_sanity] ERROR: Duplicate sub_uid in strategies.yaml:", dup_strats_file)
    else:
        print("[subaccount_sanity] OK: no duplicate sub_uid in strategies.yaml")

    # 2) Cross-file consistency
    subs_ids = set(subs_map_from_subs.keys())
    strats_ids = set(subs_map_from_strats.keys())

    only_in_subs = subs_ids - strats_ids
    only_in_strats = strats_ids - subs_ids
    in_both = subs_ids & strats_ids

    if only_in_subs:
        print("[subaccount_sanity] WARNING: sub_uid present in subaccounts.yaml but NOT in strategies.yaml:")
        for key in sorted(only_in_subs):
            row = subs_map_from_subs[key]
            print(f"  - {key}: name={row.get('name')} role={row.get('role')}")
    else:
        print("[subaccount_sanity] OK: every sub_uid in subaccounts.yaml has a corresponding entry in strategies.yaml")

    if only_in_strats:
        print("[subaccount_sanity] WARNING: sub_uid present in strategies.yaml but NOT in subaccounts.yaml:")
        for key in sorted(only_in_strats):
            row = subs_map_from_strats[key]
            print(f"  - {key}: name={row.get('name')} role={row.get('role')}")
    else:
        print("[subaccount_sanity] OK: every sub_uid in strategies.yaml has a corresponding entry in subaccounts.yaml")

    # 3) Validate automation_mode + brief per-sub summary
    print()
    print("[subaccount_sanity] Per-subaccount summary:")
    print("  key | name | role | from | enabled | automation_mode | risk_pct | exit_profile")

    def _fmt_row(source: str, key: str, row: Dict[str, Any]) -> None:
        name = row.get("name", "")
        role = row.get("role", "")
        enabled = row.get("enabled")
        mode_raw = str(row.get("automation_mode", "")).upper().strip()
        if not mode_raw:
            mode_raw = "(missing)"
        risk_pct = row.get("risk_pct", row.get("risk_per_trade_pct"))
        exit_profile = row.get("exit_profile")
        if isinstance(exit_profile, dict):
            exit_profile = exit_profile.get("name", "(dict)")
        print(
            f"  {key} | {name} | {role} | {source} | {enabled} | {mode_raw} | {risk_pct} | {exit_profile}"
        )
        if mode_raw not in VALID_MODES and mode_raw != "(missing)":
            print(f"    -> INVALID automation_mode={mode_raw} (must be one of {sorted(VALID_MODES)})")

    for key in sorted(subs_ids | strats_ids):
        row_subs = subs_map_from_subs.get(key)
        row_strats = subs_map_from_strats.get(key)

        if row_subs:
            _fmt_row("subaccounts.yaml", key, row_subs)
        if row_strats:
            _fmt_row("strategies.yaml", key, row_strats)

    print()
    print("[subaccount_sanity] Completed sanity check.")


if __name__ == "__main__":
    main()
