#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Config Validator (DEBUG VERSION)

This is a v5-aware validator that:
  - Prints raw info about strategies.yaml layout
  - Assumes a top-level 'subaccounts:' list in config/strategies.yaml
  - Validates basic fields on each entry

If you still see the OLD message:
  '[WARN] Could not infer strategies layout from strategies.yaml...'
then you are NOT running this file.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, Any, Set, List

import yaml  # requires pyyaml


ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = ROOT / "config"

SUBACCOUNTS_YAML = CONFIG_DIR / "subaccounts.yaml"
SUBACCOUNTS_JSON = CONFIG_DIR / "subaccounts.json"
EXIT_PROFILES_YAML = CONFIG_DIR / "exit_profiles.yaml"
STRATEGIES_YAML = CONFIG_DIR / "strategies.yaml"

ALLOWED_AUTOMATION_MODES = {
    "OFF",
    "LEARN_DRY",
    "LEARN_PAPER",
    "LIVE_CANARY",
    "LIVE_FULL",
}


def log(msg: str) -> None:
    print(msg)


def load_yaml(path: Path) -> Any:
    if not path.exists():
        log(f"[DEBUG] YAML file not found: {path}")
        return None
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_subaccounts() -> Set[str]:
    labels: Set[str] = set()

    data_yaml = load_yaml(SUBACCOUNTS_YAML)
    if isinstance(data_yaml, dict):
        labels.update(str(k) for k in data_yaml.keys())

    if SUBACCOUNTS_JSON.exists():
        try:
            import json
            with SUBACCOUNTS_JSON.open("r", encoding="utf-8") as f:
                data_json = json.load(f)
            if isinstance(data_json, dict):
                labels.update(str(k) for k in data_json.keys())
            elif isinstance(data_json, list):
                for item in data_json:
                    if isinstance(item, dict):
                        label = item.get("label") or item.get("account_label")
                        if label:
                            labels.add(str(label))
        except Exception as exc:
            log(f"[WARN] Failed to read subaccounts.json: {exc}")

    return labels


def load_exit_profiles() -> Set[str]:
    profiles: Set[str] = set()
    data = load_yaml(EXIT_PROFILES_YAML)
    if isinstance(data, dict):
        for name in data.keys():
            profiles.add(str(name))
    return profiles


def load_strategies_v5() -> Dict[str, Dict[str, Any]]:
    """
    Hard-coded reader for strategies.yaml v5 layout with 'subaccounts:' list.
    """
    log(f"[DEBUG] Reading strategies from: {STRATEGIES_YAML}")
    data = load_yaml(STRATEGIES_YAML)

    log(f"[DEBUG] type(strategies.yaml root) = {type(data)}")
    if isinstance(data, dict):
        log(f"[DEBUG] root keys: {list(data.keys())}")
    else:
        log("[DEBUG] strategies.yaml root is not a dict, cannot proceed.")
        return {}

    sub_node = data.get("subaccounts")
    log(f"[DEBUG] type(data['subaccounts']) = {type(sub_node)}")

    if not isinstance(sub_node, list):
        log("[ERROR] strategies.yaml does not contain 'subaccounts:' list at root.")
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    for idx, item in enumerate(sub_node):
        if not isinstance(item, dict):
            log(f"[DEBUG] Skipping non-dict entry at subaccounts[{idx}]: {type(item)}")
            continue
        name = (
            item.get("name")
            or item.get("strategy")
            or item.get("id")
            or item.get("account_label")
            or f"sub_{idx}"
        )
        out[str(name)] = item

    log(f"[DEBUG] Parsed {len(out)} strategy entries from strategies.yaml/subaccounts.")
    return out


def validate() -> int:
    errors: List[str] = []

    log(f"[INFO] Project root: {ROOT}")
    log("[INFO] Using DEBUG v5 validator for strategies.yaml")
    log("[INFO] Loading subaccounts, exit profiles, and strategies...")

    subaccount_labels = load_subaccounts()
    exit_profiles = load_exit_profiles()
    strategies = load_strategies_v5()

    if not strategies:
        errors.append("No strategies found in strategies.yaml 'subaccounts:' list.")

    log(f"[INFO] Found {len(subaccount_labels)} infra subaccounts (subaccounts.yaml/json).")
    log(f"[INFO] Found {len(exit_profiles)} exit profiles.")
    log(f"[INFO] Found {len(strategies)} strategy entries (from strategies.yaml subaccounts).")

    for strat_name, strat in strategies.items():
        if not isinstance(strat, dict):
            errors.append(f"[{strat_name}] Strategy config is not a mapping.")
            continue

        account_label = (
            strat.get("account_label")
            or strat.get("account")
            or strat.get("label")
        )
        exit_profile = strat.get("exit_profile")
        automation_mode = strat.get("automation_mode")
        risk_pct = strat.get("risk_pct") or strat.get("risk_per_trade_pct")

        if not account_label:
            errors.append(f"[{strat_name}] Missing account_label.")
        else:
            if subaccount_labels and account_label not in subaccount_labels:
                errors.append(
                    f"[{strat_name}] account_label '{account_label}' not found in subaccounts.yaml/json."
                )

        if isinstance(exit_profile, str):
            if exit_profile not in exit_profiles:
                errors.append(
                    f"[{strat_name}] exit_profile '{exit_profile}' not found in exit_profiles.yaml."
                )
        elif isinstance(exit_profile, dict):
            pass
        elif exit_profile is None:
            errors.append(f"[{strat_name}] Missing exit_profile.")
        else:
            errors.append(
                f"[{strat_name}] exit_profile has unexpected type: {type(exit_profile)}"
            )

        if automation_mode is None:
            errors.append(f"[{strat_name}] Missing automation_mode.")
        elif automation_mode not in ALLOWED_AUTOMATION_MODES:
            errors.append(
                f"[{strat_name}] automation_mode '{automation_mode}' is not in allowed set "
                f"{sorted(ALLOWED_AUTOMATION_MODES)}."
            )

        if risk_pct is None:
            errors.append(f"[{strat_name}] Missing risk_pct/risk_per_trade_pct.")

    if errors:
        log("\n[VALIDATION FAILED]")
        for e in errors:
            log(f" - {e}")
        log(f"\nTotal errors: {len(errors)}")
        return 1

    log("[VALIDATION OK] All strategies, subaccounts, and exit profiles look consistent.")
    return 0


def main() -> int:
    return validate()


if __name__ == "__main__":
    sys.exit(main())
