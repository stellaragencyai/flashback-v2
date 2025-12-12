#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Multi-Sub Isolation & Exposure Config Test (v1)

Goal
----
Static checks that support multi-account isolation:

  - All subaccounts have unique UIDs (if 'uid' field exists).
  - All strategies reference real subaccounts.
  - Each subaccount binds to exactly one risk_profile.
  - Optional: warn if too many subs share the same high-risk profile.

This doesn't hit the exchange. It's a config-level sanity guard.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Set

import yaml  # type: ignore

ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = ROOT / "config"

SUB_YAML = CONFIG_DIR / "subaccounts.yaml"
STRAT_YAML = CONFIG_DIR / "strategies.yaml"
RISK_YAML = CONFIG_DIR / "risk_profiles.yaml"


def load_yaml(path: Path) -> Any:
    if not path.exists():
        print(f"[ERROR] Missing YAML: {path}")
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception as exc:
        print(f"[ERROR] Failed to load YAML {path}: {exc}")
        return None


def load_subaccounts() -> Dict[str, Dict[str, Any]]:
    data = load_yaml(SUB_YAML)
    if not isinstance(data, dict):
        print("[ERROR] subaccounts.yaml root must be a dict.")
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for k, v in data.items():
        if k in {"version", "notes"}:
            continue
        if isinstance(v, dict):
            out[str(k)] = v
    return out


def load_strategies() -> List[Dict[str, Any]]:
    data = load_yaml(STRAT_YAML)
    if not isinstance(data, dict):
        print("[ERROR] strategies.yaml root must be a dict.")
        return []
    subs = data.get("subaccounts")
    if not isinstance(subs, list):
        print("[ERROR] strategies.yaml must have 'subaccounts' list.")
        return []
    return [e for e in subs if isinstance(e, dict)]


def load_risk_profiles() -> Dict[str, Dict[str, Any]]:
    data = load_yaml(RISK_YAML)
    if not isinstance(data, dict):
        print("[ERROR] risk_profiles.yaml root must be a dict.")
        return {}
    profiles = data.get("profiles")
    if not isinstance(profiles, dict):
        print("[ERROR] risk_profiles.yaml must have 'profiles' dict.")
        return {}
    return {str(k): v for k, v in profiles.items() if isinstance(v, dict)}


def check_unique_uids(subs: Dict[str, Dict[str, Any]]) -> int:
    errors = 0
    uid_to_label: Dict[Any, str] = {}
    for label, cfg in subs.items():
        if "uid" not in cfg:
            print(f"[WARN] subaccount '{label}' has no 'uid' field.")
            continue
        uid = cfg["uid"]
        if uid in uid_to_label:
            other = uid_to_label[uid]
            print(f"[ERROR] subaccounts '{label}' and '{other}' share the same uid={uid}.")
            errors += 1
        else:
            uid_to_label[uid] = label
    return errors


def check_risk_profile_binding(
    subs: Dict[str, Dict[str, Any]],
    risk_profiles: Dict[str, Dict[str, Any]],
) -> int:
    errors = 0
    profile_usage: Dict[str, List[str]] = {}

    for label, cfg in subs.items():
        rp = cfg.get("risk_profile")
        if rp is None:
            print(f"[ERROR] subaccount '{label}' missing risk_profile.")
            errors += 1
            continue
        if rp not in risk_profiles:
            print(f"[ERROR] subaccount '{label}' uses unknown risk_profile '{rp}'.")
            errors += 1
            continue
        profile_usage.setdefault(rp, []).append(label)

    print("")
    print("Risk profile usage:")
    print("-------------------")
    for rp_name, labels in sorted(profile_usage.items(), key=lambda kv: kv[0]):
        labels_str = ", ".join(labels)
        print(f"  {rp_name:16s} -> {labels_str}")

    return errors


def check_strategy_account_mapping(
    subs: Dict[str, Dict[str, Any]],
    strategies: List[Dict[str, Any]],
) -> int:
    errors = 0
    sub_labels: Set[str] = set(subs.keys())
    for idx, strat in enumerate(strategies):
        ctx = f"strategies.subaccounts[{idx}]"
        acct = strat.get("account_label")
        name = strat.get("strategy_name", "<no-name>")
        if not acct:
            print(f"[ERROR] {ctx} ({name}) missing account_label.")
            errors += 1
            continue
        if acct not in sub_labels:
            print(f"[ERROR] {ctx} ({name}) references unknown subaccount '{acct}'.")
            errors += 1

    return errors


def main() -> int:
    print("=== Flashback Multi-Sub Isolation & Exposure Config Test ===")
    print(f"ROOT:        {ROOT}")
    print(f"CONFIG_DIR:  {CONFIG_DIR}")
    print("")

    subs = load_subaccounts()
    strategies = load_strategies()
    risk_profiles = load_risk_profiles()

    if not subs:
        print("[FATAL] No subaccounts loaded.")
        return 1
    if not strategies:
        print("[WARN] No strategies loaded.")

    errors = 0
    errors += check_unique_uids(subs)
    errors += check_risk_profile_binding(subs, risk_profiles)
    errors += check_strategy_account_mapping(subs, strategies)

    print("")
    if errors == 0:
        print("[OK] Multi-sub isolation/exposure config test passed ✅")
        return 0

    print(f"[FAIL] Found {errors} issue(s) in multi-sub configuration ❌")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
