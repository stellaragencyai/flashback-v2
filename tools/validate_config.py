#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Config Validator (v4, schema-hardened)

What this does:
  - Loads core config files:
        * config/subaccounts.yaml
        * config/strategies.yaml
        * config/exit_profiles.yaml
        * config/risk_profiles.yaml
        * config/bots.yaml
  - Validates:
        * Referential integrity between subaccounts & strategies
        * exit_profile names
        * risk_profile names
        * automation_mode values (MISSING = ERROR)
        * basic sanity of risk and promotion_rules
        * exit profile TP sizing (sum ~= 1.0, RRs sorted)

Exit codes:
  - 0 : all good
  - 1 : validation errors found
"""

from __future__ import annotations

import sys
import math
from pathlib import Path
from typing import Any, Dict, List, Set

import yaml  # type: ignore


ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = ROOT / "config"

SUBACCOUNTS_YAML = CONFIG_DIR / "subaccounts.yaml"
STRATEGIES_YAML = CONFIG_DIR / "strategies.yaml"
EXIT_PROFILES_YAML = CONFIG_DIR / "exit_profiles.yaml"
RISK_PROFILES_YAML = CONFIG_DIR / "risk_profiles.yaml"
BOTS_YAML = CONFIG_DIR / "bots.yaml"

ALLOWED_AUTOMATION_MODES = {
    "OFF",
    "LEARN_DRY",
    "LEARN_PAPER",
    "LIVE_CANARY",
    "LIVE_FULL",
}

EPSILON = 1e-6


def log(msg: str) -> None:
    print(msg)


def load_yaml(path: Path) -> Any:
    if not path.exists():
        log(f"[ERROR] Missing YAML file: {path}")
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception as exc:
        log(f"[ERROR] Failed to parse YAML {path}: {exc}")
        return None


def load_subaccounts() -> Dict[str, Dict[str, Any]]:
    """
    Accepts either:
      A) Flat:
         main: { ... }
         flashback01: { ... }

      B) Nested mapping:
         accounts:
           main: { ... }
           flashback01: { ... }

    NOTE: If you also keep a legacy accounts: LIST, that is ignored here by design.
    """
    data = load_yaml(SUBACCOUNTS_YAML)
    if not isinstance(data, dict):
        log("[ERROR] subaccounts.yaml root must be a dict.")
        return {}

    maybe_accounts = data.get("accounts")
    if isinstance(maybe_accounts, dict):
        accounts_node = maybe_accounts
    else:
        accounts_node = data

    sub_map: Dict[str, Dict[str, Any]] = {}
    for label, entry in accounts_node.items():
        if label in {"version", "notes"}:
            continue
        if not isinstance(entry, dict):
            log(f"[ERROR] subaccounts[{label}] is not a dict.")
            continue
        sub_map[str(label)] = entry
    return sub_map


def load_risk_profiles() -> Dict[str, Dict[str, Any]]:
    data = load_yaml(RISK_PROFILES_YAML)
    if not isinstance(data, dict):
        log("[ERROR] risk_profiles.yaml root must be a dict.")
        return {}
    profiles = data.get("profiles")
    if not isinstance(profiles, dict):
        log("[ERROR] risk_profiles.yaml must have 'profiles' dict.")
        return {}
    return {str(k): v for k, v in profiles.items() if isinstance(v, dict)}


def load_exit_profiles() -> Dict[str, Dict[str, Any]]:
    """
    Returns all exit profiles by name.
    Canonical is:
      profiles: {...}
    """
    data = load_yaml(EXIT_PROFILES_YAML)
    if not isinstance(data, dict):
        log("[ERROR] exit_profiles.yaml root must be a dict.")
        return {}

    profiles: Dict[str, Dict[str, Any]] = {}

    main_profiles = data.get("profiles")
    if isinstance(main_profiles, dict):
        for k, v in main_profiles.items():
            if isinstance(v, dict):
                profiles[str(k)] = v

    # Allow legacy top-level dict profiles too (but don't require them)
    for k, v in data.items():
        if k == "profiles":
            continue
        if isinstance(v, dict) and ("tps" in v or "sl" in v):
            profiles.setdefault(str(k), v)

    return profiles


def load_strategies() -> List[Dict[str, Any]]:
    data = load_yaml(STRATEGIES_YAML)
    if not isinstance(data, dict):
        log("[ERROR] strategies.yaml root must be a dict.")
        return []

    sub_node = data.get("subaccounts")
    if not isinstance(sub_node, list):
        log("[ERROR] strategies.yaml must contain 'subaccounts:' as a list.")
        return []

    strategies: List[Dict[str, Any]] = []
    for entry in sub_node:
        if not isinstance(entry, dict):
            log("[ERROR] strategies.yaml subaccounts entry is not a dict.")
            continue
        strategies.append(entry)
    return strategies


def load_bots() -> List[Dict[str, Any]]:
    data = load_yaml(BOTS_YAML)
    if not isinstance(data, dict):
        log("[ERROR] bots.yaml root must be a dict.")
        return []
    bots = data.get("bots")
    if not isinstance(bots, list):
        log("[ERROR] bots.yaml must contain 'bots:' list.")
        return []
    result: List[Dict[str, Any]] = []
    for b in bots:
        if isinstance(b, dict):
            result.append(b)
    return result


def check_subaccounts_vs_risk_profiles(
    sub_map: Dict[str, Dict[str, Any]],
    risk_profiles: Dict[str, Dict[str, Any]],
) -> int:
    errors = 0
    for label, entry in sub_map.items():
        rp = entry.get("risk_profile")
        if rp is None:
            log(f"[WARN] subaccount '{label}' has no risk_profile set.")
            continue
        if rp not in risk_profiles:
            log(f"[ERROR] subaccount '{label}' references unknown risk_profile '{rp}'.")
            errors += 1
    return errors


def check_exit_profiles_sanity(exit_profiles: Dict[str, Dict[str, Any]]) -> int:
    errors = 0
    for name, cfg in exit_profiles.items():
        tps = cfg.get("tps")
        sl = cfg.get("sl")
        if tps is None or sl is None:
            log(f"[ERROR] exit_profile '{name}' missing 'tps' or 'sl' section.")
            errors += 1
            continue

        if isinstance(tps, list):
            total = 0.0
            prev_rr = -math.inf
            for tp in tps:
                if not isinstance(tp, dict):
                    log(f"[ERROR] exit_profile '{name}' has non-dict TP entry {tp}.")
                    errors += 1
                    continue
                rr = tp.get("rr")
                sz = tp.get("size_pct")
                try:
                    rr_f = float(rr)
                    sz_f = float(sz)
                except Exception:
                    log(f"[ERROR] exit_profile '{name}' TP rr/size_pct not numeric: {tp}.")
                    errors += 1
                    continue

                if rr_f < prev_rr:
                    log(f"[ERROR] exit_profile '{name}' TP rr values not sorted ascending.")
                    errors += 1
                prev_rr = rr_f
                total += sz_f

            if not (0.99 <= total <= 1.01):
                log(f"[ERROR] exit_profile '{name}' TP size_pct sum {total:.4f} != 1.0 (±0.01).")
                errors += 1
        else:
            log(f"[ERROR] exit_profile '{name}' tps section must be a list.")
            errors += 1

        if not isinstance(sl, dict):
            log(f"[ERROR] exit_profile '{name}' sl section must be a dict.")
            errors += 1
        else:
            rr = sl.get("rr")
            try:
                float(rr)
            except Exception:
                log(f"[ERROR] exit_profile '{name}' sl.rr must be numeric.")
                errors += 1

    return errors


def check_strategies(
    strategies: List[Dict[str, Any]],
    sub_map: Dict[str, Dict[str, Any]],
    exit_profiles: Dict[str, Dict[str, Any]],
) -> int:
    errors = 0
    seen_names: Set[str] = set()
    exit_names: Set[str] = set(exit_profiles.keys())

    for idx, strat in enumerate(strategies):
        ctx = f"strategies.subaccounts[{idx}]"

        account_label = strat.get("account_label")
        strategy_name = strat.get("strategy_name") or strat.get("name")
        exit_profile = strat.get("exit_profile") or strat.get("exitProfile")
        automation_mode = strat.get("automation_mode")
        risk_pct = strat.get("risk_pct", strat.get("risk_per_trade_pct"))
        promo = strat.get("promotion_rules")

        if not account_label:
            log(f"[ERROR] {ctx} missing 'account_label'.")
            errors += 1
        else:
            if account_label not in sub_map:
                log(f"[ERROR] {ctx} references unknown subaccount '{account_label}'.")
                errors += 1

        if not strategy_name:
            log(f"[ERROR] {ctx} missing 'strategy_name' (or 'name').")
            errors += 1
        else:
            if strategy_name in seen_names:
                log(f"[ERROR] Duplicate strategy name '{strategy_name}' in strategies.yaml.")
                errors += 1
            seen_names.add(strategy_name)

        if exit_profile:
            if exit_profile not in exit_names:
                log(f"[ERROR] {ctx} uses unknown exit_profile '{exit_profile}'.")
                errors += 1
        else:
            log(f"[ERROR] {ctx} missing exit_profile.")  # hardened
            errors += 1

        # HARDENED: missing automation_mode is now an ERROR
        if automation_mode:
            automation_mode = str(automation_mode).upper()
            if automation_mode not in ALLOWED_AUTOMATION_MODES:
                log(f"[ERROR] {ctx} has invalid automation_mode '{automation_mode}'.")
                errors += 1
        else:
            log(f"[ERROR] {ctx} missing automation_mode.")
            errors += 1

        if risk_pct is not None:
            try:
                r = float(risk_pct)
                if not (0.0 < r <= 5.0):
                    log(f"[ERROR] {ctx} risk_pct={r} must be in (0, 5].")
                    errors += 1
            except Exception:
                log(f"[ERROR] {ctx} risk_pct '{risk_pct}' is not numeric.")
                errors += 1
        else:
            log(f"[ERROR] {ctx} has no risk_pct / risk_per_trade_pct set.")  # hardened
            errors += 1

        # Promotion rules sanity (optional)
        if promo is not None:
            if not isinstance(promo, dict):
                log(f"[ERROR] {ctx}.promotion_rules must be a dict.")
                errors += 1
            else:
                enabled = promo.get("enabled", False)
                if enabled:
                    min_trades = promo.get("min_trades")
                    min_winrate = promo.get("min_winrate")
                    min_avg_r = promo.get("min_avg_r")
                    min_exp_r = promo.get("min_expectancy_r")
                    max_dd = promo.get("max_drawdown_pct")

                    if not isinstance(min_trades, int) or min_trades <= 0:
                        log(f"[ERROR] {ctx}.promotion_rules.min_trades must be positive int.")
                        errors += 1
                    if not (isinstance(min_winrate, (int, float)) and 0.0 <= float(min_winrate) <= 1.0):
                        log(f"[ERROR] {ctx}.promotion_rules.min_winrate must be between 0.0 and 1.0.")
                        errors += 1
                    for field, val in [
                        ("min_avg_r", min_avg_r),
                        ("min_expectancy_r", min_exp_r),
                        ("max_drawdown_pct", max_dd),
                    ]:
                        if not isinstance(val, (int, float)):
                            log(f"[ERROR] {ctx}.promotion_rules.{field} must be numeric.")
                            errors += 1
        else:
            log(f"[WARN] {ctx} has no promotion_rules defined.")

    return errors


def check_bots(bots: List[Dict[str, Any]]) -> int:
    errors = 0
    for idx, bot in enumerate(bots):
        name = bot.get("name")
        module = bot.get("module")
        enabled = bot.get("enabled")
        ctx = f"bots[{idx}]"

        if not name:
            log(f"[ERROR] {ctx} missing 'name'.")
            errors += 1
        if not module:
            log(f"[ERROR] {ctx} missing 'module'.")
            errors += 1
        if not isinstance(enabled, bool):
            log(f"[ERROR] {ctx} 'enabled' must be a bool.")
            errors += 1

    return errors


def main() -> int:
    log("=== Flashback Config Validator ===")
    log(f"ROOT: {ROOT}")
    log("")

    sub_map = load_subaccounts()
    risk_profiles = load_risk_profiles()
    exit_profiles = load_exit_profiles()
    strategies = load_strategies()
    bots = load_bots()

    if not sub_map:
        log("[ERROR] No valid subaccounts loaded.")
    if not strategies:
        log("[ERROR] No valid strategies loaded.")

    errors = 0
    errors += check_subaccounts_vs_risk_profiles(sub_map, risk_profiles)
    errors += check_exit_profiles_sanity(exit_profiles)
    errors += check_strategies(strategies, sub_map, exit_profiles)
    errors += check_bots(bots)

    if errors == 0:
        log("")
        log("[OK] All config checks passed ✅")
        return 0

    log("")
    log(f"[FAIL] Config validation finished with {errors} error(s). ❌")
    return 1


if __name__ == "__main__":
    sys.exit(main())
