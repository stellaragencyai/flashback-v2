#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Config Validator (canonical, hard-fail)

Validates:
- config/strategies.yaml (v6 schema used by Flashback)
- config/exit_profiles.yaml (v3 schema)

Exit codes:
- 0 PASS ✅
- 2 FAIL ❌
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ROOT
try:
    from app.core.config import settings  # type: ignore
    ROOT: Path = Path(settings.ROOT)  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

STRATEGIES_PATH = ROOT / "config" / "strategies.yaml"
EXIT_PROFILES_PATH = ROOT / "config" / "exit_profiles.yaml"

ALLOWED_AUTOMATION = {"OFF", "LEARN_DRY", "LIVE_CANARY", "LIVE_FULL"}


def _load_yaml(path: Path) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not path.exists():
        return None, f"missing file: {path}"
    try:
        import yaml  # type: ignore
    except Exception:
        return None, "PyYAML not installed (pip install pyyaml)"

    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        if not isinstance(data, dict):
            return None, f"root must be a dict/object in {path.name}"
        return data, None
    except Exception as e:
        return None, f"parse error in {path.name}: {e}"


def _is_str(x: Any) -> bool:
    return isinstance(x, str) and x.strip() != ""


def _num(x: Any) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None


def _err(errs: List[str], msg: str) -> None:
    errs.append(msg)


def _validate_exit_profiles(exit_cfg: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    errs: List[str] = []
    profile_names: List[str] = []

    if "profiles" not in exit_cfg or not isinstance(exit_cfg.get("profiles"), dict):
        _err(errs, "exit_profiles.yaml: missing top-level 'profiles:' dict")
        return errs, profile_names

    profiles = exit_cfg["profiles"]
    for name, blk in profiles.items():
        if not _is_str(name):
            _err(errs, "exit_profiles.yaml: profile name must be non-empty string")
            continue
        if not isinstance(blk, dict):
            _err(errs, f"exit_profiles.yaml: profiles.{name} must be a dict")
            continue
        # required keys
        if "tps" not in blk or "sl" not in blk:
            _err(errs, f"exit_profiles.yaml: profiles.{name} must have tps + sl")
            continue
        tps = blk.get("tps")
        sl = blk.get("sl")
        if not isinstance(tps, list) or len(tps) == 0:
            _err(errs, f"exit_profiles.yaml: profiles.{name}.tps must be a non-empty list")
            continue
        if not isinstance(sl, dict):
            _err(errs, f"exit_profiles.yaml: profiles.{name}.sl must be a dict")
            continue
        if "rr" not in sl or _num(sl.get("rr")) is None:
            _err(errs, f"exit_profiles.yaml: profiles.{name}.sl.rr must be numeric (negative for SL)")
            continue

        # Validate TP rows sum to ~1.0
        size_sum = 0.0
        for i, tp in enumerate(tps):
            if not isinstance(tp, dict):
                _err(errs, f"exit_profiles.yaml: profiles.{name}.tps[{i}] must be a dict")
                continue
            rr = _num(tp.get("rr"))
            sp = _num(tp.get("size_pct"))
            if rr is None:
                _err(errs, f"exit_profiles.yaml: profiles.{name}.tps[{i}].rr must be numeric")
            if sp is None or sp <= 0 or sp > 1:
                _err(errs, f"exit_profiles.yaml: profiles.{name}.tps[{i}].size_pct must be in (0,1]")
            if sp is not None:
                size_sum += sp

        # allow slight float slop
        if abs(size_sum - 1.0) > 0.02:
            _err(errs, f"exit_profiles.yaml: profiles.{name} TP size_pct sums to {size_sum:.4f} (expected ~1.0)")

        profile_names.append(name)

    return errs, profile_names


def _validate_strategies(strat_cfg: Dict[str, Any], exit_profiles: List[str]) -> List[str]:
    errs: List[str] = []

    if "subaccounts" not in strat_cfg or not isinstance(strat_cfg.get("subaccounts"), list):
        _err(errs, "strategies.yaml: missing top-level 'subaccounts:' list")
        return errs

    seen_labels = set()
    seen_uids = set()

    subs: List[dict] = strat_cfg["subaccounts"]
    for idx, s in enumerate(subs):
        ctx = f"strategies.yaml: subaccounts[{idx}]"
        if not isinstance(s, dict):
            _err(errs, f"{ctx}: must be a dict")
            continue

        # Required fields
        for k in ("account_label", "strategy_name", "enabled", "symbols", "timeframes", "risk_pct", "automation_mode", "exit_profile"):
            if k not in s:
                _err(errs, f"{ctx}: missing '{k}'")

        label = s.get("account_label")
        if not _is_str(label):
            _err(errs, f"{ctx}.account_label must be non-empty string")
        else:
            if label in seen_labels:
                _err(errs, f"{ctx}.account_label duplicate: '{label}'")
            seen_labels.add(label)

        # sub_uid: allow null ONLY for main
        sub_uid = s.get("sub_uid")
        if label == "main":
            # ok if null
            pass
        else:
            if sub_uid is None:
                _err(errs, f"{ctx}.sub_uid cannot be null for non-main label='{label}'")
            else:
                try:
                    uid_int = int(sub_uid)
                    if uid_int in seen_uids:
                        _err(errs, f"{ctx}.sub_uid duplicate: {uid_int}")
                    seen_uids.add(uid_int)
                except Exception:
                    _err(errs, f"{ctx}.sub_uid must be an int for non-main label='{label}'")

        # automation_mode
        am = str(s.get("automation_mode") or "").strip()
        if am not in ALLOWED_AUTOMATION:
            _err(errs, f"{ctx}.automation_mode invalid '{am}' (allowed: {sorted(ALLOWED_AUTOMATION)})")

        # risk_pct
        rp = _num(s.get("risk_pct"))
        if rp is None:
            _err(errs, f"{ctx}.risk_pct must be numeric")
        else:
            if rp <= 0:
                _err(errs, f"{ctx}.risk_pct must be > 0 (got {rp})")
            if rp > 5:
                _err(errs, f"{ctx}.risk_pct must be <= 5 (got {rp})")

        # symbols/timeframes sanity
        symbols = s.get("symbols")
        if not isinstance(symbols, list) or len(symbols) == 0:
            _err(errs, f"{ctx}.symbols must be a non-empty list")
        timeframes = s.get("timeframes")
        if not isinstance(timeframes, list) or len(timeframes) == 0:
            _err(errs, f"{ctx}.timeframes must be a non-empty list")

        # exit_profile must exist
        ep = s.get("exit_profile")
        if not _is_str(ep):
            _err(errs, f"{ctx}.exit_profile must be non-empty string")
        else:
            if ep not in exit_profiles:
                _err(errs, f"{ctx}.exit_profile '{ep}' not found in exit_profiles.yaml profiles")

        # promotion_rules (only enforce if enabled)
        pr = s.get("promotion_rules") or {}
        if isinstance(pr, dict) and pr.get("enabled") is True:
            for k in ("min_trades", "min_winrate", "min_avg_r", "min_expectancy_r", "max_drawdown_pct"):
                if k not in pr:
                    _err(errs, f"{ctx}.promotion_rules enabled but missing '{k}'")
            # type checks
            if _num(pr.get("min_trades")) is None:
                _err(errs, f"{ctx}.promotion_rules.min_trades must be numeric/int")
            for k in ("min_winrate", "min_avg_r", "min_expectancy_r", "max_drawdown_pct"):
                if _num(pr.get(k)) is None:
                    _err(errs, f"{ctx}.promotion_rules.{k} must be numeric")

        elif pr not in ({}, None) and not isinstance(pr, dict):
            _err(errs, f"{ctx}.promotion_rules must be dict if present")

        # enabled + OFF consistency
        enabled = s.get("enabled")
        if isinstance(enabled, bool) and enabled is False and am != "OFF":
            _err(errs, f"{ctx}: enabled=false but automation_mode='{am}' (expected OFF)")

    return errs


def main() -> int:
    print("\n=== CONFIG VALIDATOR ===")
    print(f"ROOT: {ROOT}")

    strat_cfg, e1 = _load_yaml(STRATEGIES_PATH)
    exit_cfg, e2 = _load_yaml(EXIT_PROFILES_PATH)

    if e1:
        print(f"\nFAIL ❌ {e1}")
        return 2
    if e2:
        print(f"\nFAIL ❌ {e2}")
        return 2

    exit_errs, exit_names = _validate_exit_profiles(exit_cfg or {})
    strat_errs = _validate_strategies(strat_cfg or {}, exit_names)

    errs = exit_errs + strat_errs
    if errs:
        print(f"\nFAIL ❌ ({len(errs)} issues)")
        for e in errs[:120]:
            print(f" - {e}")
        if len(errs) > 120:
            print(f" - ...and {len(errs) - 120} more")
        return 2

    print("\nPASS ✅")
    print(f"Exit profiles: {len(exit_names)}")
    print("Strategies: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
