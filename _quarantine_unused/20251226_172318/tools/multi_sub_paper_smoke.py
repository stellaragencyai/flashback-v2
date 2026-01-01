#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Multi-Sub Paper Smoke Test (v1)

Goal
----
Quick, structural sanity check for all accounts:

  - Every subaccount in subaccounts.yaml has a manifest entry.
  - Every non-MANUAL account has at least one strategy.
  - For each account:
      * If a paper state file exists (state/paper/<label>.json), it's valid JSON.
  - Summarized OK/FAIL per account.

This does NOT simulate live signals. It's a cheap "is the wiring at least coherent?" test.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml  # type: ignore


ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = ROOT / "config"
STATE_DIR = ROOT / "state"
PAPER_DIR = STATE_DIR / "paper"

SUB_YAML = CONFIG_DIR / "subaccounts.yaml"
STRAT_YAML = CONFIG_DIR / "strategies.yaml"
STACK_MANIFEST_YAML = CONFIG_DIR / "sub_stack_manifest.yaml"


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


def load_strategies_by_account() -> Dict[str, List[Dict[str, Any]]]:
    data = load_yaml(STRAT_YAML)
    strategies_by_acct: Dict[str, List[Dict[str, Any]]] = {}
    if not isinstance(data, dict):
        print("[WARN] strategies.yaml root not a dict, skipping strategy checks.")
        return strategies_by_acct

    subs = data.get("subaccounts")
    if not isinstance(subs, list):
        print("[WARN] strategies.yaml has no 'subaccounts' list.")
        return strategies_by_acct

    for entry in subs:
        if not isinstance(entry, dict):
            continue
        acct = entry.get("account_label")
        if not acct:
            continue
        strategies_by_acct.setdefault(str(acct), []).append(entry)

    return strategies_by_acct


def load_stack_manifest() -> Dict[str, Dict[str, Any]]:
    data = load_yaml(STACK_MANIFEST_YAML)
    if not isinstance(data, dict):
        print("[ERROR] sub_stack_manifest.yaml root must be a dict.")
        return {}
    accounts = data.get("accounts")
    if not isinstance(accounts, dict):
        print("[ERROR] sub_stack_manifest.yaml must have 'accounts' dict.")
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for k, v in accounts.items():
        if isinstance(v, dict):
            out[str(k)] = v
    return out


def check_paper_file(label: str) -> Tuple[bool, str]:
    path = PAPER_DIR / f"{label}.json"
    if not path.exists():
        return False, "paper file missing"
    try:
        raw = path.read_text(encoding="utf-8")
        if not raw.strip():
            return False, "paper file empty"
        json.loads(raw)
        return True, "paper file OK"
    except Exception as exc:
        return False, f"paper file invalid JSON: {exc}"


def main() -> int:
    print("=== Flashback Multi-Sub Paper Smoke Test ===")
    print(f"ROOT:         {ROOT}")
    print(f"CONFIG_DIR:   {CONFIG_DIR}")
    print(f"STATE_DIR:    {STATE_DIR}")
    print(f"PAPER_DIR:    {PAPER_DIR}")
    print("")

    subs = load_subaccounts()
    manifest = load_stack_manifest()
    strats_by_acct = load_strategies_by_account()

    if not subs:
        print("[FATAL] No subaccounts loaded, aborting.")
        return 1

    missing_manifest = []
    results: List[Dict[str, Any]] = []

    for label, cfg in subs.items():
        m = manifest.get(label)
        if m is None:
            missing_manifest.append(label)
            mode = "UNKNOWN"
        else:
            mode = str(m.get("mode", "UNKNOWN"))

        strategies = strats_by_acct.get(label, [])
        has_strategy = len(strategies) > 0

        # For MANUAL accounts we don't require strategies
        requires_strategy = mode not in {"MANUAL", "OFF"}

        paper_ok = False
        paper_msg = "n/a"
        if PAPER_DIR.exists():
            paper_ok, paper_msg = check_paper_file(label)

        results.append(
            {
                "label": label,
                "mode": mode,
                "requires_strategy": requires_strategy,
                "strategy_count": len(strategies),
                "has_strategy": has_strategy,
                "paper_ok": paper_ok,
                "paper_msg": paper_msg,
            }
        )

    print("Subaccount summary:")
    print("-------------------")
    for r in sorted(results, key=lambda x: x["label"]):
        label = r["label"]
        mode = r["mode"]
        sc = r["strategy_count"]
        requires_strategy = r["requires_strategy"]
        has_strategy = r["has_strategy"]
        paper_ok = r["paper_ok"]
        paper_msg = r["paper_msg"]

        strat_status = "OK"
        if requires_strategy and not has_strategy:
            strat_status = "MISSING"

        paper_icon = "✅" if paper_ok else "⚠️"

        print(
            f"{label:12s} mode={mode:11s} "
            f"strategies={sc:2d} ({strat_status})  "
            f"paper={paper_icon} ({paper_msg})"
        )

    print("")

    errors = 0

    if missing_manifest:
        print("[ERROR] The following subaccounts have NO manifest entry:")
        for lbl in missing_manifest:
            print(f"  - {lbl}")
        errors += len(missing_manifest)

    for r in results:
        if r["requires_strategy"] and not r["has_strategy"]:
            print(f"[ERROR] {r['label']} requires strategies but has none.")
            errors += 1

    print("")
    if errors == 0:
        print("[OK] Multi-sub paper smoke test passed ✅")
        return 0

    print(f"[FAIL] Multi-sub paper smoke test found {errors} issue(s) ❌")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
