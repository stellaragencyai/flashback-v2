#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Strategy Table Dumper

Produces a readable table of strategies with:
    - strategy name
    - account_label
    - role
    - symbols
    - timeframes
    - risk_pct
    - exit_profile
    - automation_mode
    - ai_profile
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import yaml  # requires pyyaml


ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = ROOT / "config"
STRATEGIES_YAML = CONFIG_DIR / "strategies.yaml"


def log(msg: str) -> None:
    print(msg)


def load_strategies() -> Dict[str, Dict[str, Any]]:
    if not STRATEGIES_YAML.exists():
        log(f"[ERROR] strategies.yaml not found at {STRATEGIES_YAML}")
        return {}

    with STRATEGIES_YAML.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if isinstance(data, dict):
        if "strategies" in data and isinstance(data["strategies"], dict):
            return data["strategies"]
        # Fallback: treat root as strategies mapping if it "looks like" one
        maybe_all_dict = all(isinstance(v, dict) for v in data.values())
        if maybe_all_dict and "version" not in data and "notes" not in data:
            return data

    log("[WARN] Could not infer strategies layout; expected 'strategies:' mapping or pure mapping.")
    return {}


def stringify_list(value: Any) -> str:
    if isinstance(value, (list, tuple)):
        return ",".join(str(v) for v in value)
    if value is None:
        return ""
    return str(value)


def build_table_rows(strategies: Dict[str, Dict[str, Any]]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []

    for name, cfg in strategies.items():
        if not isinstance(cfg, dict):
            continue

        account_label = (
            cfg.get("account_label")
            or cfg.get("account")
            or cfg.get("label")
            or ""
        )
        role = cfg.get("role") or cfg.get("strategy_role") or ""
        symbols = (
            cfg.get("symbols")
            or cfg.get("symbol_universe")
            or cfg.get("pairs")
        )
        timeframes = (
            cfg.get("timeframes")
            or cfg.get("time_frames")
            or cfg.get("tfs")
        )
        risk_pct = cfg.get("risk_pct") or cfg.get("risk_per_trade_pct")
        exit_profile = cfg.get("exit_profile")
        automation_mode = cfg.get("automation_mode") or ""
        ai_profile = cfg.get("ai_profile") or cfg.get("ai_policy") or ""

        row = {
            "strategy": str(name),
            "account": str(account_label),
            "role": str(role),
            "symbols": stringify_list(symbols),
            "timeframes": stringify_list(timeframes),
            "risk_pct": "" if risk_pct is None else str(risk_pct),
            "exit_profile": (
                exit_profile if isinstance(exit_profile, str) else "[inline]"
                if isinstance(exit_profile, dict)
                else ""
            ),
            "automation": str(automation_mode),
            "ai_profile": str(ai_profile),
        }
        rows.append(row)

    return rows


def print_table(rows: List[Dict[str, str]]) -> None:
    if not rows:
        print("[INFO] No strategies to display.")
        return

    headers = [
        "strategy",
        "account",
        "role",
        "symbols",
        "timeframes",
        "risk_pct",
        "exit_profile",
        "automation",
        "ai_profile",
    ]

    # Compute column widths
    col_widths = {h: len(h) for h in headers}
    for row in rows:
        for h in headers:
            col_widths[h] = max(col_widths[h], len(row.get(h, "")))

    # Build format string
    sep = " | "
    header_line = sep.join(h.ljust(col_widths[h]) for h in headers)
    divider_line = "-+-".join("-" * col_widths[h] for h in headers)

    print(header_line)
    print(divider_line)

    for row in rows:
        line = sep.join(row.get(h, "").ljust(col_widths[h]) for h in headers)
        print(line)


def main() -> int:
    strategies = load_strategies()
    if not strategies:
        return 1

    rows = build_table_rows(strategies)
    print_table(rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
