from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict

import yaml

ROOT = Path(__file__).resolve().parents[2]
CFG = ROOT / "config"

STRATEGIES = CFG / "strategies.yaml"
SUBACCOUNTS = CFG / "subaccounts.yaml"

# Canonical altcoin learning lanes (Model A):
# - flashback04 / flashback05 / flashback06 share ONE risk envelope in subaccounts.yaml:
#     risk_profile: extractor_25x_v1
# - strategy-specific behavior is handled by symbols/timeframes/setup_types/filters/etc.
ALT_LANES: Dict[str, Dict[str, Any]] = {
    "flashback04": {
        "strategy_name": "Alt04_Breakout_Alts25x",
        "role": "alt_breakout_25x",
        "ai_profile": "breakout_v1",
        "symbols": ["ARBUSDT","OPUSDT","INJUSDT","NEARUSDT","HBARUSDT","AVAXUSDT","SOLUSDT","LINKUSDT","ADAUSDT","XRPUSDT","DOGEUSDT"],
        "timeframes": ["5","15"],
        "setup_types": ["breakout_high","breakout_range","squeeze_release"],
        "risk_pct": 0.10,
        "max_concurrent_positions": 1,
        "exit_profile": "standard_5",
        "promotion": {"enabled": True, "min_trades": 200, "min_winrate": 0.50, "min_avg_r": 0.30, "min_expectancy_r": 0.15, "max_drawdown_pct": 20.0},
    },
    "flashback05": {
        # Model A: match 04 baseline strategy (known-good) unless/until you define a dedicated Alt05 strategy
        "strategy_name": "Alt04_Breakout_Alts25x",
        "role": "alt_breakout_25x",
        "ai_profile": "breakout_v1",
        "symbols": ["ARBUSDT","OPUSDT","INJUSDT","NEARUSDT","HBARUSDT","AVAXUSDT","SOLUSDT","LINKUSDT","ADAUSDT","XRPUSDT","DOGEUSDT"],
        "timeframes": ["5","15"],
        "setup_types": ["breakout_high","breakout_range","squeeze_release"],
        "risk_pct": 0.10,
        "max_concurrent_positions": 1,
        "exit_profile": "standard_5",
        "promotion": {"enabled": True, "min_trades": 200, "min_winrate": 0.50, "min_avg_r": 0.30, "min_expectancy_r": 0.15, "max_drawdown_pct": 20.0},
    },
    "flashback06": {
        "strategy_name": "Alt06_ScalpSweep_Alts25x",
        "role": "alt_sweep_scalp_25x",
        "ai_profile": "scalp_v1",
        "symbols": ["SOLUSDT","AVAXUSDT","INJUSDT","LINKUSDT","OPUSDT","ARBUSDT","NEARUSDT","HBARUSDT","ADAUSDT","XRPUSDT","DOGEUSDT"],
        "timeframes": ["1","5"],
        "setup_types": ["scalp_liquidity_sweep","scalp_reversal_snapback","failed_breakout_fade"],
        "risk_pct": 0.10,
        "max_concurrent_positions": 1,
        "exit_profile": "standard_5",
        "promotion": {"enabled": True, "min_trades": 250, "min_winrate": 0.50, "min_avg_r": 0.20, "min_expectancy_r": 0.10, "max_drawdown_pct": 22.0},
    },
}

ALT_RISK_PROFILE = "extractor_25x_v1"

def _load(p: Path) -> Dict[str, Any]:
    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}

def _dump(p: Path, data: Dict[str, Any]) -> None:
    p.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8")

def _ensure_strategies_rows(strat: Dict[str, Any], labels: list[str]) -> Dict[str, Any]:
    """
    strategies.yaml expects: {"subaccounts": [ {account_label: ...}, ... ]}
    If a label is missing, we add a minimal stub row so the tool can be idempotent.
    """
    rows = strat.get("subaccounts") or []
    if not isinstance(rows, list):
        raise SystemExit("strategies.yaml: top-level subaccounts must be a list")

    by_label = {r.get("account_label"): r for r in rows if isinstance(r, dict) and r.get("account_label")}
    for lab in labels:
        if lab not in by_label:
            stub = {"account_label": lab, "enabled": False}
            rows.append(stub)
            by_label[lab] = stub

    strat["subaccounts"] = rows
    return by_label

def _apply_patch(strat: Dict[str, Any], subs: Dict[str, Any]) -> tuple[bool, list[str]]:
    changed = False
    notes: list[str] = []

    labels = list(ALT_LANES.keys())
    by_label = _ensure_strategies_rows(strat, labels)

    # Patch strategies.yaml subaccounts list entries
    for lab, cfg in ALT_LANES.items():
        r = by_label[lab]
        before = dict(r)

        r["strategy_name"] = cfg["strategy_name"]
        r["role"] = cfg["role"]
        r["enabled"] = True
        r["symbols"] = cfg["symbols"]
        r["timeframes"] = cfg["timeframes"]
        r["setup_types"] = cfg["setup_types"]
        r["risk_per_trade_pct"] = float(cfg["risk_pct"])
        r["risk_pct"] = float(cfg["risk_pct"])
        r["max_concurrent_positions"] = int(cfg["max_concurrent_positions"])
        r["ai_profile"] = cfg["ai_profile"]
        r["automation_mode"] = "LEARN_DRY"
        r["exit_profile"] = cfg["exit_profile"]
        r["promotion_rules"] = cfg["promotion"]

        if r != before:
            changed = True
            notes.append(f"strategies.yaml: patched {lab}")

    # Patch subaccounts.yaml top-level mapping entries
    for lab, cfg in ALT_LANES.items():
        if lab not in subs or not isinstance(subs.get(lab), dict):
            raise SystemExit(f"subaccounts.yaml missing mapping for {lab}")
        before = dict(subs[lab])

        subs[lab]["strategy_name"] = cfg["strategy_name"]
        subs[lab]["role"] = cfg["role"]
        subs[lab]["enabled"] = True
        subs[lab]["enable_ai_stack"] = True
        subs[lab]["ai_profile"] = cfg["ai_profile"]
        subs[lab]["automation_mode"] = "LEARN_DRY"

        # Model A enforcement: unify alt lanes to ONE risk envelope
        subs[lab]["risk_profile"] = ALT_RISK_PROFILE

        if subs[lab] != before:
            changed = True
            notes.append(f"subaccounts.yaml: patched {lab}")

    return changed, notes

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Patch canonical altcoin learning lanes (04/05/06) into strategies.yaml + subaccounts.yaml. "
                    "Safe by default: use --apply to write.",
    )
    ap.add_argument("--apply", action="store_true", help="Write changes to disk (otherwise dry-run).")
    args = ap.parse_args()

    strat = _load(STRATEGIES)
    subs = _load(SUBACCOUNTS)

    changed, notes = _apply_patch(strat, subs)

    if not changed:
        print("OK: no changes needed (altcoin lanes already canonical: 04/05/06).")
        return 0

    if not args.apply:
        print("DRY-RUN: changes would be applied:")
        for n in notes:
            print(" -", n)
        print("DRY-RUN: re-run with --apply to write changes.")
        return 0

    _dump(STRATEGIES, strat)
    _dump(SUBACCOUNTS, subs)
    print("OK: patched strategies.yaml + subaccounts.yaml for canonical altcoin lanes: 04/05/06")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
