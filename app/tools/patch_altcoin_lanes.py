from __future__ import annotations

from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[2]
CFG = ROOT / "config"

STRATEGIES = CFG / "strategies.yaml"
SUBACCOUNTS = CFG / "subaccounts.yaml"

ALT_LANES = {
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
    "flashback08": {
        "strategy_name": "Alt08_SwingTrend_Alts25x",
        "role": "alt_swing_trend_25x",
        "ai_profile": "swing_v1",
        "symbols": ["SOLUSDT","AVAXUSDT","INJUSDT","LINKUSDT","OPUSDT","ARBUSDT","NEARUSDT","HBARUSDT","ADAUSDT","XRPUSDT","DOGEUSDT"],
        "timeframes": ["30","240"],
        "setup_types": ["swing_trend_follow","swing_reversion_extreme"],
        "risk_pct": 0.10,
        "max_concurrent_positions": 1,
        "exit_profile": "standard_5",
        "promotion": {"enabled": True, "min_trades": 150, "min_winrate": 0.50, "min_avg_r": 0.30, "min_expectancy_r": 0.15, "max_drawdown_pct": 20.0},
    },
    "flashback09": {
        "strategy_name": "Alt09_RangeFade_Alts25x",
        "role": "alt_range_fade_25x",
        "ai_profile": "range_v1",
        "symbols": ["ARBUSDT","OPUSDT","NEARUSDT","HBARUSDT","SOLUSDT","AVAXUSDT","LINKUSDT","ADAUSDT","XRPUSDT","DOGEUSDT"],
        "timeframes": ["5","15"],
        "setup_types": ["intraday_range_fade","failed_breakout_fade"],
        "risk_pct": 0.10,
        "max_concurrent_positions": 1,
        "exit_profile": "standard_5",
        "promotion": {"enabled": True, "min_trades": 180, "min_winrate": 0.50, "min_avg_r": 0.25, "min_expectancy_r": 0.12, "max_drawdown_pct": 20.0},
    },
}

def _load(p: Path):
    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}

def _dump(p: Path, data):
    p.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8")

def main() -> int:
    strat = _load(STRATEGIES)
    subs = _load(SUBACCOUNTS)

    # strategies.yaml expects: {"subaccounts": [ ... ]}
    rows = strat.get("subaccounts") or []
    if not isinstance(rows, list):
        raise SystemExit("strategies.yaml: top-level subaccounts must be a list")

    # Update existing rows in-place
    by_label = {r.get("account_label"): r for r in rows if isinstance(r, dict) and r.get("account_label")}
    missing = [lab for lab in ALT_LANES.keys() if lab not in by_label]
    if missing:
        raise SystemExit(f"strategies.yaml missing entries for labels: {missing}")

    for lab, cfg in ALT_LANES.items():
        r = by_label[lab]
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

    # subaccounts.yaml is mapping style at top-level
    for lab, cfg in ALT_LANES.items():
        if lab not in subs or not isinstance(subs.get(lab), dict):
            raise SystemExit(f"subaccounts.yaml missing mapping for {lab}")
        subs[lab]["strategy_name"] = cfg["strategy_name"]
        subs[lab]["role"] = cfg["role"]
        subs[lab]["enabled"] = True
        subs[lab]["enable_ai_stack"] = True
        subs[lab]["ai_profile"] = cfg["ai_profile"]
        subs[lab]["automation_mode"] = "LEARN_DRY"

    _dump(STRATEGIES, strat)
    _dump(SUBACCOUNTS, subs)

    print("OK: patched strategies.yaml + subaccounts.yaml for altcoin lanes: 04/06/08/09")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
