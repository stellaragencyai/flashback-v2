#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
app.core.strategies

Flashback — Strategy Registry Loader & Gate
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Optional

import yaml  # type: ignore

from app.core.config import settings
from app.core.logger import get_logger

log = get_logger("strategies")

CONFIG_PATH = Path(settings.ROOT) / "config" / "strategies.yaml"


@dataclass
class Strategy:
    sub_uid: Optional[int]
    account_label: str
    name: str
    role: str
    enabled: bool
    symbols: List[str]
    timeframes: List[str]
    risk_per_trade_pct: float
    risk_pct: float
    max_concurrent_positions: int
    ai_profile: str
    automation_mode: str
    exit_profile: Optional[object]
    raw: Dict

    @property
    def id(self) -> str:
        label_part = self.account_label or "no_label"
        uid_part = f"{self.sub_uid}" if self.sub_uid is not None else "MAIN"
        return f"{self.name} [{label_part} | {uid_part}]"

    @property
    def is_off(self) -> bool:
        return self.automation_mode.upper() == "OFF"

    @property
    def is_learn_dry(self) -> bool:
        return self.automation_mode.upper() == "LEARN_DRY"

    @property
    def is_live_canary(self) -> bool:
        return self.automation_mode.upper() == "LIVE_CANARY"

    @property
    def is_live_full(self) -> bool:
        return self.automation_mode.upper() == "LIVE_FULL"

    @property
    def can_trade_live(self) -> bool:
        return self.is_live_canary or self.is_live_full

    @property
    def wants_ai_eval(self) -> bool:
        if self.is_off:
            return False
        return True


_STRATEGIES_CACHE: Optional[List[Strategy]] = None


def _load_yaml() -> Dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"strategies.yaml not found at {CONFIG_PATH}")
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError("strategies.yaml root must be a mapping")
    return data


def _parse_strategies(data: Dict) -> List[Strategy]:
    subaccounts = data.get("subaccounts") or []
    if not isinstance(subaccounts, list):
        raise ValueError("strategies.yaml: 'subaccounts' must be a list")

    strategies: List[Strategy] = []
    for raw in subaccounts:
        if not isinstance(raw, dict):
            continue

        sub_uid_raw = raw.get("sub_uid")
        if sub_uid_raw in (None, "", "null"):
            sub_uid = None
        else:
            try:
                sub_uid = int(sub_uid_raw)
            except Exception:
                log.warning("Skipping strategy with invalid sub_uid: %r", raw)
                continue

        name = str(raw.get("name", f"Sub_{sub_uid if sub_uid is not None else 'MAIN'}"))
        role = str(raw.get("role", "") or "")
        enabled = bool(raw.get("enabled", False))

        symbols_raw = raw.get("symbols") or []
        timeframes_raw = raw.get("timeframes") or []

        symbols = [str(s).strip().upper() for s in symbols_raw if str(s).strip()]
        timeframes = [str(tf).strip() for tf in timeframes_raw if str(tf).strip()]

        risk_per_trade_pct = float(raw.get("risk_per_trade_pct", 0.0) or 0.0)
        risk_pct = float(raw.get("risk_pct", risk_per_trade_pct) or 0.0)
        max_concurrent_positions = int(raw.get("max_concurrent_positions", 0) or 0)
        ai_profile = str(raw.get("ai_profile", "") or "")
        automation_mode = str(raw.get("automation_mode", "OFF") or "OFF").upper()

        account_label = str(raw.get("account_label") or "").strip()
        if not account_label:
            account_label = "main" if sub_uid is None else f"sub_{sub_uid}"

        exit_profile = raw.get("exit_profile")

        strategies.append(
            Strategy(
                sub_uid=sub_uid,
                account_label=account_label,
                name=name,
                role=role,
                enabled=enabled,
                symbols=symbols,
                timeframes=timeframes,
                risk_per_trade_pct=risk_per_trade_pct,
                risk_pct=risk_pct,
                max_concurrent_positions=max_concurrent_positions,
                ai_profile=ai_profile,
                automation_mode=automation_mode,
                exit_profile=exit_profile,
                raw=raw,
            )
        )

    log.info(
        "Loaded %d strategies from %s (version=%s)",
        len(strategies),
        CONFIG_PATH,
        data.get("version"),
    )
    return strategies


def _ensure_loaded() -> None:
    global _STRATEGIES_CACHE
    if _STRATEGIES_CACHE is None:
        data = _load_yaml()
        _STRATEGIES_CACHE = _parse_strategies(data)


def all_sub_strategies() -> List[Strategy]:
    _ensure_loaded()
    return list(_STRATEGIES_CACHE or [])


def enabled_strategies() -> List[Strategy]:
    return [s for s in all_sub_strategies() if s.enabled]


def strategies_for_symbol_timeframe(symbol: str, timeframe: str) -> List[Strategy]:
    sym = symbol.upper()
    tf = str(timeframe).strip()
    return [s for s in enabled_strategies() if sym in s.symbols and tf in s.timeframes]


def live_strategies_for_signal(symbol: str, timeframe: str) -> List[Strategy]:
    return [s for s in strategies_for_symbol_timeframe(symbol, timeframe) if s.can_trade_live]


def ai_strategies_for_signal(symbol: str, timeframe: str) -> List[Strategy]:
    return [s for s in strategies_for_symbol_timeframe(symbol, timeframe) if s.wants_ai_eval]


def get_strategy_by_sub_uid(sub_uid: int) -> Optional[Strategy]:
    for s in all_sub_strategies():
        if s.sub_uid == sub_uid:
            return s
    return None


def get_strategy_for_sub(sub_uid: str) -> Optional[Dict]:
    """
    Compatibility helper used by tp_sl_manager.py.
    Returns the raw dict from strategies.yaml for this sub_uid.
    """
    try:
        uid = int(sub_uid)
    except Exception:
        return None

    s = get_strategy_by_sub_uid(uid)
    return s.raw if s else None
