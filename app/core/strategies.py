#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
app.core.strategies

Flashback — Strategy Registry Loader & Gate

Normalization rules (permanent):
- strategies.yaml owns STRATEGY INTENT only: symbols/timeframes/setup_types/exit_profile/ai_profile/automation_mode.
- Risk is NOT owned here. Risk is canonical in config/risk_profiles.yaml and referenced via subaccounts.yaml risk_profile.
- Option A Snipers (07/08/09) must be hard to misconfigure:
    * exactly 1 symbol in strategies.yaml
    * max_concurrent_positions must be 1
    * ai_profile must be sniper_v1
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Optional, Any

import yaml  # type: ignore

from app.core.config import settings
from app.core.logger import get_logger

log = get_logger("strategies")

CONFIG_PATH = Path(settings.ROOT) / "config" / "strategies.yaml"

# Option A sniper intent constraints (risk is NOT validated here)
SNIPER_EXPECTED_AI_PROFILE = "sniper_v1"
SNIPER_EXPECTED_MAX_CONCURRENCY = 1


@dataclass
class Strategy:
    sub_uid: Optional[int]
    account_label: str
    name: str  # canonical strategy_name (v7+)
    role: str
    enabled: bool
    symbols: List[str]
    timeframes: List[str]
    max_concurrent_positions: int
    ai_profile: str
    automation_mode: str
    exit_profile: Optional[Any]
    raw: Dict[str, Any]

    @property
    def id(self) -> str:
        label_part = self.account_label or "no_label"
        uid_part = f"{self.sub_uid}" if self.sub_uid is not None else "MAIN"
        return f"{self.name} [{label_part} | {uid_part}]"

    @property
    def is_off(self) -> bool:
        return (self.automation_mode or "").upper() == "OFF"

    @property
    def is_learn_dry(self) -> bool:
        return (self.automation_mode or "").upper() == "LEARN_DRY"

    @property
    def is_live_canary(self) -> bool:
        return (self.automation_mode or "").upper() == "LIVE_CANARY"

    @property
    def is_live_full(self) -> bool:
        return (self.automation_mode or "").upper() == "LIVE_FULL"

    @property
    def can_trade_live(self) -> bool:
        return self.is_live_canary or self.is_live_full

    @property
    def wants_ai_eval(self) -> bool:
        return not self.is_off

    @property
    def is_sniper(self) -> bool:
        r = (self.role or "").lower()
        n = (self.name or "")
        return r.startswith("sniper_") or n.startswith("Sniper")


_STRATEGIES_CACHE: Optional[List[Strategy]] = None


def _load_yaml() -> Dict[str, Any]:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"strategies.yaml not found at {CONFIG_PATH}")
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError("strategies.yaml root must be a mapping")
    return data


def _as_int(v: Any, default: int = 0) -> int:
    try:
        if v in (None, "", "null"):
            return int(default)
        return int(v)
    except Exception:
        return int(default)


def _normalize_mode(v: Any) -> str:
    # yaml "false" can become boolean False; normalize that to OFF
    if v is False:
        return "OFF"
    mode = str(v or "OFF").strip().upper()
    if mode in ("FALSE", "NONE", "0"):
        return "OFF"
    return mode or "OFF"


def _normalize_symbols(symbols_raw: Any) -> List[str]:
    if not isinstance(symbols_raw, list):
        return []
    out: List[str] = []
    for s in symbols_raw:
        ss = str(s).strip().upper()
        if ss:
            out.append(ss)
    # de-dupe but preserve order
    deduped: List[str] = []
    seen = set()
    for s in out:
        if s not in seen:
            deduped.append(s)
            seen.add(s)
    return deduped


def _normalize_timeframes(tfs_raw: Any) -> List[str]:
    if not isinstance(tfs_raw, list):
        return []
    out: List[str] = []
    for tf in tfs_raw:
        t = str(tf).strip()
        if t:
            out.append(t)
    # de-dupe preserve order
    deduped: List[str] = []
    seen = set()
    for t in out:
        if t not in seen:
            deduped.append(t)
            seen.add(t)
    return deduped


def _validate_strategy(s: Strategy) -> None:
    # Basic sanity
    if not s.account_label:
        raise ValueError(f"Strategy missing account_label: {s.raw}")
    if not s.name:
        raise ValueError(f"Strategy missing strategy_name/name for account {s.account_label}: {s.raw}")
    if s.enabled and not s.timeframes:
        raise ValueError(f"{s.id}: enabled but timeframes is empty")
    if s.enabled and not s.symbols and s.sub_uid is not None:
        raise ValueError(f"{s.id}: enabled but symbols is empty")

    # Sniper INTENT invariants (risk is enforced via risk_profile elsewhere)
    if s.is_sniper and s.enabled:
        if len(s.symbols) != 1:
            raise ValueError(f"{s.id}: SNIPER must have exactly 1 symbol, got {s.symbols}")
        if s.max_concurrent_positions != SNIPER_EXPECTED_MAX_CONCURRENCY:
            raise ValueError(
                f"{s.id}: SNIPER max_concurrent_positions must be {SNIPER_EXPECTED_MAX_CONCURRENCY}, "
                f"got {s.max_concurrent_positions}"
            )
        if (s.ai_profile or "").strip() != SNIPER_EXPECTED_AI_PROFILE:
            raise ValueError(
                f"{s.id}: SNIPER ai_profile must be '{SNIPER_EXPECTED_AI_PROFILE}', got '{s.ai_profile}'"
            )

    if s.is_off and s.can_trade_live:
        raise ValueError(f"{s.id}: automation_mode OFF cannot be live-tradable")


def _parse_strategies(data: Dict[str, Any]) -> List[Strategy]:
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

        # v7+ canonical field is "strategy_name"; keep "name" as legacy fallback
        strategy_name = str(raw.get("strategy_name") or raw.get("name") or "").strip()
        if not strategy_name:
            strategy_name = f"Sub_{sub_uid if sub_uid is not None else 'MAIN'}"

        role = str(raw.get("role", "") or "").strip()
        enabled = bool(raw.get("enabled", False))

        symbols = _normalize_symbols(raw.get("symbols"))
        timeframes = _normalize_timeframes(raw.get("timeframes"))

        max_concurrent_positions = _as_int(raw.get("max_concurrent_positions"), default=0)
        ai_profile = str(raw.get("ai_profile", "") or "").strip()
        automation_mode = _normalize_mode(raw.get("automation_mode"))

        account_label = str(raw.get("account_label") or "").strip()
        if not account_label:
            account_label = "main" if sub_uid is None else f"sub_{sub_uid}"

        exit_profile = raw.get("exit_profile")

        s = Strategy(
            sub_uid=sub_uid,
            account_label=account_label,
            name=strategy_name,
            role=role,
            enabled=enabled,
            symbols=symbols,
            timeframes=timeframes,
            max_concurrent_positions=max_concurrent_positions,
            ai_profile=ai_profile,
            automation_mode=automation_mode,
            exit_profile=exit_profile,
            raw=raw,
        )

        _validate_strategy(s)
        strategies.append(s)

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


def get_strategy_for_sub(sub_uid: str) -> Optional[Dict[str, Any]]:
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
