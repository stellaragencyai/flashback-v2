#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Strategy Gate

Purpose
-------
Single source of truth for answering questions like:

  • "Given this symbol + timeframe, which strategies care about it?"
  • "For sub_uid X, what is its risk, mode, and symbols/TFs?"
  • "Is this strategy currently allowed to auto-trade or only learn/paper?"

This sits on top of:
    - config/strategies.yaml
    - config/subaccounts.yaml
    - config/risk_profiles.yaml
    - app.core.strategies
    - app.core.subs

Typical usage (executor, AI gate, dashboards):
    from app.core.strategy_gate import (
        get_strategies_for_signal,
        get_strategy_for_sub,
        is_strategy_live,
        is_strategy_enabled,
        strategy_risk_pct,
    )

Modes (Option A)
----------------
We keep the richer 4-mode automation scheme used by executor_v2:

    OFF         : ignore strategy
    LEARN_DRY   : learn-only / paper mode (log, AI, no live orders)
    LIVE_CANARY : small-size live trades (canary / experimental)
    LIVE_FULL   : normal live trading

Timeframes
----------
strategies.yaml uses raw minute intervals as strings ("1", "5", "15", "60", "240"...).
Signal engine may emit "5", "5m", "1h", etc. We normalize both sides to raw minute strings.

Risk Source of Truth (IMPORTANT)
--------------------------------
This module now supports a hardened, deterministic risk resolution order:

Primary:
  1) Resolve account_label for the strategy (prefer strategy["account_label"], else look up via subs.py by sub_uid)
  2) From subaccounts.yaml, read risk_profile key for that account_label
  3) From risk_profiles.yaml, load profiles[risk_profile] and extract per-trade risk %
     (supports multiple schema keys: risk_per_trade_pct, risk_pct, max_risk_pct, per_trade_risk_pct)

Fallbacks:
  A) strategy["risk_per_trade_pct"] (legacy normalized key)
  B) strategy["risk_pct"] / "risk_percent" / "max_risk_pct" (common legacy keys)
  C) 0.0 (fail-closed)

If you want risk_profiles.yaml to truly govern executor sizing, make executor_v2 call
strategy_risk_pct(strategy) and apply multipliers (learning/decision) there.
"""

from __future__ import annotations

import re
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from app.core import strategies as stratreg
from app.core.logger import get_logger
from app.core.subs import get_sub_by_uid, get_sub_by_label

log = get_logger("strategy_gate")

# ---------------------------------------------------------------------------
# Helpers for timeframe normalization
# ---------------------------------------------------------------------------


def _normalize_tf(tf: str) -> str:
    """
    Normalize timeframe strings.

    Accepted inputs:
        "5"   -> "5"
        "5m"  -> "5"
        "15"  -> "15"
        "1h"  -> "60"
        "60"  -> "60"
        "4h"  -> "240"
        "240" -> "240"

    We keep strategies.yaml in raw-minute form ("1", "5", "15", "60", "240", ...).
    """
    s = str(tf).strip().lower()

    # pure integer string -> minutes already
    if s.isdigit():
        return s

    # match like: 5m, 15m, 1h, 4h, 1d, etc.
    m = re.match(r"^(\d+)([mhd])$", s)
    if not m:
        # unknown pattern, return as-is (fail-closed-ish for matching)
        return s

    val = int(m.group(1))
    unit = m.group(2)

    if unit == "m":
        return str(val)
    if unit == "h":
        return str(val * 60)
    if unit == "d":
        return str(val * 60 * 24)

    return s


# ---------------------------------------------------------------------------
# Strategy normalization helpers
# ---------------------------------------------------------------------------


def _strategy_to_dict(s: Any) -> Dict[str, Any]:
    """
    Normalize a strategy object into a plain dict.

    Supports:
      - raw dicts
      - dataclass-based Strategy objects
      - generic objects with __dict__
    """
    if isinstance(s, dict):
        return s

    # Try dataclass first
    try:
        return asdict(s)  # type: ignore[arg-type]
    except Exception:
        pass

    # Fallback to __dict__
    if hasattr(s, "__dict__"):
        return dict(s.__dict__)  # type: ignore[assignment]

    # Last resort: wrap as-is
    return {"value": s}


# ---------------------------------------------------------------------------
# Config loaders (cached)
# ---------------------------------------------------------------------------

_ROOT = Path(__file__).resolve().parents[2]  # .../app/core/strategy_gate.py -> .../ (repo root)
_CONFIG_DIR = _ROOT / "config"
_RISK_PROFILES_PATH = _CONFIG_DIR / "risk_profiles.yaml"

# tiny cache to avoid hammering disk
_CACHE: Dict[str, Any] = {
    "loaded_ms": 0,
    "risk_profiles": None,
}
_CACHE_TTL_MS = 2000  # 2 seconds: fast enough for dev, stable enough for runtime


def _now_ms() -> int:
    return int(time.time() * 1000)


def _load_risk_profiles_cached() -> Dict[str, Any]:
    """
    Load config/risk_profiles.yaml and return the dict.

    Cached with short TTL to prevent repeated disk IO during tight loops.
    Fail-closed: returns {} on any error or missing file.
    """
    now = _now_ms()
    loaded_ms = int(_CACHE.get("loaded_ms") or 0)
    if (now - loaded_ms) < _CACHE_TTL_MS and isinstance(_CACHE.get("risk_profiles"), dict):
        return _CACHE["risk_profiles"]

    data: Dict[str, Any] = {}
    try:
        if _RISK_PROFILES_PATH.exists():
            data = yaml.safe_load(_RISK_PROFILES_PATH.read_text(encoding="utf-8")) or {}
    except Exception:
        data = {}

    _CACHE["risk_profiles"] = data if isinstance(data, dict) else {}
    _CACHE["loaded_ms"] = now
    return _CACHE["risk_profiles"]


# ---------------------------------------------------------------------------
# Risk profile key normalization (ANTI-DRIFT)
# ---------------------------------------------------------------------------

# Canonical aliases live HERE (one place) to prevent “identical profile” duplication drift.
# If you later delete aliases from config, nothing breaks.
_RISK_PROFILE_ALIASES: Dict[str, str] = {
    # Sniper aliases
    "sniper_25": "sniper_v1",
    "sniper25": "sniper_v1",
    "sniper-v1": "sniper_v1",
    "sniper-v01": "sniper_v1",
}

# Track one-time warnings per alias used (so logs don’t become useless noise)
_RISK_PROFILE_ALIAS_WARNED: set[str] = set()


def _normalize_risk_profile_key(key: Any) -> Tuple[Optional[str], Optional[str]]:
    """
    Normalize a risk_profile key from subaccounts.yaml into a canonical key.

    Returns:
      (canonical_key, alias_used)

      - canonical_key: normalized key that should exist in risk_profiles.yaml.profiles
      - alias_used   : original key if it was an alias, else None
    """
    if not isinstance(key, str):
        return None, None

    raw = key.strip()
    if not raw:
        return None, None

    k = raw.lower().strip()
    canonical = _RISK_PROFILE_ALIASES.get(k, raw)
    alias_used = raw if canonical != raw else None
    return canonical, alias_used


def _warn_alias_once(account_label: str, alias_used: str, canonical: str) -> None:
    """
    Warn once per process for each alias encountered.
    """
    token = f"{alias_used}->{canonical}"
    if token in _RISK_PROFILE_ALIAS_WARNED:
        return
    _RISK_PROFILE_ALIAS_WARNED.add(token)
    log.warning(
        "Risk profile alias used for account_label=%s: '%s' -> '%s' (safe; consider migrating config)",
        account_label,
        alias_used,
        canonical,
    )


# ---------------------------------------------------------------------------
# Core accessors
# ---------------------------------------------------------------------------


def _normalized_strategies() -> List[Dict[str, Any]]:
    """
    Load all strategies and attach some normalized fields:

        - "sub_uid_str"          : canonical string version of sub_uid
        - "automation_mode_norm" : OFF | LEARN_DRY | LIVE_CANARY | LIVE_FULL
        - "symbols_norm"         : [uppercased symbols]
        - "timeframes_norm"      : [normalized raw mins, as strings]
    """
    out: List[Dict[str, Any]] = []

    for raw in stratreg.all_sub_strategies():
        s = _strategy_to_dict(raw)

        sub_uid_raw = s.get("sub_uid", None)

        # executor only cares about real subaccounts; manual main (sub_uid=None)
        # will be skipped here and handled elsewhere (e.g. TP/SL manager).
        if sub_uid_raw is None:
            continue

        sub_uid = str(sub_uid_raw)

        # normalize automation mode into the 4-mode scheme used by executor_v2
        mode_raw = str(s.get("automation_mode", "OFF")).strip().upper()
        if mode_raw not in ("OFF", "LEARN_DRY", "LIVE_CANARY", "LIVE_FULL"):
            # Fail-closed: any weird / missing value becomes OFF
            mode_raw = "OFF"

        symbols_raw = s.get("symbols") or []
        tfs_raw = s.get("timeframes") or []

        symbols_norm = [str(sym).upper().strip() for sym in symbols_raw if str(sym).strip()]
        tfs_norm = [_normalize_tf(tf) for tf in tfs_raw if str(tf).strip()]

        wrapped = dict(s)
        wrapped["sub_uid_str"] = sub_uid
        wrapped["automation_mode_norm"] = mode_raw
        wrapped["symbols_norm"] = symbols_norm
        wrapped["timeframes_norm"] = tfs_norm
        out.append(wrapped)

    return out


def all_strategies() -> List[Dict[str, Any]]:
    """
    Public: return all normalized strategies.
    """
    return _normalized_strategies()


def get_strategy_for_sub(sub_uid: str) -> Optional[Dict[str, Any]]:
    """
    Get the strategy dict for a given sub_uid (string or int).
    Returns normalized dict, or None if not found.
    """
    sub_uid_str = str(sub_uid)
    for s in _normalized_strategies():
        if s.get("sub_uid_str") == sub_uid_str:
            return s
    return None


def get_strategies_for_signal(symbol: str, timeframe: str) -> List[Dict[str, Any]]:
    """
    Given a signal (symbol + timeframe), return all strategies that
    should consider acting on it.

    Strategy matches if:
      - symbol is in its symbols list
      - normalized timeframe matches one of its timeframes

    The returned strategies are normalized and include:
        sub_uid_str, automation_mode_norm, symbols_norm, timeframes_norm
    """
    sym_u = str(symbol).upper().strip()
    tf_norm = _normalize_tf(timeframe)

    matches: List[Dict[str, Any]] = []
    for s in _normalized_strategies():
        if sym_u not in s.get("symbols_norm", []):
            continue
        if tf_norm not in s.get("timeframes_norm", []):
            continue
        matches.append(s)
    return matches


# ---------------------------------------------------------------------------
# Automation mode helpers (4-mode aware)
# ---------------------------------------------------------------------------


def strategy_mode(strategy: Dict[str, Any]) -> str:
    """
    Return the normalized automation mode for a strategy:
        OFF | LEARN_DRY | LIVE_CANARY | LIVE_FULL
    """
    mode = strategy.get("automation_mode_norm") or str(strategy.get("automation_mode", "")).upper().strip()
    if mode not in ("OFF", "LEARN_DRY", "LIVE_CANARY", "LIVE_FULL"):
        mode = "OFF"
    return mode


def is_strategy_enabled(strategy: Dict[str, Any]) -> bool:
    """
    Returns True if the strategy is 'enabled' in strategies.yaml.
    (This is separate from automation_mode; you can disable a strategy entirely.)

    IMPORTANT:
      Default is True. Defaulting to False silently disables everything, which is
      a classic human mistake.
    """
    return bool(strategy.get("enabled", True))


def is_strategy_off(strategy: Dict[str, Any]) -> bool:
    """
    Returns True if the strategy's automation_mode is OFF.
    """
    return strategy_mode(strategy) == "OFF"


def is_strategy_learn_dry(strategy: Dict[str, Any]) -> bool:
    """
    Returns True if the strategy's automation_mode is LEARN_DRY
    (learn-only / paper-style: AI + logging, no live orders).
    """
    return strategy_mode(strategy) == "LEARN_DRY"


def is_strategy_live_canary(strategy: Dict[str, Any]) -> bool:
    """
    Returns True if the strategy's automation_mode is LIVE_CANARY.
    """
    return strategy_mode(strategy) == "LIVE_CANARY"


def is_strategy_live_full(strategy: Dict[str, Any]) -> bool:
    """
    Returns True if the strategy's automation_mode is LIVE_FULL.
    """
    return strategy_mode(strategy) == "LIVE_FULL"


def is_strategy_live(strategy: Dict[str, Any]) -> bool:
    """
    Convenience: returns True if the strategy is in ANY live mode:
        LIVE_CANARY or LIVE_FULL
    """
    return strategy_mode(strategy) in ("LIVE_CANARY", "LIVE_FULL")


def is_strategy_paper(strategy: Dict[str, Any]) -> bool:
    """
    Backwards-compatible helper: treat LEARN_DRY as 'paper' mode.
    """
    return strategy_mode(strategy) == "LEARN_DRY"


# ---------------------------------------------------------------------------
# Risk & concurrency helpers
# ---------------------------------------------------------------------------


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _resolve_account_label(strategy: Dict[str, Any]) -> Optional[str]:
    """
    Resolve the account_label for a strategy.

    Priority:
      1) strategy['account_label'] (if present)
      2) lookup by sub_uid via subs.get_sub_by_uid()
    """
    al = strategy.get("account_label")
    if isinstance(al, str) and al.strip():
        return al.strip()

    sub_uid = strategy.get("sub_uid_str") or strategy.get("sub_uid")
    if sub_uid is None:
        return None

    try:
        sub = get_sub_by_uid(str(sub_uid))
    except Exception:
        sub = None

    if isinstance(sub, dict):
        al2 = sub.get("account_label")
        if isinstance(al2, str) and al2.strip():
            return al2.strip()

    return None


def _risk_pct_from_profile(profile: Dict[str, Any]) -> float:
    """
    Extract per-trade risk % from a risk profile dict.

    Supports multiple schema variants because humans cannot commit to one:
      - risk_per_trade_pct (preferred)
      - per_trade_risk_pct
      - risk_pct
      - max_risk_pct (fallback)
    """
    if not isinstance(profile, dict):
        return 0.0

    for k in ("risk_per_trade_pct", "per_trade_risk_pct", "risk_pct", "max_risk_pct"):
        if k in profile:
            v = _safe_float(profile.get(k), default=0.0)
            if v > 0:
                return v

    # Some profiles may nest under "sizing" or similar. Try a couple common ones.
    sizing = profile.get("sizing")
    if isinstance(sizing, dict):
        for k in ("risk_per_trade_pct", "per_trade_risk_pct", "risk_pct", "max_risk_pct"):
            if k in sizing:
                v = _safe_float(sizing.get(k), default=0.0)
                if v > 0:
                    return v

    return 0.0


def strategy_risk_pct(strategy: Dict[str, Any]) -> float:
    """
    Return per-trade risk % (float).

    Primary truth path:
      subaccounts.yaml[risk_profile] -> risk_profiles.yaml.profiles[risk_profile] -> risk_per_trade_pct (or equivalent)

    Fallback:
      strategy["risk_per_trade_pct"] then common legacy keys (risk_pct, risk_percent, max_risk_pct)

    Fail-closed:
      returns 0.0 on any inconsistency.
    """
    # 1) Try profile-driven risk (preferred)
    account_label = _resolve_account_label(strategy)
    if account_label:
        try:
            sub = get_sub_by_label(account_label)
        except Exception:
            sub = None

        if isinstance(sub, dict):
            rp_key_raw = sub.get("risk_profile")
            canonical_key, alias_used = _normalize_risk_profile_key(rp_key_raw)

            if canonical_key:
                if alias_used:
                    _warn_alias_once(account_label, alias_used, canonical_key)

                rp = _load_risk_profiles_cached()
                profiles = (rp.get("profiles") or {}) if isinstance(rp, dict) else {}
                prof = profiles.get(canonical_key) if isinstance(profiles, dict) else None
                pct = _risk_pct_from_profile(prof if isinstance(prof, dict) else {})
                if pct > 0:
                    return float(pct)

    # 2) Legacy: normalized key used by older code paths
    pct = _safe_float(strategy.get("risk_per_trade_pct"), default=0.0)
    if pct > 0:
        return pct

    # 3) Legacy: common variants that might exist in raw YAML
    for k in ("risk_pct", "risk_percent", "max_risk_pct", "risk"):
        pct2 = _safe_float(strategy.get(k), default=0.0)
        if pct2 > 0:
            return pct2

    return 0.0


def strategy_max_concurrent(strategy: Dict[str, Any]) -> int:
    """
    Convenience: get max_concurrent_positions as int.
    If missing, defaults to 1.
    """
    try:
        v = int(strategy.get("max_concurrent_positions", 1))
        return v if v > 0 else 1
    except Exception:
        return 1


def strategy_label(strategy: Dict[str, Any]) -> str:
    """
    Returns a nice label for logs/Telegram:
        "<name> (sub <uid>)"
    """
    sub_uid = strategy.get("sub_uid_str") or strategy.get("sub_uid")
    name = strategy.get("name") or stratreg.get_sub_label(str(sub_uid))
    return f"{name} (sub {sub_uid})"


def should_strategy_handle(symbol: str, timeframe: str) -> Dict[str, Dict[str, Any]]:
    """
    Adapter for callers that want a dict keyed by strategy name.

    Given symbol + timeframe, return:
        {
          "<strategy_name>": <strategy_dict>,
          ...
        }

    Strategy name is taken from:
      - strategy["name"] if present
      - otherwise a label from strategy_label(...)
    """
    matches = get_strategies_for_signal(symbol, timeframe)
    out: Dict[str, Dict[str, Any]] = {}
    for s in matches:
        name = s.get("name") or strategy_label(s)
        out[name] = s
    return out
