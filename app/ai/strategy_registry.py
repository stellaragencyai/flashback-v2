#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — YAML Registry Loader (Hardened, Option-A Compatible)

Builds a registry mapping:
  strategy_id -> [{bot_id, subaccount, status}, ...]

This loader is defensive and supports multiple config shapes because humans
love "almost the same but slightly different" YAML formats.

Supported shapes
----------------
config/strategies.yaml:
  A) Option A (current canonical): { subaccounts: [ {strategy_name, ...}, ... ] }
  B) Legacy: { strategies: [ {id|strategy_id|name, ...}, ... ] }

config/subaccounts.yaml:
  A) Option A (current canonical): top-level mapping {flashback01: {...}, main: {...}, ...}
  B) Legacy: { subaccounts: [ {label|id|name, ...}, ... ] }

config/bots.yaml:
  Expected: { bots: [ {id, strategy, subaccount, ...}, ... ] }

Design goals
------------
- Fail-soft with clear structure checks
- Path-safe (works regardless of cwd)
- Defensive YAML parsing
- Deterministic output ordering
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

try:
    import yaml  # type: ignore
except Exception as e:  # pragma: no cover
    raise RuntimeError("PyYAML is required. Install with: pip install pyyaml") from e


# ----------------------------- PATHS ----------------------------------------

ROOT = Path(__file__).resolve().parents[2]  # app/... -> repo root
CONFIG_DIR = ROOT / "config"

STRATEGIES_YAML = CONFIG_DIR / "strategies.yaml"
BOTS_YAML = CONFIG_DIR / "bots.yaml"
SUBACCOUNTS_YAML = CONFIG_DIR / "subaccounts.yaml"


# ----------------------------- HELPERS --------------------------------------

def _safe_read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def load_yaml(path: Path) -> Dict[str, Any]:
    """
    Load YAML file into a dict. Fail-soft:
    - missing file -> {}
    - invalid YAML -> {}
    - non-dict root -> {}
    """
    try:
        if not path.exists():
            return {}
        raw = _safe_read_text(path)
        obj = yaml.safe_load(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _as_list(x: Any) -> List[Any]:
    return x if isinstance(x, list) else []


def _as_dict(x: Any) -> Dict[str, Any]:
    return x if isinstance(x, dict) else {}


def _as_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s or None


def _warn(msg: str) -> None:
    # Enable warnings if you want them:
    #   $env:FB_STRATEGY_REGISTRY_WARN = "1"
    if os.environ.get("FB_STRATEGY_REGISTRY_WARN", "").strip() in ("1", "true", "True", "YES", "yes"):
        print(f"[strategy_registry] WARN: {msg}")


def _strategy_ids_from_strategies_yaml(cfg: Dict[str, Any]) -> Set[str]:
    """
    Extract known strategy ids from config/strategies.yaml.

    Supports:
      - canonical Option A: top-level "subaccounts" list with "strategy_name" (preferred)
      - legacy: top-level "strategies" list with "id"/"strategy_id"/"name"
    """
    out: Set[str] = set()

    # Option A: strategies.yaml holds a list under "subaccounts"
    for row in _as_list(cfg.get("subaccounts")):
        if isinstance(row, dict):
            # Prefer strategy_name, but also accept "id" if you ever add it
            sid = _as_str(row.get("strategy_name") or row.get("id") or row.get("strategy_id") or row.get("name"))
            if sid:
                out.add(sid)

    # Legacy fallback: strategies.yaml has "strategies"
    for row in _as_list(cfg.get("strategies")):
        if isinstance(row, dict):
            sid = _as_str(row.get("id") or row.get("strategy_id") or row.get("name"))
            if sid:
                out.add(sid)

    return out


def _subaccount_labels_from_subaccounts_yaml(cfg: Dict[str, Any]) -> Set[str]:
    """
    Extract known subaccount labels from config/subaccounts.yaml.

    Supports:
      - canonical Option A: top-level mapping of account_label -> { ... }
      - legacy: top-level "subaccounts" list with "label"/"id"/"name"
    """
    out: Set[str] = set()

    # Option A: mapping style
    for k, v in cfg.items():
        if k in ("version", "notes"):
            continue
        if k == "legacy":
            continue
        if isinstance(v, dict):
            out.add(str(k))

    # Legacy list fallback (rare)
    for row in _as_list(cfg.get("subaccounts")):
        if isinstance(row, dict):
            lbl = _as_str(row.get("label") or row.get("account_label") or row.get("id") or row.get("name"))
            if lbl:
                out.add(lbl)

    return out


# ----------------------------- CORE -----------------------------------------

def load_strategies() -> Dict[str, List[Dict[str, str]]]:
    """
    Builds:
      {
        "<strategy_id>": [
            {"bot_id": "...", "subaccount": "...", "status": "idle"},
            ...
        ],
        ...
      }

    Notes:
    - Deterministic ordering: bots are processed in stable order of appearance.
    - If a bot entry is missing required fields, it is skipped (fail-soft).
    - strategies.yaml and subaccounts.yaml are loaded to validate presence
      (but do not hard-fail if missing, by design).
    """
    strategies_cfg = load_yaml(STRATEGIES_YAML)
    bots_cfg = load_yaml(BOTS_YAML)
    subaccounts_cfg = load_yaml(SUBACCOUNTS_YAML)

    strategies_defined = _strategy_ids_from_strategies_yaml(strategies_cfg)
    subaccounts_defined = _subaccount_labels_from_subaccounts_yaml(subaccounts_cfg)

    if not strategies_defined:
        _warn("No strategy ids discovered from strategies.yaml (check shape/keys).")
    if not subaccounts_defined:
        _warn("No subaccounts discovered from subaccounts.yaml (check shape/keys).")

    registry: Dict[str, List[Dict[str, str]]] = {}

    bots_list = _as_list(bots_cfg.get("bots"))
    for bot in bots_list:
        if not isinstance(bot, dict):
            continue

        bot_id = _as_str(bot.get("id"))
        strategy_id = _as_str(bot.get("strategy"))
        subaccount = _as_str(bot.get("subaccount"))

        # Required fields
        if not bot_id or not strategy_id or not subaccount:
            continue

        # Best-effort validation (do NOT fail)
        if strategies_defined and strategy_id not in strategies_defined:
            _warn(f"Bot '{bot_id}' references strategy '{strategy_id}' not declared in strategies.yaml")
        if subaccounts_defined and subaccount not in subaccounts_defined:
            _warn(f"Bot '{bot_id}' references subaccount '{subaccount}' not declared in subaccounts.yaml")

        registry.setdefault(strategy_id, []).append(
            {"bot_id": bot_id, "subaccount": subaccount, "status": "idle"}
        )

    return registry


__all__ = ["load_yaml", "load_strategies"]
