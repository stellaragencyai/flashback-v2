#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Paper Tick Daemon (LEARN_DRY closer, v1.1)

Purpose
-------
Drive PaperBroker price updates from the WS-fed trades bus so that
LEARN_DRY paper positions actually CLOSE and produce OutcomeRecords.

Improvements vs v1.0:
- Build label -> symbols map (no more O(labels * symbols) waste)
- Skip brokers with zero open positions
- Periodic reload of strategies.yaml (no restart needed)
"""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import yaml  # type: ignore

# Robust settings / ROOT
try:
    from app.core.config import settings  # type: ignore
except Exception:  # pragma: no cover
    settings = None  # type: ignore

# Logging
try:
    from app.core.logger import get_logger  # type: ignore
except Exception:  # pragma: no cover
    try:
        from app.core.log import get_logger  # type: ignore
    except Exception:  # pragma: no cover
        def get_logger(name: str) -> "logging.Logger":  # type: ignore
            logger_ = logging.getLogger(name)
            if not logger_.handlers:
                handler = logging.StreamHandler(sys.stdout)
                fmt = logging.Formatter(
                    "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
                )
                handler.setFormatter(fmt)
                logger_.addHandler(handler)
            logger_.setLevel(logging.INFO)
            return logger_

# Heartbeat
try:
    from app.core.flashback_common import record_heartbeat  # type: ignore
except Exception:  # pragma: no cover
    def record_heartbeat(name: str) -> None:  # type: ignore[override]
        return None

# Market bus: WS-fed trades
from app.core.market_bus import (  # type: ignore
    get_recent_trades,
    trades_bus_age_sec,
)

# Paper broker (per-account LEARN_DRY ledger)
from app.sim.paper_broker import PaperBroker  # type: ignore

ROOT: Path = settings.ROOT if settings else Path(__file__).resolve().parents[2]  # type: ignore
log = get_logger("paper_tick_daemon")

STRATEGIES_PATH: Path = ROOT / "config" / "strategies.yaml"


# ---------------------------------------------------------------------------
# Strategy discovery
# ---------------------------------------------------------------------------

def _load_learn_dry_map() -> Tuple[List[str], Dict[str, Set[str]], Set[str]]:
    """
    Scan strategies.yaml and return:

      labels: sorted list of LEARN_DRY account_labels
      label_to_symbols: mapping label -> set(symbols)
      all_symbols: union of all symbols across LEARN_DRY labels
    """
    if not STRATEGIES_PATH.exists():
        log.error("strategies.yaml not found at %s", STRATEGIES_PATH)
        return [], {}, set()

    try:
        raw = yaml.safe_load(STRATEGIES_PATH.read_text(encoding="utf-8")) or {}
    except Exception as e:
        log.error("Failed to parse %s: %r", STRATEGIES_PATH, e)
        return [], {}, set()

    subs = raw.get("subaccounts") or []
    if not isinstance(subs, list):
        log.error("Invalid 'subaccounts' structure in strategies.yaml (expected list).")
        return [], {}, set()

    labels: Set[str] = set()
    label_to_symbols: Dict[str, Set[str]] = {}
    all_symbols: Set[str] = set()

    for sub in subs:
        if not isinstance(sub, dict):
            continue

        enabled = bool(sub.get("enabled", False))
        mode_raw = str(sub.get("automation_mode", "OFF")).strip().upper()
        account_label = str(sub.get("account_label") or "").strip()

        if not enabled or mode_raw != "LEARN_DRY" or not account_label:
            continue

        labels.add(account_label)

        syms = sub.get("symbols") or []
        if not isinstance(syms, list):
            syms = []

        sset = set()
        for s in syms:
            if not s:
                continue
            sym = str(s).upper().strip()
            if sym:
                sset.add(sym)
                all_symbols.add(sym)

        if account_label not in label_to_symbols:
            label_to_symbols[account_label] = set()
        label_to_symbols[account_label].update(sset)

    labels_list = sorted(labels)
    return labels_list, label_to_symbols, all_symbols


# ---------------------------------------------------------------------------
# Broker cache
# ---------------------------------------------------------------------------

_BROKERS: Dict[str, PaperBroker] = {}


def _get_broker(account_label: str, starting_equity: float = 1000.0) -> PaperBroker:
    broker = _BROKERS.get(account_label)
    if broker is not None:
        return broker

    broker = PaperBroker.load_or_create(
        account_label=account_label,
        starting_equity=starting_equity,
    )
    _BROKERS[account_label] = broker
    return broker


# ---------------------------------------------------------------------------
# Price extraction
# ---------------------------------------------------------------------------

def _extract_last_price(trade: Dict[str, Any]) -> float:
    if not isinstance(trade, dict):
        return 0.0
    price = trade.get("p") or trade.get("price")
    if price is None:
        return 0.0
    try:
        return float(price)
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run_loop(
    sleep_seconds: float = 0.5,
    stale_cutoff_sec: float = 5.0,
    reload_every_sec: float = 30.0,
) -> None:
    """
    - reload_every_sec: periodically reload strategies.yaml so config edits apply live.
    """
    labels, label_to_symbols, all_symbols = _load_learn_dry_map()
    if not labels:
        log.warning("No LEARN_DRY strategies discovered; nothing to do.")
        return
    if not all_symbols:
        log.warning("LEARN_DRY strategies found (%s) but no symbols; nothing to do.", labels)
        return

    log.info(
        "Paper Tick Daemon starting (ROOT=%s) LEARN_DRY labels=%s symbols=%s",
        ROOT,
        labels,
        sorted(all_symbols),
    )

    # Warm up brokers (so you see ledger summaries once)
    for lbl in labels:
        _get_broker(lbl)

    last_reload = time.time()

    while True:
        try:
            record_heartbeat("paper_tick_daemon")
        except Exception:
            pass

        # Periodic reload
        now = time.time()
        if (now - last_reload) >= reload_every_sec:
            new_labels, new_map, new_all = _load_learn_dry_map()
            if new_labels and new_all:
                labels, label_to_symbols, all_symbols = new_labels, new_map, new_all
            last_reload = now

        # Stale-trades guard
        try:
            age = trades_bus_age_sec()
        except Exception:
            age = None

        if age is not None and age > stale_cutoff_sec:
            log.warning(
                "trades_bus stale (age=%.2fs > cutoff=%.2fs); skipping tick.",
                age, stale_cutoff_sec,
            )
            time.sleep(sleep_seconds)
            continue

        # Update per label, only symbols that label trades, only if it has open positions
        for lbl in labels:
            try:
                broker = _get_broker(lbl)
            except Exception as e:
                log.warning("Failed to get broker for %s: %r", lbl, e)
                continue

            try:
                if not broker.list_open_positions():
                    continue
            except Exception:
                # If list_open_positions fails, don't brick the daemon
                pass

            symset = label_to_symbols.get(lbl) or set()
            if not symset:
                continue

            for sym in symset:
                try:
                    trades = get_recent_trades(sym, limit=1)
                except Exception as e:
                    log.warning("get_recent_trades failed for %s: %r", sym, e)
                    continue
                if not trades:
                    continue
                price = _extract_last_price(trades[-1])
                if price <= 0:
                    continue
                try:
                    broker.update_price(sym, price)
                except Exception as e:
                    log.warning("PaperBroker.update_price failed for %s %s: %r", lbl, sym, e)

        time.sleep(sleep_seconds)


def main() -> None:
    """
    Entrypoint:
        python -m app.bots.paper_tick_daemon
    """
    try:
        run_loop()
    except KeyboardInterrupt:
        log.info("paper_tick_daemon interrupted; exiting.")


if __name__ == "__main__":
    main()
