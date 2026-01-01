#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Paper Price Feeder (LEARN_DRY closer, v1.1)

Purpose
-------
PaperBroker only closes positions if somebody feeds it prices.
This worker reads WS-fed market bus snapshots and drives PaperBroker.update_price()
so paper trades hit TP/SL and emit OutcomeRecord events.

Inputs (read-only):
  - state/paper/<account_label>.json                 (paper ledgers)
  - state/trades_bus.json                            (WS trades snapshot)
  - state/orderbook_bus.json                         (WS orderbook snapshot)
  - config/strategies.yaml                           (to validate labels / profiles via PaperBroker)

Outputs:
  - state/ai_events/outcomes_raw.jsonl               (via ai_events_spine)
  - state/ai_events/outcomes.jsonl                   (via ai_events_spine)
  - state/ai_events/pending_setups.json              (setup registry maintained elsewhere)

Behavior
--------
- Scans for open paper positions across all paper ledgers.
- Builds a unique symbol set.
- For each symbol:
    price = last trade price (trades_bus) OR midprice from best bid/ask (orderbook)
- Calls broker.update_price(symbol, price) for every ledger that has that symbol open.
- Gates on WS freshness so you don't "learn" from stale buses.

Env
---
PAPER_FEEDER_ENABLED            (default: "true")
PAPER_FEEDER_POLL_MS            (default: "500")
PAPER_FEEDER_MAX_SYMBOLS        (default: "200")   # safety cap
PAPER_FEEDER_MAX_ACCOUNTS       (default: "50")    # safety cap

# Staleness guardrails (seconds)
PAPER_FEEDER_MAX_TRADES_AGE_SEC (default: "5.0")
PAPER_FEEDER_MAX_OB_AGE_SEC     (default: "10.0")

# Behavior toggles
PAPER_FEEDER_REQUIRE_TRADES     (default: "false") # if true, will NOT use orderbook mid fallback
PAPER_FEEDER_DEBUG_LOG          (default: "false")
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    from app.core.config import settings  # type: ignore
    ROOT: Path = settings.ROOT  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

# Logging (robust)
try:
    from app.core.log import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging
    import sys

    def get_logger(name: str) -> "logging.Logger":  # type: ignore
        logger_ = logging.getLogger(name)
        if not logger_.handlers:
            handler = logging.StreamHandler(sys.stdout)
            fmt = logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
            handler.setFormatter(fmt)
            logger_.addHandler(handler)
        logger_.setLevel(logging.INFO)
        return logger_

log = get_logger("paper_price_feeder")

# Heartbeat
try:
    from app.core.flashback_common import record_heartbeat  # type: ignore
except Exception:  # pragma: no cover
    def record_heartbeat(name: str) -> None:  # type: ignore[override]
        return None

# Market bus (WS snapshots) - hardened import (no reliance on __init__.py exports)
try:
    import app.core.market_bus as market_bus  # type: ignore
except Exception as e:  # pragma: no cover
    raise ImportError(f"Failed to import app.core.market_bus: {e}")

# Paper broker
try:
    from app.sim.paper_broker import PaperBroker  # type: ignore
except Exception as e:  # pragma: no cover
    raise ImportError(f"Failed to import app.sim.paper_broker: {e}")


def _env_bool(name: str, default: str = "false") -> bool:
    raw = os.getenv(name, default).strip().lower()
    return raw in ("1", "true", "yes", "y", "on")


def _env_int(name: str, default: str) -> int:
    try:
        return int(os.getenv(name, default).strip())
    except Exception:
        return int(default)


def _env_float(name: str, default: str) -> float:
    try:
        return float(os.getenv(name, default).strip())
    except Exception:
        return float(default)


PAPER_DIR: Path = ROOT / "state" / "paper"
PAPER_DIR.mkdir(parents=True, exist_ok=True)

ENABLED: bool = _env_bool("PAPER_FEEDER_ENABLED", "true")
POLL_MS: int = max(100, _env_int("PAPER_FEEDER_POLL_MS", "500"))
MAX_SYMBOLS: int = max(10, _env_int("PAPER_FEEDER_MAX_SYMBOLS", "200"))
MAX_ACCOUNTS: int = max(1, _env_int("PAPER_FEEDER_MAX_ACCOUNTS", "50"))

MAX_TRADES_AGE_SEC: float = max(0.5, _env_float("PAPER_FEEDER_MAX_TRADES_AGE_SEC", "5.0"))
MAX_OB_AGE_SEC: float = max(1.0, _env_float("PAPER_FEEDER_MAX_OB_AGE_SEC", "10.0"))

REQUIRE_TRADES: bool = _env_bool("PAPER_FEEDER_REQUIRE_TRADES", "false")
DEBUG_LOG: bool = _env_bool("PAPER_FEEDER_DEBUG_LOG", "false")


def _list_paper_ledgers() -> List[Path]:
    if not PAPER_DIR.exists():
        return []
    files = sorted([p for p in PAPER_DIR.glob("*.json") if p.is_file()])
    if len(files) > MAX_ACCOUNTS:
        files = files[:MAX_ACCOUNTS]
    return files


def _extract_open_symbols_from_ledger_json(path: Path) -> Set[str]:
    """
    Read the paper ledger JSON and return set of symbols with open positions.
    We parse JSON here so we can avoid instantiating PaperBroker for accounts
    that have no open positions.
    """
    try:
        txt = path.read_text(encoding="utf-8")
    except Exception:
        return set()

    try:
        import json
        data = json.loads(txt or "{}")
    except Exception:
        return set()

    out: Set[str] = set()
    opens = data.get("open_positions") or []
    if not isinstance(opens, list):
        return out

    for p in opens:
        if not isinstance(p, dict):
            continue
        sym = p.get("symbol")
        if sym:
            out.add(str(sym).upper().strip())
    return out


def _ws_fresh_enough() -> Tuple[bool, str]:
    """
    Staleness guard: if both buses are ancient, don't drive paper fills.
    """
    tr_age = market_bus.trades_bus_age_sec()
    ob_age = market_bus.orderbook_bus_age_sec()

    # If trades are required, enforce trades bus freshness strictly.
    if REQUIRE_TRADES:
        if tr_age is None:
            return False, "trades_bus_age unknown (require_trades=true)"
        if tr_age > MAX_TRADES_AGE_SEC:
            return False, f"trades_bus stale ({tr_age:.2f}s > {MAX_TRADES_AGE_SEC:.2f}s)"
        return True, "ok"

    # Otherwise, allow either trades OR orderbook to be fresh enough.
    trades_ok = (tr_age is not None and tr_age <= MAX_TRADES_AGE_SEC)
    ob_ok = (ob_age is not None and ob_age <= MAX_OB_AGE_SEC)

    if trades_ok or ob_ok:
        return True, "ok"

    return False, f"buses stale (trades_age={tr_age}, ob_age={ob_age})"


def _price_from_trades(symbol: str) -> Optional[float]:
    """
    Try to get a last trade price from trades_bus snapshot.
    """
    trades = market_bus.get_recent_trades(symbol, limit=1)
    if not trades:
        return None

    t = trades[-1]
    # Bybit trade payloads vary; common keys: price / p / execPrice
    for k in ("price", "p", "execPrice", "exec_price", "lastPrice", "last_price"):
        v = t.get(k)
        if v is None:
            continue
        try:
            px = float(v)
            if px > 0:
                return px
        except Exception:
            continue

    return None


def _price_from_orderbook_mid(symbol: str) -> Optional[float]:
    """
    Fallback: midprice from best bid/ask.
    """
    bid, ask = market_bus.best_bid_ask(symbol)
    if bid is None or ask is None:
        return None
    try:
        mid = (float(bid) + float(ask)) / 2.0
        if mid > 0:
            return mid
    except Exception:
        return None
    return None


def _get_ws_first_price(symbol: str) -> Optional[float]:
    """
    WS-first price selection:
      1) last trade price
      2) orderbook mid (unless REQUIRE_TRADES)
    """
    px = _price_from_trades(symbol)
    if px is not None:
        return px

    if REQUIRE_TRADES:
        return None

    return _price_from_orderbook_mid(symbol)


def loop() -> None:
    if not ENABLED:
        log.warning("Paper Price Feeder disabled via PAPER_FEEDER_ENABLED=false. Exiting.")
        return

    log.info(
        "Paper Price Feeder starting (poll=%dms, max_symbols=%d, max_accounts=%d, "
        "max_trades_age=%.2fs, max_ob_age=%.2fs, require_trades=%s)",
        POLL_MS,
        MAX_SYMBOLS,
        MAX_ACCOUNTS,
        MAX_TRADES_AGE_SEC,
        MAX_OB_AGE_SEC,
        REQUIRE_TRADES,
    )

    broker_cache: Dict[str, PaperBroker] = {}

    while True:
        record_heartbeat("paper_price_feeder")
        t0 = time.time()

        ok, reason = _ws_fresh_enough()
        if not ok:
            if DEBUG_LOG:
                log.info("WS stale, skipping tick: %s", reason)
            time.sleep(max(0.25, POLL_MS / 1000.0))
            continue

        ledger_files = _list_paper_ledgers()
        if not ledger_files:
            time.sleep(max(0.25, POLL_MS / 1000.0))
            continue

        # Build account->symbols map
        acct_symbols: Dict[str, Set[str]] = {}
        all_symbols: Set[str] = set()

        for fp in ledger_files:
            account_label = fp.stem
            syms = _extract_open_symbols_from_ledger_json(fp)
            if not syms:
                continue
            acct_symbols[account_label] = syms
            all_symbols.update(syms)

        if not all_symbols:
            time.sleep(max(0.25, POLL_MS / 1000.0))
            continue

        symbols_list = sorted(list(all_symbols))
        if len(symbols_list) > MAX_SYMBOLS:
            symbols_list = symbols_list[:MAX_SYMBOLS]

        # Compute prices once per symbol
        price_map: Dict[str, float] = {}
        for sym in symbols_list:
            px = _get_ws_first_price(sym)
            if px is None:
                continue
            price_map[sym] = px

        if not price_map:
            if DEBUG_LOG:
                log.info("No prices available this tick (symbols=%d).", len(symbols_list))
            time.sleep(max(0.25, POLL_MS / 1000.0))
            continue

        # Feed prices to each broker that has open positions in those symbols
        closes = 0
        for account_label, syms in acct_symbols.items():
            broker = broker_cache.get(account_label)
            if broker is None:
                try:
                    broker = PaperBroker.load_or_create(account_label, starting_equity=1000.0)
                    broker_cache[account_label] = broker
                except Exception as e:
                    log.warning("Failed to load paper broker for %s: %r", account_label, e)
                    continue

            for sym in syms:
                px = price_map.get(sym)
                if px is None:
                    continue
                try:
                    before_open = len(broker.list_open_positions())
                    broker.update_price(sym, px)
                    after_open = len(broker.list_open_positions())
                    if after_open < before_open:
                        closes += (before_open - after_open)
                except Exception as e:
                    log.warning("update_price failed (%s %s @ %s): %r", account_label, sym, px, e)

        if DEBUG_LOG:
            log.info(
                "tick done: accounts=%d symbols=%d prices=%d closes=%d age_ok=%s",
                len(acct_symbols),
                len(symbols_list),
                len(price_map),
                closes,
                reason,
            )

        elapsed = time.time() - t0
        sleep_sec = max(0.10, (POLL_MS / 1000.0) - elapsed)
        time.sleep(sleep_sec)


def main() -> None:
    loop()


if __name__ == "__main__":
    main()
