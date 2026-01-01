#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Paper Price Daemon v2.0

Role
----
Tail state/public_trades.jsonl and feed prices into PaperBroker so that
PAPER positions (LEARN_DRY / EXEC_DRY_RUN) can actually close on TP/SL.

Behavior
--------
- Tracks a cursor in: state/public_trades.cursor
- For each new line in state/public_trades.jsonl:
    • Parse JSON: { "version": 1, "received_ms": ..., "symbol": "BTCUSDT", "trade": {...} }
    • Extract price from trade["p"] (Bybit publicTrade), falling back to a few other keys
    • Refresh the list of known paper ledgers from state/paper/*.json
    • For EACH PaperBroker (per account_label), call broker.update_price(symbol, price)

This is multi-account aware:
    - Any ledger at state/paper/<account_label>.json will be respected.
    - You already have: flashback02, flashback03, flashback07, flashback09, etc.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Dict, Any

from app.sim.paper_broker import PaperBroker  # type: ignore

try:
    from app.core.config import settings  # type: ignore
    ROOT: Path = settings.ROOT  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

# Robust logger import
try:
    from app.core.logger import get_logger  # type: ignore
except Exception:
    try:
        from app.core.log import get_logger  # type: ignore
    except Exception:  # pragma: no cover
        import logging
        import sys

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
log = get_logger("paper_price_daemon")

# Heartbeat (optional)

# Heartbeat (optional)
try:
    from app.core.flashback_common import record_heartbeat  # type: ignore
except Exception:  # pragma: no cover
    def record_heartbeat(name: str) -> None:  # type: ignore[override]
        return None


STATE_DIR: Path = ROOT / "state"
PUBLIC_TRADES_PATH: Path = STATE_DIR / "public_trades.jsonl"
CURSOR_PATH: Path = STATE_DIR / "public_trades.cursor"
PAPER_DIR: Path = STATE_DIR / "paper"

# Cache of account_label -> PaperBroker
_BROKERS: Dict[str, PaperBroker] = {}


# ---------------------------------------------------------------------------
# Cursor helpers
# ---------------------------------------------------------------------------

def load_cursor() -> int:
    if not CURSOR_PATH.exists():
        return 0
    try:
        txt = CURSOR_PATH.read_text(encoding="utf-8").strip()
        return int(txt or "0")
    except Exception:
        return 0


def save_cursor(pos: int) -> None:
    try:
        CURSOR_PATH.write_text(str(pos), encoding="utf-8")
    except Exception as e:
        log.warning("paper_price_daemon: failed to save cursor %s: %r", pos, e)


# ---------------------------------------------------------------------------
# Broker discovery
# ---------------------------------------------------------------------------

def refresh_brokers() -> None:
    """
    Discover all paper ledgers in state/paper/*.json and ensure
    we have a PaperBroker instance per account_label.
    """
    if not PAPER_DIR.exists():
        return

    for path in PAPER_DIR.glob("*.json"):
        account_label = path.stem
        if account_label in _BROKERS:
            continue
        try:
            broker = PaperBroker.load_or_create(
                account_label=account_label,
                starting_equity=1000.0,  # used only on very first creation
            )
            _BROKERS[account_label] = broker
            log.info(
                "paper_price_daemon: discovered paper ledger for %s (equity=%.2f)",
                account_label,
                broker.equity,
            )
        except Exception as e:
            log.warning(
                "paper_price_daemon: failed to load/create broker for %s: %r",
                account_label,
                e,
            )


# ---------------------------------------------------------------------------
# Price extraction
# ---------------------------------------------------------------------------

def _extract_price(row: Dict[str, Any]) -> tuple[str | None, float | None]:
    """
    Extract (symbol, price) from a public_trades.jsonl row.

    Expected shape:
        {
          "version": 1,
          "received_ms": ...,
          "symbol": "BTCUSDT",
          "trade": {
              "p": "43000.5",
              ...
          }
        }
    """
    symbol = row.get("symbol")
    trade = row.get("trade") or {}
    if not isinstance(trade, dict):
        trade = {}

    # Bybit uses "p" (string) for price in publicTrade
    price_raw = (
        trade.get("p")
        or trade.get("price")
        or trade.get("lastPrice")
        or trade.get("fillPrice")
    )

    if symbol is None or price_raw is None:
        return None, None

    try:
        price_f = float(price_raw)
        if price_f <= 0:
            return symbol, None
        return symbol, price_f
    except Exception:
        return symbol, None


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

async def paper_price_loop() -> None:
    pos = load_cursor()
    log.info(
        "paper_price_daemon starting at cursor=%s, public_trades=%s",
        pos,
        PUBLIC_TRADES_PATH,
    )

    while True:
        try:
            record_heartbeat("paper_price_daemon")
        except Exception:
            pass

        if not PUBLIC_TRADES_PATH.exists():
            await asyncio.sleep(1.0)
            continue

        try:
            file_size = PUBLIC_TRADES_PATH.stat().st_size
        except Exception as e:
            log.warning("paper_price_daemon: stat() failed on public_trades: %r", e)
            await asyncio.sleep(1.0)
            continue

        # File truncated (rotation / manual cleanup)
        if pos > file_size:
            log.info(
                "paper_price_daemon: public_trades truncated (size=%s, cursor=%s). Resetting cursor to 0.",
                file_size,
                pos,
            )
            pos = 0
            save_cursor(pos)

        # Ensure brokers exist for all known paper ledgers
        refresh_brokers()

        if not _BROKERS:
            # Nothing to drive yet, but we still advance cursor so we don't backlog forever
            await asyncio.sleep(0.5)
            continue

        try:
            with PUBLIC_TRADES_PATH.open("rb") as f:
                f.seek(pos)
                for raw in f:
                    pos = f.tell()
                    try:
                        line = raw.decode("utf-8").strip()
                    except Exception as e:
                        log.warning(
                            "paper_price_daemon: failed to decode line at pos=%s: %r",
                            pos,
                            e,
                        )
                        continue

                    if not line:
                        continue

                    try:
                        row = json.loads(line)
                    except Exception:
                        # Silently skip corrupt JSON; WS will keep appending.
                        continue

                    if not isinstance(row, dict):
                        continue

                    symbol, price = _extract_price(row)
                    if not symbol or price is None:
                        continue

                    # Feed price to ALL paper brokers
                    for label, broker in list(_BROKERS.items()):
                        try:
                            broker.update_price(symbol, price)
                        except Exception as e:
                            log.warning(
                                "paper_price_daemon: update_price failed for %s/%s: %r",
                                label,
                                symbol,
                                e,
                            )

                save_cursor(pos)
        except Exception as e:
            log.exception(
                "paper_price_daemon loop error: %r; backing off 1s", e
            )
            await asyncio.sleep(1.0)

        await asyncio.sleep(0.25)


def main() -> None:
    try:
        asyncio.run(paper_price_loop())
    except KeyboardInterrupt:
        log.info("paper_price_daemon stopped by user")


if __name__ == "__main__":
    main()
