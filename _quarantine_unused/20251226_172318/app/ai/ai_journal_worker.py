#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Journal Worker

Purpose
-------
Background worker that listens to the in-process AI events bus and
persists events to JSONL files for later training / analysis.

Events currently supported (see app.ai.ai_events):
    - SetupContext   (event_type="setup_context")
    - OutcomeRecord  (event_type="outcome_record")

Flow:
    app.ai.ai_events.build_setup_context / build_outcome_record
        → publish_ai_event(...)
        → ai_events_bus (deque in app.core.bus_types)
        → ai_journal_worker (this file)
        → state/ai_events/setups.jsonl / outcomes.jsonl

Notes
-----
- Right now logs are global (all account_labels into the same files).
- You can later shard per-account by adjusting _handle_setup_context /
  _handle_outcome_record to route on 'account_label'.
"""

from __future__ import annotations

import os
import signal
import time
from pathlib import Path
from typing import Any, Deque, Dict

import orjson

# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------

try:
    from app.core.logger import get_logger  # type: ignore
except Exception:
    import logging

    def get_logger(name: str) -> "logging.Logger":  # type: ignore
        logger_ = logging.getLogger(name)
        if not logger_.handlers:
            handler = logging.StreamHandler()
            fmt = logging.Formatter(
                "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
            )
            handler.setFormatter(fmt)
            logger_.addHandler(handler)
        logger_.setLevel(logging.INFO)
        return logger_


log = get_logger("ai_journal_worker")

# ---------------------------------------------------------------------------
# Config / paths
# ---------------------------------------------------------------------------

try:
    from app.core.config import settings  # type: ignore
    ROOT: Path = settings.ROOT
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

STATE_DIR: Path = ROOT / "state" / "ai_events"
STATE_DIR.mkdir(parents=True, exist_ok=True)

SETUPS_PATH: Path = STATE_DIR / "setups.jsonl"
OUTCOMES_PATH: Path = STATE_DIR / "outcomes.jsonl"

HEARTBEAT_PATH: Path = ROOT / "state" / "ai_journal_heartbeat.txt"

# ---------------------------------------------------------------------------
# AI events bus import
# ---------------------------------------------------------------------------

try:
    # Expected: ai_events_bus is a deque of dict events
    from app.core.bus_types import ai_events_bus  # type: ignore
    AI_EVENTS_BUS: Deque[Dict[str, Any]] = ai_events_bus  # type: ignore
except Exception as e:  # pragma: no cover
    log.error("Failed to import ai_events_bus from app.core.bus_types: %r", e)
    AI_EVENTS_BUS = None  # type: ignore


def _write_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    try:
        with path.open("ab") as f:
            f.write(orjson.dumps(payload) + b"\n")
    except Exception as e:
        log.error("Failed to append to %s: %r", path, e)


def _handle_setup_context(ev: Dict[str, Any]) -> None:
    """
    Handle a 'setup_context' event.

    Guardrails:
    - Drop events where setup_type is 'unknown' (top-level or nested) to prevent
      taxonomy-unknown rows from polluting training data.
    """
    # Determine setup_type (flexible shapes)
    st = (
        ev.get("setup_type")
        or ((ev.get("payload") or {}).get("setup_type") if isinstance(ev.get("payload"), dict) else None)
        or (((ev.get("payload") or {}).get("features") or {}).get("setup_type") if isinstance((ev.get("payload") or {}).get("features"), dict) else None)
    )
    st_s = str(st or "").strip().lower()

    if st_s == "unknown":
        try:
            tid = ev.get("trade_id")
            sym = ev.get("symbol")
            acct = ev.get("account_label")
            log.warning("journal_drop_unknown_setup: trade_id=%s symbol=%s account=%s", tid, sym, acct)
        except Exception:
            pass
        return

    payload = {
        "ts": ev.get("ts") or int(time.time() * 1000),
        "trade_id": ev.get("trade_id"),
        "symbol": ev.get("symbol"),
        "account_label": ev.get("account_label"),
        "strategy": ev.get("strategy"),
        "data": ev.get("payload") or ev,
    }
    _write_jsonl(SETUPS_PATH, payload)
def _handle_outcome_record(ev: Dict[str, Any]) -> None:
    """
    Handle an 'outcome_record' event.

    Expected shape (flexible):
        {
          "event_type": "outcome_record",
          "ts": <epoch_ms>,
          "trade_id": "...",
          "symbol": "...",
          "account_label": "...",
          "payload": { ... outcome / pnl / stats ... }
        }
    """
    payload = {
        "ts": ev.get("ts") or int(time.time() * 1000),
        "trade_id": ev.get("trade_id"),
        "symbol": ev.get("symbol"),
        "account_label": ev.get("account_label"),
        "strategy": ev.get("strategy"),
        "data": ev.get("payload") or ev,
    }
    _write_jsonl(OUTCOMES_PATH, payload)


def _write_heartbeat() -> None:
    try:
        HEARTBEAT_PATH.write_text(str(int(time.time() * 1000)), encoding="utf-8")
    except Exception as e:
        log.error("Failed to write heartbeat: %r", e)


def _drain_one_event(ev: Dict[str, Any]) -> None:
    etype = str(ev.get("event_type") or ev.get("type") or "").lower()
    if etype == "setup_context":
        _handle_setup_context(ev)
    elif etype == "outcome_record":
        _handle_outcome_record(ev)
    else:
        log.debug("Unknown AI event_type=%r, ignoring", etype)


def _install_signal_handlers(stop_flag: Dict[str, bool]) -> None:
    def _handler(signum, frame):  # type: ignore[override]
        log.info("Signal %s received, stopping AI journal worker.", signum)
        stop_flag["stop"] = True

    try:
        signal.signal(signal.SIGINT, _handler)
        signal.signal(signal.SIGTERM, _handler)
    except Exception as e:  # pragma: no cover
        log.warning("Could not install signal handlers: %r", e)


def main_loop(poll_sleep: float = 0.25) -> None:
    if AI_EVENTS_BUS is None:
        log.error("AI_EVENTS_BUS is None; nothing to consume. Exiting.")
        return

    log.info("AI Journal Worker started. Writing to %s", STATE_DIR)
    stop_flag: Dict[str, bool] = {"stop": False}
    _install_signal_handlers(stop_flag)

    last_hb = 0.0

    while not stop_flag["stop"]:
        try:
            # Heartbeat once per second
            now = time.time()
            if now - last_hb >= 1.0:
                _write_heartbeat()
                last_hb = now

            drained = 0
            while True:
                try:
                    ev = AI_EVENTS_BUS.popleft()
                except IndexError:
                    break

                if not isinstance(ev, dict):
                    log.debug("Ignoring non-dict AI event: %r", ev)
                    continue

                _drain_one_event(ev)
                drained += 1

            if drained == 0:
                time.sleep(poll_sleep)
        except Exception as e:
            log.exception("Error in AI journal loop: %r", e)
            time.sleep(0.5)

    log.info("AI Journal Worker exiting.")


def main() -> None:
    main_loop()


if __name__ == "__main__":
    main()
