#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Executor Dry-Run Replayer

Purpose
-------
Feed historical signals into the Auto Executor pipeline in **PAPER mode only**.

- Reads a JSONL file of signals (same schema as signals/observed.jsonl).
- For each signal:
    • Finds matching strategies via strategy_gate.get_strategies_for_signal(...)
    • Forces automation_mode="LEARN_DRY" (no live orders)
    • Calls executor_v2.handle_strategy_signal(...) so:
        - AI gate runs
        - correlation gate runs
        - sizing & risk logic runs
        - feature logging & AI events run
    • BUT entries stay PAPER, because automation_mode is not LIVE_*.

Usage
-----
    python -m app.tools.executor_replay \
        --file signals/observed.jsonl \
        --max-lines 10000

Environment variables
---------------------
    REPLAY_SIGNAL_FILE   : default path to signals file if --file is omitted.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from app.core.config import settings

# Strategy mapping
from app.core.strategy_gate import get_strategies_for_signal

# Reuse the real executor's strategy handler
from app.bots.executor_v2 import handle_strategy_signal  # type: ignore


# ---------- Logger (simple) ---------- #

try:
    from app.core.logger import get_logger  # type: ignore
except Exception:
    import logging

    def get_logger(name: str):  # type: ignore
        logger_ = logging.getLogger(name)
        if not logger_.handlers:
            h = logging.StreamHandler()
            fmt = logging.Formatter(
                "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
            )
            h.setFormatter(fmt)
            logger_.addHandler(h)
        logger_.setLevel(logging.INFO)
        return logger_


log = get_logger("executor_replay")

ROOT: Path = settings.ROOT


def _normalize_strategies_for_signal(
    strategies: Any,
) -> Iterable[Tuple[str, Dict[str, Any]]]:
    """
    Normalize whatever get_strategies_for_signal(...) returns into
    an iterable of (strat_name, strat_cfg) pairs.

    Copied from executor_v2, but kept local to avoid import of private symbol.
    """
    if not strategies:
        return []

    if isinstance(strategies, dict):
        return strategies.items()

    if isinstance(strategies, (list, tuple)):
        if not strategies:
            return []

        first = strategies[0]

        if isinstance(first, (list, tuple)) and len(first) == 2:
            return strategies

        if isinstance(first, dict):
            out: List[Tuple[str, Dict[str, Any]]] = []
            for cfg in strategies:
                if not isinstance(cfg, dict):
                    continue
                name = (
                    cfg.get("name")
                    or cfg.get("id")
                    or cfg.get("label")
                    or cfg.get("strategy_name")
                    or "unnamed_strategy"
                )
                out.append((str(name), cfg))
            return out

        return []

    return []


def _iter_lines(path: Path, max_lines: int | None = None):
    """
    Yield decoded, stripped lines from a JSONL file.
    """
    count = 0
    with path.open("rb") as f:
        for raw in f:
            try:
                line = raw.decode("utf-8").strip()
            except Exception:
                continue
            if not line:
                continue
            yield line
            count += 1
            if max_lines is not None and count >= max_lines:
                break


async def _replay_signal_line(line: str) -> None:
    """
    Parse one JSON signal and route it through executor_v2.handle_strategy_signal,
    forcing PAPER mode by overriding automation_mode.
    """
    try:
        sig = json.loads(line)
    except Exception:
        log.warning("invalid JSON in replay file: %r", line[:200])
        return

    symbol = sig.get("symbol")
    tf = sig.get("timeframe") or sig.get("tf")
    if not symbol or not tf:
        return

    strategies = get_strategies_for_signal(symbol, tf)
    strat_items = _normalize_strategies_for_signal(strategies)

    if not strat_items:
        return

    for strat_name, strat_cfg in strat_items:
        if not isinstance(strat_cfg, dict):
            continue

        # Shallow copy so we don't mutate the global registry
        cfg_copy = dict(strat_cfg)

        # Force strategy enabled + PAPER mode
        cfg_copy["enabled"] = True
        cfg_copy["automation_mode"] = "LEARN_DRY"

        try:
            await handle_strategy_signal(strat_name, cfg_copy, sig)
        except Exception as e:
            log.exception("replay: strategy error (%s): %r", strat_name, e)


async def replay_file(path: Path, max_lines: int | None = None) -> None:
    """
    Main replay function:
      - Iterates over signals in 'path'
      - Feeds each line into _replay_signal_line
    """
    if not path.exists():
        log.error("Replay file does not exist: %s", path)
        return

    log.info("Starting executor dry-run replay from %s", path)
    if max_lines is not None:
        log.info("Limiting to max_lines=%s", max_lines)

    t0 = time.time()
    count = 0

    # Lazy import so asyncio is only needed when actually running replay
    import asyncio

    for line in _iter_lines(path, max_lines=max_lines):
        await _replay_signal_line(line)
        count += 1
        if count % 100 == 0:
            await asyncio.sleep(0)  # cooperative yield

    dt = time.time() - t0
    log.info("Replay complete: %s lines processed in %.2fs", count, dt)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Flashback executor dry-run replayer")
    parser.add_argument(
        "--file",
        type=str,
        default=os.getenv("REPLAY_SIGNAL_FILE", "signals/observed.jsonl"),
        help="Path to signals JSONL file (default: signals/observed.jsonl or REPLAY_SIGNAL_FILE)",
    )
    parser.add_argument(
        "--max-lines",
        type=int,
        default=None,
        help="Optional max number of lines to replay (default: all)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    if argv is None:
        argv = sys.argv[1:]

    args = parse_args(argv)
    path = Path(args.file)
    max_lines = args.max_lines

    import asyncio

    asyncio.run(replay_file(path, max_lines=max_lines))


if __name__ == "__main__":
    main()
