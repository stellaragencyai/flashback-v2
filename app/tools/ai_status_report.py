#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Status Reporter (console + Telegram)

Role
----
Read:
    state/ai_metrics/strategies_snapshot.json

Render:
    - Clean console table for quick inspection
    - Compact Telegram summary via notifier_bot.tg_send(...)

This is a push-style status. Later you can plug it into a scheduler
(Windows Task Scheduler, cron, etc.) for daily reports.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

try:
    from app.core.config import settings  # type: ignore
    ROOT: Path = settings.ROOT  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

SNAPSHOT_PATH = ROOT / "state" / "ai_metrics" / "strategies_snapshot.json"

# Logger
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
            return logger_

log = get_logger("ai_status_report")

# Telegram notifier (optional)
try:
    from app.core.notifier_bot import tg_send  # type: ignore
except Exception:
    def tg_send(msg: str) -> None:  # type: ignore[override]
        log.info("tg_send stub: %s", msg)


def _load_snapshot() -> Dict[str, Any]:
    if not SNAPSHOT_PATH.exists():
        raise FileNotFoundError(f"Snapshot not found: {SNAPSHOT_PATH}")
    data = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8") or "{}")
    return data


def _format_console_table(strategies: List[Dict[str, Any]]) -> str:
    """
    Build a simple ascii table for console output.
    """
    headers = [
        "Strategy",
        "Account",
        "Mode",
        "Trades",
        "Win%",
        "Avg R",
        "MaxDD R",
        "AI score",
        "Rating",
    ]
    rows: List[List[str]] = []

    for s in strategies:
        name = str(s.get("strategy_name") or "?")
        acc = str(s.get("account_label") or "?")
        mode = str(s.get("mode") or "PAPER")

        n_trades = int(s.get("n_trades") or 0)
        win_rate = float(s.get("win_rate") or 0.0) * 100.0
        avg_R = float(s.get("avg_R") or 0.0)
        max_dd_R = float(s.get("max_dd_R") or 0.0)
        ai_avg = s.get("ai_avg_score")
        score = int(s.get("score") or 0)
        tier = str(s.get("tier") or "D")

        rows.append(
            [
                name,
                acc,
                mode,
                str(n_trades),
                f"{win_rate:5.1f}%",
                f"{avg_R:+.2f}",
                f"{max_dd_R:+.2f}",
                f"{ai_avg:.2f}" if isinstance(ai_avg, (int, float)) else "n/a",
                f"{score:3d} ({tier})",
            ]
        )

    # Column widths
    cols = list(zip(headers, *rows)) if rows else [(h,) for h in headers]
    widths = [max(len(str(cell)) for cell in col) for col in cols]

    def fmt_row(cells: List[str]) -> str:
        return " | ".join(
            str(cell).ljust(widths[i]) for i, cell in enumerate(cells)
        )

    line = "-+-".join("-" * w for w in widths)
    out_lines = [
        fmt_row(headers),
        line,
    ]
    for r in rows:
        out_lines.append(fmt_row(r))

    return "\n".join(out_lines)


def _format_telegram_summary(strategies: List[Dict[str, Any]]) -> str:
    """
    Build a compact Telegram-friendly summary.
    """
    total_strats = len(strategies)
    total_trades = sum(int(s.get("n_trades") or 0) for s in strategies)

    # Top 3 by score
    top = sorted(
        strategies,
        key=lambda s: int(s.get("score") or 0),
        reverse=True,
    )[:3]

    bad = [
        s
        for s in strategies
        if int(s.get("score") or 0) < 55 and int(s.get("n_trades") or 0) >= 20
    ][:3]

    lines: List[str] = []
    lines.append("📊 *AI Strategy Training Status*")
    lines.append("")
    lines.append(f"Total strategies: *{total_strats}*")
    lines.append(f"Total paper trades (all-time): *{total_trades}*")
    lines.append("")

    if top:
        lines.append("🥇 *Top strategies*")
        for s in top:
            name = str(s.get("strategy_name") or "?")
            acc = str(s.get("account_label") or "?")
            score = int(s.get("score") or 0)
            tier = str(s.get("tier") or "D")
            n_trades = int(s.get("n_trades") or 0)
            win_rate = float(s.get("win_rate") or 0.0) * 100.0
            avg_R = float(s.get("avg_R") or 0.0)
            lines.append(
                f"• *{name}* ({acc}) — *{score}* ({tier})\n"
                f"  Trades: {n_trades} | Win: {win_rate:.1f}% | Avg R: {avg_R:+.2f}"
            )
        lines.append("")

    if bad:
        lines.append("🧊 *Under review*")
        for s in bad:
            name = str(s.get("strategy_name") or "?")
            acc = str(s.get("account_label") or "?")
            score = int(s.get("score") or 0)
            n_trades = int(s.get("n_trades") or 0)
            avg_R = float(s.get("avg_R") or 0.0)
            lines.append(
                f"• *{name}* ({acc}) — {score} pts, {n_trades} trades, Avg R: {avg_R:+.2f}"
            )

    if not top and not bad:
        lines.append("_No trades yet. Models are still starving._")

    return "\n".join(lines)


def main() -> None:
    try:
        snapshot = _load_snapshot()
    except FileNotFoundError as e:
        log.error("%s", e)
        return

    strategies = snapshot.get("strategies") or []
    if not strategies:
        log.info("No strategy metrics found in snapshot.")
        return

    # Console table
    table = _format_console_table(strategies)
    print(table)

    # Telegram summary
    try:
        msg = _format_telegram_summary(strategies)
        tg_send(msg)
        log.info("Sent AI status summary to Telegram.")
    except Exception as e:
        log.warning("Failed to send Telegram summary: %r", e)


if __name__ == "__main__":
    main()
