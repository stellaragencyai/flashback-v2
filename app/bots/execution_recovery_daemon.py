#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Execution Recovery Daemon (v1.1, read-only + lock flag)

Purpose
-------
Periodic reconciliation between:

  - Live positions (via position_bus.get_positions_snapshot)
  - Journal open trades (state/journal_open.json)

It detects anomalies such as:

  - Positions open on exchange but missing from journal_open ("orphan_position")
  - Journal entries that claim a trade is open but position size is 0 ("stale_journal_open")

It then:

  - Writes a summary report into: state/execution_recovery_report.json
  - Sends Telegram alerts on anomalies (once per run, summarized)
  - Records a heartbeat: "execution_recovery_daemon"
  - Maintains a lock file: state/execution_suspect.lock
      • created when anomalies exist
      • removed when they clear

Scope
-----
This v1 is intentionally **read-only**:

  - It does NOT modify Bybit positions.
  - It does NOT auto-close trades.
  - It does NOT mutate journal_open.json.

The lock file is a signalling mechanism for executor / supervisor:
  - If present, they should avoid placing new live entries.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

from app.core.config import settings

# Logging & Telegram
from app.core.logger import get_logger
from app.core.notifier_bot import tg_send

# Position bus → unified snapshot of open positions
from app.core.position_bus import get_positions_snapshot

# Heartbeat
from app.core.flashback_common import record_heartbeat

log = get_logger("execution_recovery_daemon")

ROOT: Path = settings.ROOT
STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

JOURNAL_OPEN_PATH: Path = STATE_DIR / "journal_open.json"
REPORT_PATH: Path = STATE_DIR / "execution_recovery_report.json"
SUSPECT_LOCK_PATH: Path = STATE_DIR / "execution_suspect.lock"

# How often to run reconciliation (seconds)
INTERVAL_SECONDS = 15

# How many anomalies to include in Telegram message before truncating
MAX_ALERT_ITEMS = 10


@dataclass(frozen=True)
class PositionKey:
    account_label: str
    symbol: str
    side: str  # "Buy" / "Sell"
    sub_uid: str


def _norm_side(s: Any) -> str:
    s_str = str(s or "").strip().lower()
    if s_str in ("buy", "long"):
        return "Buy"
    if s_str in ("sell", "short"):
        return "Sell"
    # Unknown is still something, but for keying we force one.
    return "Buy" if s_str == "" else s_str.capitalize()


def _norm_label(x: Any, default: str = "main") -> str:
    s = str(x or "").strip()
    return s if s else default


def _norm_sub_uid(p: Dict[str, Any]) -> str:
    v = (
        p.get("sub_uid")
        or p.get("subAccountId")
        or p.get("accountId")
        or p.get("subId")
        or ""
    )
    return str(v or "").strip()


def _load_positions() -> List[Dict[str, Any]]:
    """
    Pull open positions via position_bus. This prefers WS-fed positions_bus.json
    and falls back to REST for MAIN when needed (position_bus handles that).
    """
    try:
        positions = get_positions_snapshot(
            label=None,          # default ACCOUNT_LABEL inside position_bus
            category="linear",
            max_age_seconds=None,
            allow_rest_fallback=True,
        )
        if not isinstance(positions, list):
            return []
        return positions
    except Exception as e:
        log.warning("Failed to load positions from position_bus: %r", e)
        return []


def _load_journal_open() -> List[Dict[str, Any]]:
    """
    Load journal_open.json in a tolerant way.

    Supported shapes:
      - { "open_trades": [ ... ] }
      - [ ... ]
    """
    if not JOURNAL_OPEN_PATH.exists():
        return []

    try:
        with JOURNAL_OPEN_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        log.warning("Failed to read journal_open.json: %r", e)
        return []

    if isinstance(data, dict) and isinstance(data.get("open_trades"), list):
        return data["open_trades"]

    if isinstance(data, list):
        return data

    return []


def _position_key_from_position(p: Dict[str, Any]) -> PositionKey:
    symbol = str(p.get("symbol") or "").upper()
    side = _norm_side(p.get("side") or p.get("positionSide"))
    account_label = _norm_label(
        p.get("account_label") or p.get("label") or p.get("account_label_slug") or "main"
    )
    sub_uid = _norm_sub_uid(p)

    return PositionKey(
        account_label=account_label,
        symbol=symbol,
        side=side,
        sub_uid=sub_uid,
    )


def _position_key_from_journal(j: Dict[str, Any]) -> PositionKey:
    symbol = str(j.get("symbol") or "").upper()
    side = _norm_side(j.get("side") or j.get("positionSide"))
    account_label = _norm_label(
        j.get("account_label") or j.get("label") or j.get("account") or "main"
    )
    sub_uid = str(j.get("sub_uid") or j.get("subAccountId") or j.get("accountId") or "").strip()

    return PositionKey(
        account_label=account_label,
        symbol=symbol,
        side=side,
        sub_uid=sub_uid,
    )


def _size_from_position(p: Dict[str, Any]) -> float:
    size_raw = p.get("size") or p.get("positionQty") or p.get("qty") or 0
    try:
        return float(size_raw)
    except Exception:
        return 0.0


def reconcile_once() -> Dict[str, Any]:
    """
    Core reconciliation logic. Returns a dict summary that we also dump to JSON.
    """
    ts_ms = int(time.time() * 1000)

    # Load live positions
    positions = _load_positions()
    live_positions: Dict[PositionKey, Dict[str, Any]] = {}
    for p in positions:
        size = _size_from_position(p)
        if size <= 0:
            continue
        key = _position_key_from_position(p)
        live_positions[key] = {
            "symbol": p.get("symbol"),
            "side": _norm_side(p.get("side") or p.get("positionSide")),
            "size": size,
            "account_label": key.account_label,
            "sub_uid": key.sub_uid,
        }

    # Load journal open trades
    journal_rows = _load_journal_open()
    open_journal: Dict[PositionKey, Dict[str, Any]] = {}
    for j in journal_rows:
        key = _position_key_from_journal(j)
        open_journal[key] = {
            "trade_id": j.get("trade_id") or j.get("id"),
            "symbol": j.get("symbol"),
            "side": _norm_side(j.get("side") or j.get("positionSide")),
            "account_label": key.account_label,
            "sub_uid": key.sub_uid,
        }

    # Detect anomalies
    orphan_positions: List[Dict[str, Any]] = []      # live position but no journal_open
    stale_journal: List[Dict[str, Any]] = []         # journal_open but no live position

    for key, pos in live_positions.items():
        if key not in open_journal:
            orphan_positions.append(
                {
                    "account_label": key.account_label,
                    "sub_uid": key.sub_uid,
                    "symbol": key.symbol,
                    "side": key.side,
                    "size": pos.get("size"),
                    "reason": "position_open_but_not_in_journal_open",
                }
            )

    for key, jr in open_journal.items():
        if key not in live_positions:
            stale_journal.append(
                {
                    "account_label": key.account_label,
                    "sub_uid": key.sub_uid,
                    "symbol": key.symbol,
                    "side": key.side,
                    "trade_id": jr.get("trade_id"),
                    "reason": "journal_open_but_no_live_position",
                }
            )

    orphan_count = len(orphan_positions)
    stale_count = len(stale_journal)

    if orphan_count == 0 and stale_count == 0:
        severity = "ok"
    else:
        severity = "warn"

    summary: Dict[str, Any] = {
        "ts_ms": ts_ms,
        "live_positions_count": len(live_positions),
        "journal_open_count": len(open_journal),
        "orphan_positions_count": orphan_count,
        "stale_journal_count": stale_count,
        "orphan_positions": orphan_positions,
        "stale_journal": stale_journal,
        "severity": severity,
    }

    return summary


def _write_report(summary: Dict[str, Any]) -> None:
    try:
        with REPORT_PATH.open("w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, sort_keys=True)
    except Exception as e:
        log.warning("Failed to write execution_recovery_report.json: %r", e)


def _update_lock(summary: Dict[str, Any]) -> None:
    """
    Maintain state/execution_suspect.lock based on anomaly counts.

    - If there are any orphan or stale entries → create/update lock.
    - If there are none → remove lock if present.
    """
    orphan_count = int(summary.get("orphan_positions_count") or 0)
    stale_count = int(summary.get("stale_journal_count") or 0)

    if orphan_count > 0 or stale_count > 0:
        try:
            SUSPECT_LOCK_PATH.write_text(str(int(time.time() * 1000)), encoding="utf-8")
        except Exception as e:
            log.warning("Failed to write execution_suspect.lock: %r", e)
    else:
        if SUSPECT_LOCK_PATH.exists():
            try:
                SUSPECT_LOCK_PATH.unlink()
            except Exception as e:
                log.warning("Failed to remove execution_suspect.lock: %r", e)


def _alert_if_needed(summary: Dict[str, Any]) -> None:
    orphan_positions = summary.get("orphan_positions") or []
    stale_journal = summary.get("stale_journal") or []

    orphan_count = len(orphan_positions)
    stale_count = len(stale_journal)

    if orphan_count == 0 and stale_count == 0:
        # Nothing to complain about, life is briefly good.
        return

    lines: List[str] = []
    lines.append("⚠️ Execution Recovery Alert")

    if orphan_count > 0:
        lines.append(f"• Orphan positions (live but not in journal_open): {orphan_count}")
        for item in orphan_positions[:MAX_ALERT_ITEMS]:
            lines.append(
                f"    - {item.get('account_label')} / sub={item.get('sub_uid') or 'main'} "
                f"{item.get('symbol')} {item.get('side')} size={item.get('size')}"
            )
        if orphan_count > MAX_ALERT_ITEMS:
            lines.append(f"    ... +{orphan_count - MAX_ALERT_ITEMS} more")

    if stale_count > 0:
        lines.append(f"• Stale journal entries (journal_open but no live position): {stale_count}")
        for item in stale_journal[:MAX_ALERT_ITEMS]:
            lines.append(
                f"    - {item.get('account_label')} / sub={item.get('sub_uid') or 'main'} "
                f"{item.get('symbol')} {item.get('side')} trade_id={item.get('trade_id')}"
            )
        if stale_count > MAX_ALERT_ITEMS:
            lines.append(f"    ... +{stale_count - MAX_ALERT_ITEMS} more")

    lines.append("")
    lines.append("execution_suspect.lock is ACTIVE while anomalies exist.")
    lines.append("Executor will force PAPER-only for live modes until this clears.")

    msg = "\n".join(lines)

    try:
        tg_send(msg)
    except Exception as e:
        log.warning("Telegram send failed in execution_recovery_daemon: %r", e)


def loop() -> None:
    """
    Main daemon loop. Run forever, reconciling every INTERVAL_SECONDS.
    """
    log.info("execution_recovery_daemon starting with interval=%ss", INTERVAL_SECONDS)
    try:
        tg_send(f"🩹 Execution Recovery Daemon started (interval={INTERVAL_SECONDS}s).")
    except Exception:
        pass

    while True:
        try:
            record_heartbeat("execution_recovery_daemon")

            summary = reconcile_once()
            _write_report(summary)
            _update_lock(summary)
            _alert_if_needed(summary)

            time.sleep(INTERVAL_SECONDS)
        except KeyboardInterrupt:
            log.info("execution_recovery_daemon stopped by user")
            break
        except Exception as e:
            log.exception("execution_recovery_daemon loop error: %r", e)
            # Backoff a bit so we don't spin like crazy on repeated failure
            time.sleep(5)


if __name__ == "__main__":
    loop()
