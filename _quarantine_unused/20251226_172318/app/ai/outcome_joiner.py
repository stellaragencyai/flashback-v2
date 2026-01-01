#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Outcome Joiner v2.0 (canonical + terminal-only)

What it does
------------
- Reads WS execution events from: state/ws_executions.jsonl
- Uses cursor: state/ws_executions.cursor
- Joins executions -> setup_context via trade_id (prefers orderId)
- Emits canonical AI events to: state/ai_events/outcomes.jsonl
  ONLY when terminal:
    extra.is_terminal=True
    extra.final_status in {"WIN","LOSS","BREAKEVEN"}

Also writes raw audit lines to: state/ai_events/outcomes_raw.jsonl

Why
---
Your v1 joiner was dumping non-canonical rows into outcomes.jsonl and
logging non-terminal fragments, which Phase 3 integrity correctly flags as 100% broken.
"""

from __future__ import annotations

import json
import time
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import orjson

from app.core.config import settings
from app.core.position_bus import get_positions_snapshot as bus_get_positions_snapshot

# Prefer spine publisher if available
try:
    from app.ai.ai_events_spine import publish_ai_event  # type: ignore
except Exception:
    publish_ai_event = None  # type: ignore


log = logging.getLogger("outcome_joiner")
if not log.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s] %(message)s"))
    log.addHandler(h)
log.setLevel(logging.INFO)

ROOT: Path = settings.ROOT

EXEC_FILE: Path = ROOT / "state" / "ws_executions.jsonl"
CURSOR_FILE: Path = ROOT / "state" / "ws_executions.cursor"

AI_DIR: Path = ROOT / "state" / "ai_events"
AI_DIR.mkdir(parents=True, exist_ok=True)

SETUPS_FILE: Path = AI_DIR / "setups.jsonl"

OUTCOMES_FILE: Path = AI_DIR / "outcomes.jsonl"
OUTCOMES_RAW_FILE: Path = AI_DIR / "outcomes_raw.jsonl"
OUTCOMES_UNMATCHED_FILE: Path = AI_DIR / "outcomes_unmatched.jsonl"

DEDUPE_FILE: Path = AI_DIR / "outcome_dedupe.json"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _read_cursor() -> int:
    if not CURSOR_FILE.exists():
        return 0
    try:
        return int(CURSOR_FILE.read_text("utf-8").strip() or "0")
    except Exception:
        return 0


def _write_cursor(pos: int) -> None:
    try:
        CURSOR_FILE.write_text(str(pos), encoding="utf-8")
    except Exception as e:
        log.warning("failed to write cursor: %r", e)


def _load_dedupe() -> Dict[str, int]:
    if not DEDUPE_FILE.exists():
        return {}
    try:
        return json.loads(DEDUPE_FILE.read_text("utf-8") or "{}")
    except Exception:
        return {}


def _save_dedupe(d: Dict[str, int]) -> None:
    try:
        DEDUPE_FILE.write_text(json.dumps(d), encoding="utf-8")
    except Exception as e:
        log.warning("failed to save dedupe: %r", e)


def _append_jsonl(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("ab") as f:
        f.write(orjson.dumps(obj) + b"\n")


def _load_setup_index(max_lines: int = 20000) -> Dict[str, Dict[str, Any]]:
    """
    Build an index: trade_id -> {"setup_fingerprint": ..., "symbol": ..., "account_label": ...}
    from setups.jsonl. Loads last max_lines for speed.
    """
    idx: Dict[str, Dict[str, Any]] = {}
    if not SETUPS_FILE.exists():
        return idx

    try:
        lines = SETUPS_FILE.read_bytes().splitlines()
        if max_lines and len(lines) > max_lines:
            lines = lines[-max_lines:]
        for raw in lines:
            try:
                e = orjson.loads(raw)
            except Exception:
                continue
            if not isinstance(e, dict):
                continue
            if e.get("event_type") != "setup_context":
                continue
            trade_id = e.get("trade_id")
            payload = e.get("payload") or {}
            feats = (payload.get("features") or {}) if isinstance(payload, dict) else {}
            fp = feats.get("setup_fingerprint")
            sym = payload.get("symbol") if isinstance(payload, dict) else None
            acct = payload.get("account_label") if isinstance(payload, dict) else None
            if trade_id and fp:
                idx[str(trade_id)] = {
                    "setup_fingerprint": fp,
                    "symbol": sym,
                    "account_label": acct,
                }
    except Exception as e:
        log.warning("failed building setup index: %r", e)
    return idx


def _extract_trade_id_and_symbol(row: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Try to normalize execution row fields across your various formats.
    Returns: (trade_id, symbol, account_label)
    """
    symbol = row.get("symbol") or row.get("s")
    account_label = row.get("account_label") or row.get("label") or row.get("account") or "main"

    order_id = row.get("orderId") or row.get("order_id") or row.get("orderID")
    link_id = row.get("orderLinkId") or row.get("order_link_id") or row.get("orderLinkID")

    # Prefer orderId because your WS feed often blanks orderLinkId
    if order_id:
        return str(order_id), str(symbol) if symbol else None, str(account_label) if account_label else "main"
    if link_id:
        return str(link_id), str(symbol) if symbol else None, str(account_label) if account_label else "main"
    return None, str(symbol) if symbol else None, str(account_label) if account_label else "main"


def _is_terminal_for_symbol(account_label: str, symbol: str) -> bool:
    """
    Terminal heuristic:
    - If no open position exists for this symbol on this account_label, we call it terminal.
    This isn't perfect but it's consistent and works with the rest of your stack.
    """
    try:
        rows = bus_get_positions_snapshot(
            label=account_label,
            category="linear",
            max_age_seconds=10,
            allow_rest_fallback=True,
        )
        if not isinstance(rows, list):
            return False
        for p in rows:
            if not isinstance(p, dict):
                continue
            if str(p.get("symbol") or "") != str(symbol):
                continue
            # any non-zero size means still open
            try:
                size = float(p.get("size") or 0)
            except Exception:
                size = 0.0
            if abs(size) > 0.0:
                return False
        return True
    except Exception:
        # If position bus fails, do NOT claim terminal.
        return False


def _final_status_from_pnl(pnl_usd: float) -> str:
    if pnl_usd > 0:
        return "WIN"
    if pnl_usd < 0:
        return "LOSS"
    return "BREAKEVEN"


def _build_canonical_outcome(
    trade_id: str,
    symbol: str,
    account_label: str,
    pnl_usd: float,
    setup_fingerprint: Optional[str],
    source: str,
    raw: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Canonical event envelope expected by Phase 3 integrity.
    """
    ts = _now_ms()
    final_status = _final_status_from_pnl(pnl_usd)

    payload: Dict[str, Any] = {
        "trade_id": trade_id,
        "symbol": symbol,
        "account_label": account_label,
        "pnl_usd": float(pnl_usd),
        "setup_fingerprint": setup_fingerprint,
        "extra": {
            "is_terminal": True,
            "final_status": final_status,
            "source": source,
        },
        "raw": raw,
    }

    return {
        "event_type": "outcome_record",
        "ts_ms": ts,
        "trade_id": trade_id,
        "payload": payload,
        "schema_version": "ai_event_v1",
    }


def main() -> None:
    pos = _read_cursor()
    log.info("Outcome joiner starting at cursor=%s", pos)

    published = 0
    skipped = 0

    dedupe = _load_dedupe()
    setup_idx = _load_setup_index()

    if not EXEC_FILE.exists():
        log.warning("Missing executions file: %s", str(EXEC_FILE))
        return

    with EXEC_FILE.open("rb") as f:
        f.seek(pos)
        for raw_line in f:
            pos = f.tell()

            try:
                row = orjson.loads(raw_line)
            except Exception:
                skipped += 1
                continue
            if not isinstance(row, dict):
                skipped += 1
                continue

            # Always keep raw audit
            try:
                _append_jsonl(OUTCOMES_RAW_FILE, row)
            except Exception:
                pass

            trade_id, symbol, account_label = _extract_trade_id_and_symbol(row)
            if not trade_id or not symbol:
                skipped += 1
                continue

            # Dedupe key: trade_id + execId (if present) else trade_id + ts
            exec_id = row.get("execId") or row.get("exec_id") or row.get("executionId")
            k = f"{trade_id}:{exec_id or row.get('ts_ms') or row.get('time') or ''}"
            if k in dedupe:
                skipped += 1
                continue
            dedupe[k] = _now_ms()

            # Extract pnl
            pnl = row.get("closedPnl") or row.get("closed_pnl") or row.get("pnl") or row.get("pnl_usd")
            try:
                pnl_usd = float(pnl) if pnl is not None else 0.0
            except Exception:
                pnl_usd = 0.0

            # terminal-only gate
            if not _is_terminal_for_symbol(account_label or "main", symbol):
                # Not terminal: keep an unmatched breadcrumb (optional)
                _append_jsonl(
                    OUTCOMES_UNMATCHED_FILE,
                    {
                        "ts_ms": _now_ms(),
                        "trade_id": trade_id,
                        "symbol": symbol,
                        "account_label": account_label,
                        "reason": "not_terminal_yet",
                        "raw": row,
                    },
                )
                skipped += 1
                continue

            # Join setup fingerprint
            fp = None
            meta = setup_idx.get(trade_id)
            if meta:
                fp = meta.get("setup_fingerprint")

            outcome_event = _build_canonical_outcome(
                trade_id=trade_id,
                symbol=symbol,
                account_label=account_label or "main",
                pnl_usd=pnl_usd,
                setup_fingerprint=fp,
                source="ws_executions",
                raw=row,
            )

            # Publish canonical: use spine publisher if available, else append to outcomes.jsonl directly
            try:
                if publish_ai_event:
                    publish_ai_event(outcome_event)  # type: ignore
                else:
                    _append_jsonl(OUTCOMES_FILE, outcome_event)
                published += 1
                log.info(
                    "Logged outcome_record: trade_id=%s symbol=%s account=%s pnl_usd=%.6f terminal=True final=%s fp=%s",
                    trade_id,
                    symbol,
                    account_label,
                    pnl_usd,
                    outcome_event["payload"]["extra"]["final_status"],
                    "yes" if fp else "no",
                )
            except Exception as e:
                log.warning("failed to publish outcome: %r", e)
                skipped += 1

            if (published + skipped) % 250 == 0:
                log.info("Progress: published=%s skipped=%s cursor=%s", published, skipped, pos)
                _write_cursor(pos)
                _save_dedupe(dedupe)

    _write_cursor(pos)
    _save_dedupe(dedupe)
    log.info("Done: published=%s skipped=%s cursor=%s", published, skipped, pos)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Outcome joiner stopped by user.")
