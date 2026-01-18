# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set, Tuple

from app.core.config import settings
from app.ai.outcome_writer import append_outcome_v1


ROOT = settings.ROOT
PAPER_DIR = ROOT / "state" / "paper"


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8", errors="ignore") or "{}")
    except Exception:
        return {}


def _iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        return []
    out: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = (line or "").strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except Exception:
            continue
    return out


def _lane_dir_for_label(account_label: str) -> Path:
    # Keep consistent with trade_outcome_recorder lanes:
    # state/ai_events_flashbackXX
    return ROOT / "state" / f"ai_events_{account_label}"


def _lane_outcomes_path(account_label: str) -> Path:
    return _lane_dir_for_label(account_label) / "outcomes.v1.jsonl"


def _existing_trade_ids_for_lane(account_label: str) -> Set[str]:
    ids: Set[str] = set()
    p = _lane_outcomes_path(account_label)
    for row in _iter_jsonl(p):
        tid = row.get("trade_id")
        if tid:
            ids.add(str(tid))
    return ids


def _mk_outcome_row_from_closed_trade(account_label: str, t: Dict[str, Any]) -> Dict[str, Any]:
    # Pull fields from paper broker closed_trades entries
    trade_id = str(t.get("trade_id") or "").strip()
    symbol = str(t.get("symbol") or "").strip().upper()
    side = str(t.get("side") or "").strip().lower()

    entry_px = float(t.get("entry_price") or 0.0)
    entry_qty = float(t.get("size") or 0.0)

    opened_ms = int(t.get("opened_ms") or 0)
    closed_ms = int(t.get("closed_ms") or 0)
    exit_px = float(t.get("exit_price") or 0.0)
    pnl_usd = float(t.get("pnl_usd") or 0.0)

    timeframe = str(t.get("timeframe") or t.get("tf") or "5m").strip() or "5m"
    setup_type = str(t.get("setup_type") or t.get("setup") or "unknown").strip() or "unknown"

    if side == "long":
        entry_side = "Buy"
        exit_side = "Sell"
    elif side == "short":
        entry_side = "Sell"
        exit_side = "Buy"
    else:
        entry_side = "Unknown"
        exit_side = "Unknown"

    close_reason = str(t.get("exit_reason") or t.get("close_reason") or "backfill_unknown").strip() or "backfill_unknown"

    # Outcome v1 (writer normalizes schema_version + validates)
    row: Dict[str, Any] = {
        "event_type": "trade_outcome",
        "ts_ms": int(time.time() * 1000),

        "trade_id": trade_id,
        "symbol": symbol,

        "setup_type": setup_type,
        "timeframe": timeframe,

        "entry_side": entry_side,
        "entry_qty": float(entry_qty),
        "entry_px": float(entry_px),
        "opened_ts_ms": int(opened_ms),

        "exit_side": exit_side,
        "exit_qty": float(entry_qty),
        "exit_px": float(exit_px),
        "closed_ts_ms": int(closed_ms),

        "pnl_usd": float(pnl_usd),
        "close_reason": close_reason,

        # Helpful traceability
        "account_label": account_label,
        "source": "backfill_from_paper",
    }
    return row


def main() -> None:
    if not PAPER_DIR.exists():
        raise SystemExit(f"PAPER_DIR missing: {PAPER_DIR}")

    ledgers = sorted(PAPER_DIR.glob("flashback*.json"))
    if not ledgers:
        raise SystemExit(f"No ledgers found under: {PAPER_DIR}")

    total_written = 0
    total_skipped = 0

    for ledger_path in ledgers:
        account_label = ledger_path.stem.strip()
        raw = _read_json(ledger_path)

        closed_trades = raw.get("closed_trades") or []
        if not isinstance(closed_trades, list) or not closed_trades:
            print(f"[backfill] {account_label}: closed_trades=0 (nothing to do)")
            continue

        lane_dir = _lane_dir_for_label(account_label)
        lane_dir.mkdir(parents=True, exist_ok=True)

        # Force the outcome_writer to write into THIS lane.
        os.environ["AI_EVENTS_DIR"] = str(lane_dir)

        existing = _existing_trade_ids_for_lane(account_label)

        wrote = 0
        skipped = 0

        for t in closed_trades:
            if not isinstance(t, dict):
                continue
            tid = str(t.get("trade_id") or "").strip()
            if not tid:
                continue
            if tid in existing:
                skipped += 1
                continue

            row = _mk_outcome_row_from_closed_trade(account_label, t)
            append_outcome_v1(row)
            existing.add(tid)
            wrote += 1

        total_written += wrote
        total_skipped += skipped

        out_path = _lane_outcomes_path(account_label)
        out_bytes = out_path.stat().st_size if out_path.exists() else 0
        print(f"[backfill] {account_label}: wrote={wrote} skipped={skipped} -> {out_path} bytes={out_bytes}")

    print(f"[backfill] DONE total_written={total_written} total_skipped={total_skipped}")


if __name__ == "__main__":
    main()
