# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

from app.core.config import settings
from app.ai.outcome_writer import write_outcome_v1

OUT_PATH = settings.ROOT / "state" / "ai_events" / "outcomes.v1.jsonl"
PAPER_DIR = settings.ROOT / "state" / "paper"

def _read_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8", errors="ignore") or "{}")
    except Exception:
        return {}

def _iter_jsonl(path: Path):
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except Exception:
            continue

def _existing_trade_ids() -> Set[str]:
    ids: Set[str] = set()
    for row in _iter_jsonl(OUT_PATH):
        tid = row.get("trade_id")
        if tid:
            ids.add(str(tid))
    return ids

def _mk_outcome_row_from_closed_trade(account_label: str, t: Dict[str, Any]) -> Dict[str, Any]:
    # Minimal Outcome v1 required keys only (contract is strict on these)
    trade_id = str(t.get("trade_id") or "").strip()
    symbol = str(t.get("symbol") or "").strip().upper()
    side = str(t.get("side") or "").strip().lower()

    entry_px = float(t.get("entry_price") or 0.0)
    entry_qty = float(t.get("size") or 0.0)

    opened_ms = int(t.get("opened_ms") or 0)
    closed_ms = int(t.get("closed_ms") or 0)
    exit_px = float(t.get("exit_price") or 0.0)
    pnl_usd = float(t.get("pnl_usd") or 0.0)

    # Derive exit side
    if side == "long":
        entry_side = "Buy"
        exit_side = "Sell"
    elif side == "short":
        entry_side = "Sell"
        exit_side = "Buy"
    else:
        entry_side = "Unknown"
        exit_side = "Unknown"

    close_reason = str(t.get("exit_reason") or "backfill_unknown").strip() or "backfill_unknown"

    row: Dict[str, Any] = {
        "schema_version": "outcome.v1",
        "event_type": "trade_outcome",
        "ts_ms": int(time.time() * 1000),

        "trade_id": trade_id,
        "symbol": symbol,

        "entry_side": entry_side,
        "entry_qty": float(entry_qty),
        "entry_px": float(entry_px),
        "opened_ts_ms": int(opened_ms),

        "exit_side": exit_side,
        "exit_qty": float(entry_qty),
        "exit_px": float(exit_px),
        "closed_ts_ms": int(closed_ms if closed_ms > 0 else int(time.time() * 1000)),

        "pnl_usd": float(pnl_usd),
        "fees_usd": 0.0,
        "close_reason": close_reason,

        # Keep these optional fields (safe; validator ignores extras)
        "account_label": account_label,
        "timeframe": str(t.get("timeframe") or ""),
        "setup_type": str(t.get("setup_type") or ""),
        "mode": "PAPER",
        "backfilled": True,
    }
    return row

def main() -> None:
    PAPER_DIR.mkdir(parents=True, exist_ok=True)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    existing = _existing_trade_ids()
    total_missing = 0
    total_written = 0
    per_account: List[Tuple[str,int,int]] = []

    for fp in sorted(PAPER_DIR.glob("*.json")):
        label = fp.stem
        j = _read_json(fp)
        closed = j.get("closed_trades") or []
        if not isinstance(closed, list) or not closed:
            continue

        missing_here = 0
        written_here = 0

        for item in closed:
            if not isinstance(item, dict):
                continue
            tid = str(item.get("trade_id") or "").strip()
            if not tid:
                continue
            if tid in existing:
                continue

            # Build and write
            row = _mk_outcome_row_from_closed_trade(label, item)
            try:
                write_outcome_v1(row)
                existing.add(tid)
                missing_here += 1
                written_here += 1
            except Exception:
                # fail-soft: keep going
                missing_here += 1

        if missing_here:
            total_missing += missing_here
            total_written += written_here
        per_account.append((label, len(closed), written_here))

    print("=== BACKFILL OUTCOMES v1 ===")
    print("OUT_PATH=", str(OUT_PATH))
    print("accounts=", len(per_account))
    print("total_written=", total_written)
    print("--- per_account ---")
    for label, closed_n, wrote_n in per_account:
        if wrote_n:
            print(f"{label}: closed={closed_n} backfilled={wrote_n}")

if __name__ == '__main__':
    main()
