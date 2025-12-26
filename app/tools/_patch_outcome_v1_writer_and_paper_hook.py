#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Patch: Canonical Outcome v1 writer + PaperBroker close hook

What this does:
- Ensures app/ai/outcome_contract.py defines the canonical required fields (outcome.v1)
- Ensures app/ai/outcome_writer.py writes validated rows to state/ai_events/outcomes.v1.jsonl
- Patches app/sim/paper_broker.py to call writer on close (PAPER outcomes become v1)

Idempotent-ish: safe to re-run.
"""

from __future__ import annotations

from pathlib import Path
import re

ROOT = Path(__file__).resolve()
# tools/ -> app/ -> repo root
REPO = ROOT.parents[2]

AI_DIR = REPO / "app" / "ai"
SIM_DIR = REPO / "app" / "sim"
STATE_DIR = REPO / "state" / "ai_events"

AI_DIR.mkdir(parents=True, exist_ok=True)
SIM_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)

OUTCOME_CONTRACT = AI_DIR / "outcome_contract.py"
OUTCOME_WRITER = AI_DIR / "outcome_writer.py"
PAPER_BROKER = SIM_DIR / "paper_broker.py"

def write_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")

def patch_paper_broker() -> None:
    if not PAPER_BROKER.exists():
        raise SystemExit(f"Missing file: {PAPER_BROKER}")

    s = PAPER_BROKER.read_text(encoding="utf-8", errors="ignore")

    # 1) Add import (fail-soft)
    import_block = r"""
# --- Outcome v1 writer (fail-soft) ---
try:
    from app.ai.outcome_writer import write_outcome_from_paper_close  # type: ignore
except Exception:
    write_outcome_from_paper_close = None  # type: ignore
""".strip("\n")

    if "write_outcome_from_paper_close" not in s:
        # Insert after existing ai_events_spine import if present, else near top
        m = re.search(r"from\s+app\.ai\.ai_events_spine\s+import\s+.*\n", s)
        if m:
            insert_at = m.end()
            s = s[:insert_at] + "\n" + import_block + "\n" + s[insert_at:]
        else:
            # fallback: after first import block
            m2 = re.search(r"\n\n", s)
            insert_at = m2.end() if m2 else 0
            s = s[:insert_at] + import_block + "\n\n" + s[insert_at:]

    # 2) Hook into _close_position
    # Find end of publish_ai_event(outcome_event) call, then add v1 writer call.
    # We look for: publish_ai_event(outcome_event)
    hook = r"""
        # ✅ Canonical Outcome v1 append (PAPER close)
        if write_outcome_from_paper_close is not None:
            try:
                write_outcome_from_paper_close(
                    trade_id=str(trade_id),
                    symbol=str(symbol),
                    entry_side=str(pos.get("side") or ""),
                    entry_qty=float(pos.get("qty") or 0.0),
                    entry_px=float(pos.get("entry_price") or 0.0),
                    opened_ts_ms=int(pos.get("opened_ts_ms") or 0),
                    exit_px=float(close_px),
                    exit_qty=float(pos.get("qty") or 0.0),
                    closed_ts_ms=int(ts_ms),
                    pnl_usd=float(pnl_usd),
                    fees_usd=float(fees_usd),
                    account_label=str(self.account_label),
                    timeframe=str(pos.get("timeframe") or ""),
                    setup_type=str(pos.get("setup_type") or ""),
                    mode=str((pos.get("extra") or {}).get("mode") or "PAPER"),
                    close_reason=str(close_reason),
                    client_trade_id=str((pos.get("features") or {}).get("client_trade_id") or ""),
                    source_trade_id=str((pos.get("features") or {}).get("source_trade_id") or ""),
                )
            except Exception:
                # fail-soft: never crash broker close
                pass
""".strip("\n")

    if "Canonical Outcome v1 append (PAPER close)" not in s:
        # Insert right after publish_ai_event(outcome_event)
        m3 = re.search(r"publish_ai_event\(outcome_event\)\s*\n", s)
        if not m3:
            raise SystemExit("Could not find publish_ai_event(outcome_event) in PaperBroker._close_position")
        insert_at = m3.end()
        s = s[:insert_at] + "\n" + hook + "\n" + s[insert_at:]

    PAPER_BROKER.write_text(s, encoding="utf-8")

def main() -> int:
    contract_src = r'''# -*- coding: utf-8 -*-
"""
Canonical Trade Outcome Contract (Outcome v1)

This schema is the single source of truth for outcome rows written to:
  state/ai_events/outcomes.v1.jsonl

Design goals:
- Required fields for joins (trade_id/symbol/timestamps)
- Required exit metrics (exit_px/exit_qty/pnl/fees)
- Fail-soft writer: validation lives here, not scattered across bots
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, List

OUTCOME_SCHEMA_VERSION = "outcome.v1"

# Required keys for a v1 outcome row
OUTCOME_REQUIRED = [
    "schema_version",
    "event_type",
    "trade_id",
    "symbol",
    "entry_side",
    "entry_qty",
    "entry_px",
    "opened_ts_ms",
    "exit_side",
    "exit_qty",
    "exit_px",
    "closed_ts_ms",
    "pnl_usd",
    "fees_usd",
]

def validate_outcome_v1(row: Dict[str, Any]) -> None:
    missing = [k for k in OUTCOME_REQUIRED if k not in row or row.get(k) is None]
    if missing:
        raise ValueError(f"outcome_missing_required={missing}")

    if str(row.get("schema_version")) != OUTCOME_SCHEMA_VERSION:
        raise ValueError(f"outcome_bad_schema_version={row.get('schema_version')!r}")

    if str(row.get("event_type")) != "trade_outcome":
        raise ValueError(f"outcome_bad_event_type={row.get('event_type')!r}")

    # Basic type sanity (fail fast, not perfect)
    for k in ("entry_qty","entry_px","exit_qty","exit_px","pnl_usd","fees_usd"):
        try:
            float(row.get(k))
        except Exception:
            raise ValueError(f"outcome_bad_number:{k}={row.get(k)!r}")

    for k in ("opened_ts_ms","closed_ts_ms"):
        try:
            int(row.get(k))
        except Exception:
            raise ValueError(f"outcome_bad_int:{k}={row.get(k)!r}")

    sym = str(row.get("symbol") or "").strip()
    if not sym:
        raise ValueError("outcome_empty_symbol")

    tid = str(row.get("trade_id") or "").strip()
    if not tid:
        raise ValueError("outcome_empty_trade_id")
'''
    writer_src = r'''# -*- coding: utf-8 -*-
"""
Outcome Writer (v1)

Writes ONLY canonical v1 outcomes to:
  state/ai_events/outcomes.v1.jsonl

This is intentionally boring. Boring is reliable.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, Optional

from app.core.config import settings
from app.ai.outcome_contract import OUTCOME_SCHEMA_VERSION, validate_outcome_v1

OUT_PATH: Path = settings.ROOT / "state" / "ai_events" / "outcomes.v1.jsonl"
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

def _append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    with path.open("ab") as f:
        f.write(json.dumps(row, ensure_ascii=False).encode("utf-8") + b"\\n")

def write_outcome_v1(row: Dict[str, Any]) -> None:
    validate_outcome_v1(row)
    _append_jsonl(OUT_PATH, row)

def write_outcome_from_paper_close(
    *,
    trade_id: str,
    symbol: str,
    entry_side: str,
    entry_qty: float,
    entry_px: float,
    opened_ts_ms: int,
    exit_px: float,
    exit_qty: float,
    closed_ts_ms: int,
    pnl_usd: float,
    fees_usd: float,
    account_label: str,
    timeframe: str,
    setup_type: str,
    mode: str,
    close_reason: str,
    client_trade_id: str = "",
    source_trade_id: str = "",
) -> Dict[str, Any]:
    # Exit side is the opposite
    es = (entry_side or "").strip().lower()
    if es in ("buy", "long"):
        exit_side = "Sell"
    elif es in ("sell", "short"):
        exit_side = "Buy"
    else:
        # unknown side: still write outcome, but keep it explicit
        exit_side = "Unknown"

    row: Dict[str, Any] = {
        "schema_version": OUTCOME_SCHEMA_VERSION,
        "event_type": "trade_outcome",
        "ts_ms": int(time.time() * 1000),

        "trade_id": str(trade_id),
        "client_trade_id": (str(client_trade_id) if client_trade_id else None),
        "source_trade_id": (str(source_trade_id) if source_trade_id else None),

        "symbol": str(symbol).upper(),

        "account_label": str(account_label),
        "timeframe": str(timeframe),
        "setup_type": str(setup_type),
        "mode": str(mode),

        "entry_side": str(entry_side),
        "entry_qty": float(entry_qty),
        "entry_px": float(entry_px),
        "opened_ts_ms": int(opened_ts_ms),

        "exit_side": str(exit_side),
        "exit_qty": float(exit_qty),
        "exit_px": float(exit_px),
        "closed_ts_ms": int(closed_ts_ms),

        "pnl_usd": float(pnl_usd),
        "fees_usd": float(fees_usd),

        "close_reason": str(close_reason),
    }

    write_outcome_v1(row)
    return row
'''
    write_file(OUTCOME_CONTRACT, contract_src)
    write_file(OUTCOME_WRITER, writer_src)

    patch_paper_broker()

    print("OK: wrote app/ai/outcome_contract.py")
    print("OK: wrote app/ai/outcome_writer.py")
    print("OK: patched app/sim/paper_broker.py (PAPER close -> outcomes.v1.jsonl)")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
