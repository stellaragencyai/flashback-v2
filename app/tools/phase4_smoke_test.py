#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Flashback — Phase 4/5 Smoke Test (hardened for Phase 7 snapshot + enrichment)

Problem this fixes:
- publish_ai_event() will NOT write raw outcome_record into state/ai_events/outcomes.jsonl.
- If an outcome_record cannot be enriched (no setup context), it is routed to outcomes_orphans.jsonl.
- Old smoke test printed success even when it produced an orphan, causing false PASS.

This test now:
1) Appends a decision row (ai_decision_logger.append_decision)
2) Publishes a setup context event (so outcome can be enriched)
3) Publishes an outcome_record event
4) Verifies the trade_id exists in state/ai_events/outcomes.jsonl (NOT just orphans)
5) Runs linker + inspector and verifies joined output contains trade_id
"""

from __future__ import annotations

import inspect
import time
from pathlib import Path
from typing import Any, Dict, Optional

import orjson

from app.core.config import settings  # type: ignore
from app.core.ai_decision_logger import append_decision  # type: ignore
from app.ai import ai_events_spine as spine  # type: ignore


ROOT: Path = settings.ROOT  # type: ignore
STATE_DIR = ROOT / "state"
AI_EVENTS_DIR = STATE_DIR / "ai_events"

OUTCOMES_PATH = AI_EVENTS_DIR / "outcomes.jsonl"
ORPHANS_PATH = AI_EVENTS_DIR / "outcomes_orphans.jsonl"
DECISIONS_PATH = STATE_DIR / "ai_decisions.jsonl"
JOINED_PATH = STATE_DIR / "ai_decision_outcomes.jsonl"
CURSOR_PATH = STATE_DIR / "ai_decision_outcome_cursor.json"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_call_build(fn, **known_kwargs: Any) -> Dict[str, Any]:
    """
    Call build_* functions even if their signature changes.
    Only passes kwargs that exist in the function signature.
    """
    sig = inspect.signature(fn)
    kwargs: Dict[str, Any] = {}
    for name in sig.parameters.keys():
        if name in known_kwargs:
            kwargs[name] = known_kwargs[name]
    return fn(**kwargs)  # type: ignore[misc]


def _read_jsonl_has_trade_id(path: Path, trade_id: str, tail_lines: int = 5000) -> bool:
    if not path.exists():
        return False
    try:
        lines = path.read_bytes().splitlines()
    except Exception:
        return False
    tail = lines[-tail_lines:] if len(lines) > tail_lines else lines
    for raw in tail:
        raw = raw.strip()
        if not raw or raw[:1] != b"{":
            continue
        try:
            d = orjson.loads(raw)
        except Exception:
            continue
        if isinstance(d, dict) and d.get("trade_id") == trade_id:
            return True
    return False


def _reset_cursor() -> None:
    CURSOR_PATH.parent.mkdir(parents=True, exist_ok=True)
    CURSOR_PATH.write_bytes(orjson.dumps({"offset": 0, "updated_ms": 0}, option=orjson.OPT_INDENT_2))


def main() -> int:
    trade_id = f"PHASE4_SMOKE_{int(time.time())}"
    account_label = "flashback01"
    symbol = "BTCUSDT"
    timeframe = "5m"

    print("\n=== PHASE4/5 SMOKE (ENRICHED) ===")
    print("ROOT:", str(ROOT))
    print("trade_id:", trade_id)

    # 1) Decision row
    append_decision(
        {
            "event_type": "pilot_decision",
            "schema_version": 2,
            "trade_id": trade_id,
            "account_label": account_label,
            "symbol": symbol,
            "timeframe": timeframe,
            "decision": "COLD_START",
            "decision_code": "COLD_START",
            "allow": True,
            "size_multiplier": 1.0,
            "extra": {"stage": "phase4_smoke_test"},
        }
    )
    print("\n=== DECISION APPENDED ===")
    print("decisions_path:", str(DECISIONS_PATH))

    # 2) Publish setup context FIRST (required for enrichment)
    # Try official builder; fallback to a minimal context dict if builder signature changes.
    setup_evt: Optional[Dict[str, Any]] = None
    if hasattr(spine, "build_setup_context"):
        try:
            setup_evt = _safe_call_build(
                spine.build_setup_context,
                trade_id=trade_id,
                account_label=account_label,
                symbol=symbol,
                timeframe=timeframe,
                ts_ms=_now_ms(),
            )
        except Exception:
            setup_evt = None

    if setup_evt is None:
        setup_evt = {
            "event_type": "setup_context",
            "ts_ms": _now_ms(),
            "trade_id": trade_id,
            "account_label": account_label,
            "symbol": symbol,
            "timeframe": timeframe,
            "payload": {"source": "phase4_smoke_test_fallback"},
        }

    spine.publish_ai_event(setup_evt)
    print("\n=== SETUP_CONTEXT PUBLISHED ===")

    # 3) Publish outcome_record (should be enriched into outcomes.jsonl now)
    outcome_evt: Optional[Dict[str, Any]] = None
    if hasattr(spine, "build_outcome_record"):
        try:
            outcome_evt = _safe_call_build(
                spine.build_outcome_record,
                trade_id=trade_id,
                account_label=account_label,
                symbol=symbol,
                ts_ms=_now_ms(),
                payload={
                    "pnl_usd": 0.0,
                    "r_multiple": 0.0,
                    "win": None,
                    "final_status": "TEST",
                    "exit_reason": "PHASE4_SMOKE_ENRICH_TEST",
                },
                stats={"pnl_usd": 0.0, "r_multiple": 0.0, "win": None},
            )
        except Exception:
            outcome_evt = None

    if outcome_evt is None:
        outcome_evt = {
            "event_type": "outcome_record",
            "ts_ms": _now_ms(),
            "trade_id": trade_id,
            "account_label": account_label,
            "symbol": symbol,
            "payload": {
                "pnl_usd": 0.0,
                "r_multiple": 0.0,
                "win": None,
                "final_status": "TEST",
                "exit_reason": "PHASE4_SMOKE_ENRICH_TEST_FALLBACK",
            },
            "stats": {"pnl_usd": 0.0, "r_multiple": 0.0, "win": None},
        }

    spine.publish_ai_event(outcome_evt)
    print("\n=== OUTCOME_RECORD PUBLISHED ===")

    # 4) Verify enriched write happened (outcomes.jsonl must contain trade_id)
    in_outcomes = _read_jsonl_has_trade_id(OUTCOMES_PATH, trade_id)
    in_orphans = _read_jsonl_has_trade_id(ORPHANS_PATH, trade_id)

    print("\n=== VERIFY OUTCOME ROUTING ===")
    print("outcomes_path:", str(OUTCOMES_PATH))
    print("orphans_path :", str(ORPHANS_PATH))
    print("FOUND_IN_outcomes.jsonl:", bool(in_outcomes))
    print("FOUND_IN_outcomes_orphans.jsonl:", bool(in_orphans))

    if not in_outcomes:
        raise SystemExit(
            "FAIL: outcome was NOT enriched into outcomes.jsonl. "
            "It likely orphaned. This smoke test is doing its job by failing."
        )

    # 5) Run linker once from cursor=0 so joined output includes this trade_id
    _reset_cursor()
    import app.ai.ai_decision_outcome_linker as linker  # type: ignore

    report = linker.process_once(linker.DecisionIndex(linker.DECISIONS_PATH))
    print("\n=== LINKER RUN ===")
    print(orjson.dumps(report, option=orjson.OPT_INDENT_2).decode("utf-8"))

    # 6) Verify joined contains trade_id
    joined_has = _read_jsonl_has_trade_id(JOINED_PATH, trade_id, tail_lines=20000)
    print("\n=== VERIFY JOINED ===")
    print("joined_path:", str(JOINED_PATH))
    print("FOUND_IN_joined:", bool(joined_has))

    if not joined_has:
        raise SystemExit("FAIL: trade_id not found in joined output after linker run")

    print("\nPASS OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
