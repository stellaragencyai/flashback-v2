#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Pilot v2.2

Role
----
Thin coordinator sitting on top of:
  • app.core.ai_state_bus.build_ai_snapshot()   (state aggregation)
  • AI policies (sample + core)
  • AI actions file (AI_ACTIONS_PATH JSONL)     (writes AI "decisions" to a bus)
  • flashback_common.record_heartbeat()        (liveness for supervisor/status)

Energy:
  • DRY-RUN ONLY by default.
  • No direct order placement. Execution is delegated to ai_action_router
    (→ ExecSignal queue) or other executors later.

Env
---
ACCOUNT_LABEL                 (default: "main")
AI_PILOT_ENABLED              (default: "true")
AI_PILOT_POLL_SECONDS         (default: "3")
AI_PILOT_DRY_RUN              (default: "true")

# Policy toggles
AI_PILOT_SAMPLE_POLICY        (default: "false")  # uses app.ai.ai_policy_sample
AI_PILOT_CORE_POLICY          (default: "false")  # uses app.ai.ai_policy_core

# Action writing
AI_PILOT_WRITE_ACTIONS        (default: "false")  # if true, writes to AI_ACTIONS_PATH

# AI actions bus path
AI_ACTIONS_PATH               (default: "state/ai_actions.jsonl")

Usage
-----
Typically launched by supervisor_ai_stack:

    from app.bots.ai_pilot import loop as ai_pilot_loop

    ai_pilot_loop()
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import orjson

# Builder for schema-compliant AIAction objects
from app.core.ai_action_builder import build_trade_action_from_sample

# Logging (robust)
try:
    from app.core.log import get_logger
except Exception:  # pragma: no cover
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


logger = get_logger("ai_pilot")

# Core helpers
from app.core.flashback_common import (
    send_tg,
    record_heartbeat,
    alert_bot_error,
)

from app.core.ai_state_bus import build_ai_snapshot

# Optional sample policy
try:
    from app.ai.ai_policy_sample import evaluate_state as sample_evaluate_state
except Exception:  # pragma: no cover
    sample_evaluate_state = None  # type: ignore[assignment]

# Optional core policy
try:
    from app.ai.ai_policy_core import evaluate_state as core_evaluate_state
except Exception:  # pragma: no cover
    core_evaluate_state = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Env helpers
# ---------------------------------------------------------------------------

def _env_bool(name: str, default: str = "false") -> bool:
    raw = os.getenv(name, default).strip().lower()
    return raw in ("1", "true", "yes", "y", "on")


def _env_int(name: str, default: str) -> int:
    try:
        return int(os.getenv(name, default).strip())
    except Exception:
        return int(default)


ACCOUNT_LABEL: str = os.getenv("ACCOUNT_LABEL", "main").strip() or "main"

AI_PILOT_ENABLED: bool = _env_bool("AI_PILOT_ENABLED", "true")
POLL_SECONDS: int = _env_int("AI_PILOT_POLL_SECONDS", "3")
DRY_RUN: bool = _env_bool("AI_PILOT_DRY_RUN", "true")

# Policy toggles
USE_SAMPLE_POLICY: bool = _env_bool("AI_PILOT_SAMPLE_POLICY", "false")
USE_CORE_POLICY: bool = _env_bool("AI_PILOT_CORE_POLICY", "false")

# Action writing toggle
WRITE_ACTIONS: bool = _env_bool("AI_PILOT_WRITE_ACTIONS", "false")

# AI actions file path (aligned with ai_action_router + tools)
try:
    from app.core.config import settings  # type: ignore
    default_actions_path = getattr(settings, "AI_ACTIONS_PATH", "state/ai_actions.jsonl")
except Exception:
    default_actions_path = "state/ai_actions.jsonl"

env_actions_path = os.getenv("AI_ACTIONS_PATH", "").strip()
_actions_path_str = env_actions_path or default_actions_path

AI_ACTIONS_FILE: Path = Path(_actions_path_str).resolve()
AI_ACTIONS_FILE.parent.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# State builders / helpers
# ---------------------------------------------------------------------------

def _build_ai_state() -> Dict[str, Any]:
    """
    Wrap ai_state_bus.build_ai_snapshot() into a policy-friendly dict.

    Policies do NOT need to know about internal bus layout; they get:
      - label, dry_run
      - account summary
      - flat positions list
      - light telemetry about buses (ages)
      - raw_snapshot for advanced logic
    """
    snap = build_ai_snapshot(
        focus_symbols=None,
        include_trades=False,
        trades_limit=0,
        include_orderbook=True,
    )

    account = snap.get("account") or {}
    pos_block = snap.get("positions") or {}
    positions_by_symbol = pos_block.get("by_symbol") or {}
    positions_list: List[Dict[str, Any]] = list(positions_by_symbol.values())

    buses = {
        "positions_bus_age_sec": snap.get("positions_bus_age_sec"),
        "orderbook_bus_age_sec": snap.get("orderbook_bus_age_sec"),
        "trades_bus_age_sec": snap.get("trades_bus_age_sec"),
    }

    ai_state: Dict[str, Any] = {
        "label": ACCOUNT_LABEL,
        "dry_run": DRY_RUN,
        "account": {
            "equity_usdt": account.get("equity_usdt"),
            "mmr_pct": account.get("mmr_pct"),
            "open_positions": len(positions_list),
        },
        "positions": positions_list,
        "buses": buses,
        "raw_snapshot": snap,
    }
    return ai_state


# ---------------------------------------------------------------------------
# Policy runners
# ---------------------------------------------------------------------------

def _run_sample_policy(ai_state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Run the sample policy (if available) and return a list of
    schema-compliant AIAction dicts.

    Legacy sample policies may return "advice_only" dicts with fields like:
        {
            "type": "advice_only",
            "reason": "sample_policy",
            "label": "main",
            "symbol": "WETUSDT",
            "side": "Buy",
            "size": "382.0",
            "dry_run": True,
        }

    This function adapts those into proper AIAction objects using
    build_trade_action_from_sample(...).
    """
    if not USE_SAMPLE_POLICY:
        return []
    if sample_evaluate_state is None:
        logger.warning("Sample policy enabled but app.ai.ai_policy_sample is missing.")
        return []

    try:
        raw_actions = sample_evaluate_state(ai_state)  # type: ignore[misc]
        if not isinstance(raw_actions, list):
            return []

        ai_actions: List[Dict[str, Any]] = []
        for raw in raw_actions:
            if not isinstance(raw, dict):
                continue

            # Heuristic: treat any dict with symbol + side as a trade candidate.
            symbol = raw.get("symbol")
            side = raw.get("side")
            if not symbol or not side:
                # Pure advisory or malformed; ignore for now.
                continue

            reason = str(raw.get("reason") or "sample_policy")
            size_hint = raw.get("size")
            confidence = float(raw.get("confidence", 0.6))

            tags = ["sample_policy", "legacy_bridge"]
            extra = {"legacy_action": raw}
            if size_hint is not None:
                extra["legacy_size_hint"] = size_hint

            ai_action = build_trade_action_from_sample(
                account_label=ACCOUNT_LABEL,
                symbol=str(symbol),
                side=str(side),
                reason=reason,
                risk_R=1.0,
                expected_R=2.0,
                size_fraction=1.0,
                confidence=confidence,
                tags=tags,
                model_id="SAMPLE_POLICY_V1",
                extra=extra,
            )
            ai_actions.append(ai_action)

        return ai_actions

    except Exception as e:
        alert_bot_error("ai_pilot", f"sample_policy error: {e}", "ERROR")
        return []


def _run_core_policy(ai_state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Run the core policy (if available) and return its actions list.

    For now we assume core policies either:
      - already emit schema-compliant AIAction dicts, or
      - are disabled while being developed.

    We simply pass through any dicts they return.
    """
    if not USE_CORE_POLICY:
        return []
    if core_evaluate_state is None:
        logger.warning("Core policy enabled but app.ai.ai_policy_core is missing.")
        return []

    try:
        actions = core_evaluate_state(ai_state)  # type: ignore[misc]
        if not isinstance(actions, list):
            return []
        return [a for a in actions if isinstance(a, dict)]
    except Exception as e:
        alert_bot_error("ai_pilot", f"core_policy error: {e}", "ERROR")
        return []


# ---------------------------------------------------------------------------
# Action dispatch → AI_ACTIONS_PATH
# ---------------------------------------------------------------------------

def _dispatch_actions(actions: List[Dict[str, Any]], *, label: str) -> int:
    """
    Write actions to the AI actions JSONL bus (AI_ACTIONS_PATH).

    Each action is expected to already be CLOSE to a schema-compliant
    AIAction. We ensure:

        - ts_ms        present
        - account_label present
        - source       set to "ai_pilot"
        - dry_run      reflects AI_PILOT_DRY_RUN

    Then we append one JSON object per line to AI_ACTIONS_FILE.
    """
    if not actions:
        return 0
    if not WRITE_ACTIONS:
        return 0

    now_ms = int(time.time() * 1000)
    written = 0

    try:
        with AI_ACTIONS_FILE.open("ab") as f:
            for raw in actions:
                if not isinstance(raw, dict):
                    continue
                a = dict(raw)

                # Ensure critical metadata exists
                a.setdefault("ts_ms", now_ms)
                a.setdefault("account_label", label)
                a["source"] = "ai_pilot"
                a["dry_run"] = DRY_RUN

                payload = orjson.dumps(a)
                f.write(payload)
                f.write(b"\n")
                written += 1

    except Exception as e:
        alert_bot_error("ai_pilot", f"dispatch_actions error: {e}", "ERROR")
        return 0

    return written


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def loop() -> None:
    """
    Main AI Pilot loop.

    Every POLL_SECONDS:
      - Record heartbeat
      - Build ai_state
      - Run enabled policies (sample + core)
      - Optionally write actions to AI_ACTIONS_PATH
    """
    if not AI_PILOT_ENABLED:
        logger.warning("AI Pilot is disabled via AI_PILOT_ENABLED=false. Exiting loop().")
        return

    mode_bits = []
    mode_bits.append("DRY-RUN" if DRY_RUN else "LIVE?")
    if USE_SAMPLE_POLICY:
        mode_bits.append("sample_policy")
    if USE_CORE_POLICY:
        mode_bits.append("core_policy")
    if WRITE_ACTIONS:
        mode_bits.append("write_actions")
    mode_str = ", ".join(mode_bits) if mode_bits else "idle"

    try:
        send_tg(
            f"🧠 AI Pilot started for label={ACCOUNT_LABEL} "
            f"({mode_str}, poll={POLL_SECONDS}s)"
        )
    except Exception:
        logger.info(
            "AI Pilot started for label=%s (%s, poll=%ss)",
            ACCOUNT_LABEL,
            mode_str,
            POLL_SECONDS,
        )

    logger.info(
        "AI Pilot loop starting (label=%s, poll=%ss, dry_run=%s, sample_policy=%s, "
        "core_policy=%s, write_actions=%s, actions_file=%s)",
        ACCOUNT_LABEL,
        POLL_SECONDS,
        DRY_RUN,
        USE_SAMPLE_POLICY,
        USE_CORE_POLICY,
        WRITE_ACTIONS,
        AI_ACTIONS_FILE,
    )

    while True:
        record_heartbeat("ai_pilot")
        t0 = time.time()

        try:
            ai_state = _build_ai_state()

            total_actions_written = 0

            # 1) Sample policy (toy, advisory → adapted into AIAction)
            sample_actions = _run_sample_policy(ai_state)
            if sample_actions:
                written = _dispatch_actions(sample_actions, label=ACCOUNT_LABEL)
                total_actions_written += written

            # 2) Core policy (real brain scaffold)
            core_actions = _run_core_policy(ai_state)
            if core_actions:
                written = _dispatch_actions(core_actions, label=ACCOUNT_LABEL)
                total_actions_written += written

            if total_actions_written > 0:
                logger.info(
                    "AI Pilot emitted actions for label=%s "
                    "(sample=%d, core=%d, written=%d)",
                    ACCOUNT_LABEL,
                    len(sample_actions),
                    len(core_actions),
                    total_actions_written,
                )

        except Exception as e:
            alert_bot_error("ai_pilot", f"loop error: {e}", "ERROR")

        # Simple pacing
        elapsed = time.time() - t0
        sleep_sec = max(0.5, POLL_SECONDS - elapsed)
        time.sleep(sleep_sec)


if __name__ == "__main__":
    loop()
