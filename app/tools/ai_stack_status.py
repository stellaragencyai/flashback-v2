#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Stack Supervisor v2.2

Role
----
Single entrypoint to run the "AI stack" for ONE ACCOUNT_LABEL:

    • ws_switchboard       (WS → positions_bus / orderbook_bus / trades_bus)
    • tp_sl_manager        (position-bus aware TP/SL manager)
    • ai_pilot             (AI brain reading ai_state_bus)
    • ai_action_router     (routes AI decisions into notifications / later exec)
    • (optional) risk_daemon

Notes
-----
- ai_journal + trade_outcomes are handled via ai_events_spine + offline tools.
  They are NO-OP workers here to avoid useless restart loops.

Design
------
- One OS process per worker using multiprocessing.Process.
- Windows-safe: worker targets are TOP-LEVEL functions.
- Supervisor:
    • Reads AI_STACK_ENABLE_* env flags.
    • Starts enabled workers if not running.
    • Restarts workers that die.
    • Writes heartbeat via flashback_common.record_heartbeat("supervisor_ai_stack").
"""

from __future__ import annotations

import multiprocessing as mp
import os
import signal
import sys
import time
from typing import Callable, Dict, Optional

try:
    from app.core.log import get_logger
except Exception:  # pragma: no cover
    import logging

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


log = get_logger("supervisor_ai_stack")

# Core helpers
try:
    from app.core.flashback_common import (
        record_heartbeat,
        send_tg,
        alert_bot_error,
    )
except Exception:
    # Minimal fallbacks
    def record_heartbeat(name: str) -> None:
        return None

    def send_tg(msg: str) -> None:
        log.info("[TG Fallback] %s", msg)

    def alert_bot_error(bot_name: str, msg: str, level: str = "ERROR") -> None:
        log.error("[%s] %s", bot_name, msg)


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

AI_STACK_ENABLE_WS_SWITCHBOARD: bool = _env_bool(
    "AI_STACK_ENABLE_WS_SWITCHBOARD", "true"
)
AI_STACK_ENABLE_TP_SL_MANAGER: bool = _env_bool(
    "AI_STACK_ENABLE_TP_SL_MANAGER", "true"
)
AI_STACK_ENABLE_AI_PILOT: bool = _env_bool(
    "AI_STACK_ENABLE_AI_PILOT", "true"
)
AI_STACK_ENABLE_AI_ACTION_ROUTER: bool = _env_bool(
    "AI_STACK_ENABLE_AI_ACTION_ROUTER", "true"
)
AI_STACK_ENABLE_RISK_DAEMON: bool = _env_bool(
    "AI_STACK_ENABLE_RISK_DAEMON", "true"
)

SUPERVISOR_POLL_SECONDS: int = _env_int("AI_STACK_SUPERVISOR_POLL_SECONDS", "3")


# ---------------------------------------------------------------------------
# Worker targets (must be top-level for Windows)
# ---------------------------------------------------------------------------

def _run_ws_switchboard() -> None:
    """
    WS Switchboard → fills position_bus / market_bus.
    """
    try:
        from app.ws.ws_switchboard import main as ws_main
    except Exception as e:
        alert_bot_error("ws_switchboard", f"import error: {e}", "ERROR")
        return

    ws_main()


def _run_tp_sl_manager() -> None:
    """
    TP/SL Manager → reads position_bus, posts exit ladders.
    """
    try:
        from app.bots.tp_sl_manager import loop as tpm_loop
    except Exception as e:
        alert_bot_error("tp_sl_manager", f"import error: {e}", "ERROR")
        return

    tpm_loop()


def _run_ai_pilot() -> None:
    """
    AI Pilot → builds ai_state snapshot & emits AI actions (DRY-RUN by default).
    """
    try:
        from app.bots.ai_pilot import loop as pilot_loop
    except Exception as e:
        alert_bot_error("ai_pilot", f"import error: {e}", "ERROR")
        return

    pilot_loop()


def _run_ai_action_router() -> None:
    """
    AI Action Router → tails state/ai_actions.jsonl and routes to Telegram.
    DRY-RUN only (no orders).
    """
    try:
        from app.bots.ai_action_router import loop as router_loop
    except Exception as e:
        alert_bot_error("ai_action_router", f"import error: {e}", "ERROR")
        return

    router_loop()


def _run_risk_daemon() -> None:
    """
    Risk daemon (optional). If the module does not exist yet, no-op.
    """
    try:
        from app.bots.risk_daemon import main as risk_main
    except Exception as e:
        alert_bot_error("risk_daemon", f"import error (optional module): {e}", "WARN")
        return

    risk_main()


# ---------------------------------------------------------------------------
# Worker registry
# ---------------------------------------------------------------------------

class WorkerSpec:
    def __init__(
        self,
        name: str,
        enabled: bool,
        target: Callable[[], None],
    ) -> None:
        self.name = name
        self.enabled = enabled
        self.target = target
        self.process: Optional[mp.Process] = None


def _build_worker_specs() -> Dict[str, WorkerSpec]:
    specs: Dict[str, WorkerSpec] = {}

    specs["ws_switchboard"] = WorkerSpec(
        name="ws_switchboard",
        enabled=AI_STACK_ENABLE_WS_SWITCHBOARD,
        target=_run_ws_switchboard,
    )
    specs["tp_sl_manager"] = WorkerSpec(
        name="tp_sl_manager",
        enabled=AI_STACK_ENABLE_TP_SL_MANAGER,
        target=_run_tp_sl_manager,
    )
    specs["ai_pilot"] = WorkerSpec(
        name="ai_pilot",
        enabled=AI_STACK_ENABLE_AI_PILOT,
        target=_run_ai_pilot,
    )
    specs["ai_action_router"] = WorkerSpec(
        name="ai_action_router",
        enabled=AI_STACK_ENABLE_AI_ACTION_ROUTER,
        target=_run_ai_action_router,
    )
    specs["risk_daemon"] = WorkerSpec(
        name="risk_daemon",
        enabled=AI_STACK_ENABLE_RISK_DAEMON,
        target=_run_risk_daemon,
    )

    return specs


# ---------------------------------------------------------------------------
# Supervisor core
# ---------------------------------------------------------------------------

def _start_worker(spec: WorkerSpec) -> None:
    if spec.process is not None and spec.process.is_alive():
        return

    log.info("Starting worker %s ...", spec.name)
    p = mp.Process(
        target=spec.target,
        name=f"fb_{spec.name}",
        daemon=False,
    )
    p.start()
    spec.process = p
    log.info("Worker %s started with pid=%s", spec.name, p.pid)


def _stop_worker(spec: WorkerSpec) -> None:
    p = spec.process
    if p is None:
        return

    if not p.is_alive():
        spec.process = None
        return

    log.info("Stopping worker %s (pid=%s) ...", spec.name, p.pid)
    try:
        p.terminate()
    except Exception:
        pass

    try:
        p.join(timeout=10)
    except Exception:
        pass

    if p.is_alive():
        try:
            os.kill(p.pid, signal.SIGKILL)  # type: ignore[arg-type]
        except Exception:
            pass

    spec.process = None
    log.info("Worker %s stopped.", spec.name)


def _supervisor_loop() -> None:
    log.info(
        "AI Stack Supervisor starting for ACCOUNT_LABEL=%s (poll=%ss)",
        ACCOUNT_LABEL,
        SUPERVISOR_POLL_SECONDS,
    )

    # Make it explicit: these two are intentionally NO-OP workers here.
    log.info("ai_journal worker is a no-op; ai_events_spine is used via direct calls.")
    log.info("trade_outcomes worker is a no-op here; outcomes come from ai_events logs + offline tools.")

    try:
        send_tg(
            f"🧩 AI Stack Supervisor online (label={ACCOUNT_LABEL}, poll={SUPERVISOR_POLL_SECONDS}s)"
        )
    except Exception:
        log.info("AI Stack Supervisor online (TG notify failed or disabled).")

    specs = _build_worker_specs()

    # Initial status log
    for name, spec in specs.items():
        log.info("Worker %-15s enabled=%s", name, spec.enabled)

    while True:
        record_heartbeat("supervisor_ai_stack")

        for name, spec in specs.items():
            if not spec.enabled:
                # If disabled but still running, stop it.
                if spec.process is not None and spec.process.is_alive():
                    log.info("Worker %s disabled -> stopping.", name)
                    _stop_worker(spec)
                continue

            # Enabled: ensure running
            if spec.process is None or not spec.process.is_alive():
                if spec.process is not None:
                    exitcode = spec.process.exitcode
                    alert_bot_error(
                        "supervisor_ai_stack",
                        f"Worker {name} died (exitcode={exitcode}); restarting.",
                        "WARN",
                    )
                _start_worker(spec)

        time.sleep(SUPERVISOR_POLL_SECONDS)


def main() -> None:
    try:
        # On Windows, spawn is default; keeping this explicit is harmless.
        mp.set_start_method("spawn", force=False)
    except RuntimeError:
        # Already set; ignore.
        pass

    try:
        _supervisor_loop()
    except KeyboardInterrupt:
        log.info("AI Stack Supervisor interrupted by user; shutting down...")
    except Exception as e:
        alert_bot_error("supervisor_ai_stack", f"fatal error: {e}", "ERROR")
        raise


if __name__ == "__main__":
    main()
