#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Stack Supervisor v2.8 (import-hardened)

Fixes vs v2.7
-------------
- Correct ROOT fallback (project root, not /app)
- ws_switchboard import is now resilient:
    tries app.ws.ws_switchboard, app.core.ws_switchboard, app.bots.ws_switchboard
- Entry function detection is resilient:
    prefers main(), else loop(), else run(), else module-level __main__ style main()
- Adds clearer BOOT logging so you know what module got launched

Role
----
Single entrypoint to run the "AI stack" for ONE ACCOUNT_LABEL:

    • ws_switchboard           (WS → positions_bus / orderbook_bus / trades_bus)
    • tp_sl_manager            (position-bus aware TP/SL manager)
    • ai_pilot                 (AI brain reading ai_state_bus)
    • ai_action_router         (routes AI decisions into notifications / later exec)
    • ai_journal (no-op)       (legacy placeholder, ai_events_spine is called directly)
    • risk_daemon (optional)   (global guards)
    • trade_outcomes           (tails ws_executions → ai_events.outcomes)
    • paper_price_feeder       (feeds WS prices into PaperBroker for LEARN_DRY)

Design
------
- One OS process per worker using multiprocessing.Process.
- Windows-safe: worker targets are TOP-LEVEL functions.
- Supervisor:
    • Reads AI_STACK_ENABLE_* env flags (prefers .env file values).
    • Starts enabled workers if not running.
    • Restarts workers that die.
    • Writes heartbeat via flashback_common.record_heartbeat("supervisor_ai_stack").
    • Respects config/subaccounts.yaml enable_ai_stack per ACCOUNT_LABEL (if present).

Notes
-----
- Backward compatible:
    • AI_STACK_ENABLE_PAPER_TICK_DAEMON (old) is treated as alias for
      AI_STACK_ENABLE_PAPER_PRICE_FEEDER (new).
"""

from __future__ import annotations

import multiprocessing as mp
import os
import signal
import sys
import time
from pathlib import Path
from typing import Callable, Dict, Optional

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

try:
    from app.core.log import get_logger
except Exception:  # pragma: no cover
    import logging

    def get_logger(name: str) -> "logging.Logger":  # type: ignore
        logger_ = logging.getLogger(name)
        if not logger_.handlers:
            handler = logging.StreamHandler(sys.stdout)
            fmt = logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
            handler.setFormatter(fmt)
            logger_.addHandler(handler)
        logger_.setLevel(logging.INFO)
        return logger_


log = get_logger("supervisor_ai_stack")

# ---------------------------------------------------------------------------
# ROOT + .env loading (critical for AI_STACK_* flags)
# ---------------------------------------------------------------------------

try:
    from app.core.config import settings
    ROOT: Path = Path(settings.ROOT)  # type: ignore
except Exception:
    # Correct fallback: .../app/bots/supervisor_ai_stack.py -> project root is parents[2]
    ROOT = Path(__file__).resolve().parents[2]

# First pass: load .env into process env
try:  # pragma: no cover
    from dotenv import load_dotenv, dotenv_values  # type: ignore

    load_dotenv(ROOT / ".env")
    ENV_FILE_VARS = dotenv_values(ROOT / ".env")
    log.info("Loaded .env from %s", ROOT / ".env")
except Exception:
    log.info("Could not load .env via python-dotenv; relying on OS environment only.")
    ENV_FILE_VARS = {}

# ---------------------------------------------------------------------------
# Core helpers (TG / heartbeat)
# ---------------------------------------------------------------------------

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
        if level.upper() in ("WARN", "WARNING"):
            log.warning("[%s] %s", bot_name, msg)
        else:
            log.error("[%s] %s", bot_name, msg)

# ---------------------------------------------------------------------------
# Env helpers
# ---------------------------------------------------------------------------

def _env_int(name: str, default: str) -> int:
    try:
        return int(os.getenv(name, default).strip())
    except Exception:
        return int(default)

def _file_first_bool(name: str, default: str = "false") -> bool:
    """
    Prefer value from .env file (ENV_FILE_VARS) if present.
    Fall back to process env, then default.
    """
    if name in ENV_FILE_VARS and ENV_FILE_VARS[name] is not None:
        raw = str(ENV_FILE_VARS[name]).strip().lower()
    else:
        raw = os.getenv(name, default).strip().lower()
    return raw in ("1", "true", "yes", "y", "on")

def _file_first_bool_alias(primary_name: str, alias_name: str, default: str = "false") -> bool:
    """
    Prefer primary_name from .env; if absent, fall back to alias_name; then env; then default.
    """
    if primary_name in ENV_FILE_VARS and ENV_FILE_VARS[primary_name] is not None:
        raw = str(ENV_FILE_VARS[primary_name]).strip().lower()
        return raw in ("1", "true", "yes", "y", "on")

    if alias_name in ENV_FILE_VARS and ENV_FILE_VARS[alias_name] is not None:
        raw = str(ENV_FILE_VARS[alias_name]).strip().lower()
        return raw in ("1", "true", "yes", "y", "on")

    if os.getenv(primary_name) is not None:
        raw = os.getenv(primary_name, default).strip().lower()
        return raw in ("1", "true", "yes", "y", "on")

    if os.getenv(alias_name) is not None:
        raw = os.getenv(alias_name, default).strip().lower()
        return raw in ("1", "true", "yes", "y", "on")

    raw = str(default).strip().lower()
    return raw in ("1", "true", "yes", "y", "on")

ACCOUNT_LABEL: str = os.getenv("ACCOUNT_LABEL", "main").strip() or "main"

# For AI stack toggles we *trust the .env file* over OS env
AI_STACK_ENABLE_WS_SWITCHBOARD: bool = _file_first_bool("AI_STACK_ENABLE_WS_SWITCHBOARD", "true")
AI_STACK_ENABLE_TP_SL_MANAGER: bool = _file_first_bool("AI_STACK_ENABLE_TP_SL_MANAGER", "true")
AI_STACK_ENABLE_AI_PILOT: bool = _file_first_bool("AI_STACK_ENABLE_AI_PILOT", "true")
AI_STACK_ENABLE_AI_ACTION_ROUTER: bool = _file_first_bool("AI_STACK_ENABLE_AI_ACTION_ROUTER", "true")
AI_STACK_ENABLE_AI_JOURNAL: bool = _file_first_bool("AI_STACK_ENABLE_AI_JOURNAL", "false")  # default off; no-op
AI_STACK_ENABLE_RISK_DAEMON: bool = _file_first_bool("AI_STACK_ENABLE_RISK_DAEMON", "false")  # safer default off
AI_STACK_ENABLE_TRADE_OUTCOMES: bool = _file_first_bool("AI_STACK_ENABLE_TRADE_OUTCOMES", "true")

# Renamed flag: paper_tick_daemon -> paper_price_feeder
AI_STACK_ENABLE_PAPER_PRICE_FEEDER: bool = _file_first_bool_alias(
    "AI_STACK_ENABLE_PAPER_PRICE_FEEDER",
    "AI_STACK_ENABLE_PAPER_TICK_DAEMON",
    "true",
)

SUPERVISOR_POLL_SECONDS: int = _env_int("AI_STACK_SUPERVISOR_POLL_SECONDS", "3")

log.info(
    "BOOT | ROOT=%s | ACCOUNT_LABEL=%s | poll=%ss",
    ROOT, ACCOUNT_LABEL, SUPERVISOR_POLL_SECONDS
)

log.info(
    "Flags (file-first): WS=%s TP/SL=%s PILOT=%s ROUTER=%s RISK=%s OUTCOMES=%s PAPER_FEED=%s",
    AI_STACK_ENABLE_WS_SWITCHBOARD,
    AI_STACK_ENABLE_TP_SL_MANAGER,
    AI_STACK_ENABLE_AI_PILOT,
    AI_STACK_ENABLE_AI_ACTION_ROUTER,
    AI_STACK_ENABLE_RISK_DAEMON,
    AI_STACK_ENABLE_TRADE_OUTCOMES,
    AI_STACK_ENABLE_PAPER_PRICE_FEEDER,
)

# ---------------------------------------------------------------------------
# Subaccount gating (config/subaccounts.yaml)
# ---------------------------------------------------------------------------

def _label_ai_stack_allowed(label: str) -> bool:
    """
    Check config/subaccounts.yaml for this label.

    If an entry exists:
        - If enabled: false                -> deny
        - If enable_ai_stack: false        -> deny
        - Else                             -> allow

    If file missing or label not present, default = allow.
    """
    sub_path = ROOT / "config" / "subaccounts.yaml"
    if not sub_path.exists():
        return True

    try:
        import yaml  # type: ignore
    except Exception:
        log.warning(
            "config/subaccounts.yaml present but PyYAML not available; "
            "cannot gate AI stack by label. Defaulting to allow."
        )
        return True

    try:
        with sub_path.open("r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    except Exception as e:
        log.warning("Failed to parse %s: %s. Defaulting to allow for label=%s", sub_path, e, label)
        return True

    accounts = cfg.get("accounts") or []
    if not isinstance(accounts, list):
        return True

    for acc in accounts:
        try:
            acc_label = str(acc.get("account_label") or "").strip()
        except Exception:
            continue
        if not acc_label or acc_label != label:
            continue

        enabled = acc.get("enabled", True)
        enable_ai_stack = acc.get("enable_ai_stack", False)

        if not enabled:
            log.info("subaccounts.yaml: account_label=%s has enabled=false -> AI stack disabled.", label)
            return False

        if not enable_ai_stack:
            log.info("subaccounts.yaml: account_label=%s has enable_ai_stack=false -> AI stack disabled.", label)
            return False

        log.info("subaccounts.yaml: account_label=%s has enable_ai_stack=true -> AI stack allowed.", label)
        return True

    return True


# ---------------------------------------------------------------------------
# Dynamic import helpers (so ws_switchboard stops being a landmine)
# ---------------------------------------------------------------------------

def _import_first(mod_names: list[str]):
    last_err = None
    for m in mod_names:
        try:
            module = __import__(m, fromlist=["*"])
            log.info("Import OK: %s", m)
            return module
        except Exception as e:
            last_err = e
            log.warning("Import failed: %s (%s)", m, e)
    raise ImportError(f"All imports failed: {mod_names}. Last error: {last_err}")

def _call_entry(module, bot_name: str) -> None:
    """
    Prefer main() then loop() then run().
    """
    for fn_name in ("main", "loop", "run"):
        fn = getattr(module, fn_name, None)
        if callable(fn):
            log.info("%s entry: %s.%s()", bot_name, module.__name__, fn_name)
            fn()
            return
    raise AttributeError(f"{module.__name__} has no callable main/loop/run")


# ---------------------------------------------------------------------------
# Worker targets (must be top-level for Windows)
# ---------------------------------------------------------------------------

def _run_ws_switchboard() -> None:
    """
    WS Switchboard → fills position_bus / market_bus.
    """
    try:
        # Order preference: canonical WS package, then core, then bots (legacy)
        mod = _import_first([
            "app.ws.ws_switchboard",
            "app.core.ws_switchboard",
            "app.bots.ws_switchboard",
        ])
        _call_entry(mod, "ws_switchboard")
    except Exception as e:
        alert_bot_error("ws_switchboard", f"import/runtime error: {e}", "ERROR")


def _run_tp_sl_manager() -> None:
    """
    TP/SL Manager → reads position_bus, posts exit ladders.
    """
    try:
        mod = _import_first(["app.bots.tp_sl_manager"])
        _call_entry(mod, "tp_sl_manager")
    except Exception as e:
        alert_bot_error("tp_sl_manager", f"import/runtime error: {e}", "ERROR")


def _run_ai_pilot() -> None:
    """
    AI Pilot → builds ai_state snapshot & emits AI actions (DRY-RUN by default).
    """
    try:
        mod = _import_first(["app.bots.ai_pilot"])
        _call_entry(mod, "ai_pilot")
    except Exception as e:
        alert_bot_error("ai_pilot", f"import/runtime error: {e}", "ERROR")


def _run_ai_action_router() -> None:
    """
    AI Action Router → tails state/ai_actions.jsonl and routes to Telegram.
    """
    try:
        mod = _import_first(["app.bots.ai_action_router"])
        _call_entry(mod, "ai_action_router")
    except Exception as e:
        alert_bot_error("ai_action_router", f"import/runtime error: {e}", "ERROR")


def _run_ai_journal() -> None:
    """
    Legacy AI journal worker (NO-OP).
    """
    log.info(
        "ai_journal worker is a no-op; ai_events_spine is used via direct calls. "
        "Set AI_STACK_ENABLE_AI_JOURNAL=false to disable this worker."
    )
    while True:
        record_heartbeat("ai_journal")
        time.sleep(60)


def _run_risk_daemon() -> None:
    """
    Risk daemon (optional).
    """
    try:
        mod = _import_first(["app.bots.risk_daemon"])
        _call_entry(mod, "risk_daemon")
    except Exception as e:
        alert_bot_error("risk_daemon", f"import/runtime error (optional): {e}", "WARN")


def _run_trade_outcomes() -> None:
    """
    Trade outcome recorder (optional).
    """
    try:
        # Your earlier code imported from app.ai.trade_outcome_recorder.
        # Keep that, but be resilient.
        mod = _import_first(["app.ai.trade_outcome_recorder", "app.bots.trade_outcome_recorder"])
        _call_entry(mod, "trade_outcomes")
    except Exception as e:
        alert_bot_error("trade_outcomes", f"import/runtime error (optional): {e}", "WARN")


def _run_paper_price_feeder() -> None:
    """
    Paper Price Feeder (optional).
    """
    try:
        mod = _import_first(["app.sim.paper_price_feeder"])
        _call_entry(mod, "paper_price_feeder")
    except Exception as e:
        alert_bot_error("paper_price_feeder", f"import/runtime error (optional): {e}", "WARN")


# ---------------------------------------------------------------------------
# Worker registry
# ---------------------------------------------------------------------------

class WorkerSpec:
    def __init__(self, name: str, enabled: bool, target: Callable[[], None]) -> None:
        self.name = name
        self.enabled = enabled
        self.target = target
        self.process: Optional[mp.Process] = None


def _build_worker_specs() -> Dict[str, WorkerSpec]:
    return {
        "ws_switchboard": WorkerSpec(
            name="ws_switchboard",
            enabled=AI_STACK_ENABLE_WS_SWITCHBOARD,
            target=_run_ws_switchboard,
        ),
        "tp_sl_manager": WorkerSpec(
            name="tp_sl_manager",
            enabled=AI_STACK_ENABLE_TP_SL_MANAGER,
            target=_run_tp_sl_manager,
        ),
        "ai_pilot": WorkerSpec(
            name="ai_pilot",
            enabled=AI_STACK_ENABLE_AI_PILOT,
            target=_run_ai_pilot,
        ),
        "ai_action_router": WorkerSpec(
            name="ai_action_router",
            enabled=AI_STACK_ENABLE_AI_ACTION_ROUTER,
            target=_run_ai_action_router,
        ),
        "ai_journal": WorkerSpec(
            name="ai_journal",
            enabled=AI_STACK_ENABLE_AI_JOURNAL,
            target=_run_ai_journal,
        ),
        "risk_daemon": WorkerSpec(
            name="risk_daemon",
            enabled=AI_STACK_ENABLE_RISK_DAEMON,
            target=_run_risk_daemon,
        ),
        "trade_outcomes": WorkerSpec(
            name="trade_outcomes",
            enabled=AI_STACK_ENABLE_TRADE_OUTCOMES,
            target=_run_trade_outcomes,
        ),
        "paper_price_feeder": WorkerSpec(
            name="paper_price_feeder",
            enabled=AI_STACK_ENABLE_PAPER_PRICE_FEEDER,
            target=_run_paper_price_feeder,
        ),
    }


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

    try:
        send_tg(f"🧩 AI Stack Supervisor online (label={ACCOUNT_LABEL}, poll={SUPERVISOR_POLL_SECONDS}s)")
    except Exception:
        log.info("AI Stack Supervisor online (TG notify failed or disabled).")

    specs = _build_worker_specs()

    for name, spec in specs.items():
        log.info("Worker %-18s enabled=%s", name, spec.enabled)

    while True:
        record_heartbeat("supervisor_ai_stack")
        record_heartbeat(f"supervisor_ai_stack:{ACCOUNT_LABEL}")

        for name, spec in specs.items():
            if not spec.enabled:
                if spec.process is not None and spec.process.is_alive():
                    log.info("Worker %s disabled -> stopping.", name)
                    _stop_worker(spec)
                continue

            if spec.process is None or not spec.process.is_alive():
                if spec.process is not None:
                    exitcode = spec.process.exitcode
                    alert_bot_error(
                        "supervisor_ai_stack",
                        f"Worker {name} died (label={ACCOUNT_LABEL}, exitcode={exitcode}); restarting.",
                        "WARN",
                    )
                _start_worker(spec)

        time.sleep(SUPERVISOR_POLL_SECONDS)


def main() -> None:
    if not _label_ai_stack_allowed(ACCOUNT_LABEL):
        msg = f"AI Stack Supervisor disabled for label={ACCOUNT_LABEL} by config/subaccounts.yaml"
        log.info(msg)
        try:
            send_tg(f"🛑 {msg}")
        except Exception:
            pass
        return

    try:
        mp.set_start_method("spawn", force=False)
    except RuntimeError:
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
